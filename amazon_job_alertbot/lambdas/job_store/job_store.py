import contextlib
import difflib
import json
import logging
import random
import re
import time
import traceback

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Literal

import boto3

from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()


today = datetime.now(timezone.utc)
today_str: str = today.isoformat()
table_empty = False


def log_client_error(error: Exception, func: str) -> None:
    """
    Logs the client error message.

    Args:
        error (Exception): The error to log.
        func (str): The function that threw the error.
    """
    logger.error(f"Error in {func}: {error}")
    logger.error(f"Error code: {error.response['Error']['Code']}")
    logger.error(f"Error message: {error.response["Error"]["Message"]}")
    logger.error(f"Error RequestID: {error.response["ResponseMetadata"]["RequestId"]}")
    logger.error(
        f"Error HTTPStatusCode: {error.response['ResponseMetadata']['HTTPStatusCode']}"
    )
    logger.error(f"Complete error response: {error.response}")


def get_data(event: dict[str, Any]) -> dict[str, Any]:
    """
    Retrieves a dictionary for key
    from event.

    Args:
        event (dict[Any, Any]): A dictionary to parse.

    Returns:
        dict[Any, Any]: The parsed dictionary.

    """
    payload = event.get("Payload", {}) or event
    return payload.get("data", {})


def set_vars(
    event: dict[str, Any],
) -> tuple[dict[str, Any] | int | Any | bool | datetime | None]:
    """
    Set the variables based on the provided event.

    Args:
        event: The event containing the necessary parameters.

    Returns:
        tuple: A tuple containing the jobs, remaining hits, table, a boolean value, and the newest scrape.

    """
    config = Config(retries={"max_attempts": 10, "mode": "adaptive"})
    db = boto3.resource("dynamodb", config=config)
    data: dict[str, Any] = get_data(event)
    table = db.Table(data.get("tablename", ""))
    return (
        data.get("jobs", []),
        data.get("remaining_hits", 0),
        table,
        False,
        data.get("newest_scrape", find_newest_scrape(table=table)),
    )


def scan_all_items(table_name: str, key_to_check: str) -> list[Any]:
    """
    Scans all items in a DynamoDB table and returns a list of items using a Paginator.

    Args:
        table_name: The name of the DynamoDB table to scan.
        key_to_check: The key to include in the projection expression.

    Returns:
        list[Any]: A list of items scanned from the table.
    """
    dynamodb = boto3.client("dynamodb")
    paginator = dynamodb.get_paginator("scan")

    all_items = []
    for page in paginator.paginate(
        TableName=table_name, ProjectionExpression=key_to_check
    ):
        all_items.extend(page.get("Items", []))

    return all_items


def compare_strings(old_string: str, new_string: str, context: int) -> str | None:
    """
    Get the unified diff between the old string and the new string.

    The function encodes the old and new strings as bytes and splits them into lines.
    It then computes the unified diff using the difflib.unified_diff function with the
    specified context. The resulting diff is joined into a single string and returned.

    Args:
        old_string: The old string to compare.
        new_string: The new string to compare.
        context: The number of context lines to include in the diff. Defaults to 3.

    Returns:
        The unified diff between the old string and the new string.
    """
    old: list[str] = old_string.splitlines(keepends=True)
    new: list[str] = new_string.splitlines(keepends=True)
    d = difflib.HtmlDiff(wrapcolumn=80)
    return d.make_table(
        fromlines=old,
        tolines=new,
        fromdesc="Old Ad",
        todesc="New Ad",
        context=True,
        numlines=context,
    )


def substantial_keys(key: str) -> bool:
    """
    Checks if a key is one we care about.

    Args:
        key: The key to check.

    Returns:
        bool: True if the key is substantial, False otherwise.
    """
    if key in {
        "changes",
        "last_scrape",
        "updated_time",
        "last_updated",
        "company_name",
        "display_distance",
        "job_function_id",
        "primary_search_label",
        "source_system",
        "url_next_step",
        "sent",
        "sent_on",
        "message_id",
    }:
        return False


def get_diff(new_job: dict[str, Any], table, context: int = 3) -> str:
    try:
        response = table.get_item(Key={"id_icims": new_job["id_icims"]})
        old_job = response["Item"]
    except KeyError as e:
        logger.debug(f"Item not in database, {e}, storing as new job")
        store_job(job=new_job, table=table)
        return None
    except ClientError as e:
        log_client_error(e, "get_diff")
        return None

    changes = {}
    for key, new_value in new_job.items():
        if not substantial_keys(key=key):
            continue
        old_value = old_job.get(key, "")
        if str(old_value) != str(new_value):
            changes[key] = compare_strings(
                old_string=str(old_value), new_string=str(new_value, context=context)
            )
    return changes


def get_status(
    job_id: int, table: Any, last_updated: datetime | str
) -> Literal["new", "updated"] | None:
    """
    Get the status of the job with the specified ID.

    The function checks if the job exists in the table. If it does not exist, the status is "new".
    If the job exists and its last scraped time is greater than the provided last scraped time,
    the status is "updated". Otherwise, the status is None.

    Args:
        job_id: The ID of the job.
        table: The table object to perform the get operation on.
        last_updated: The last scraped time to compare against.

    Returns:
        The status of the job, which can be "new", "updated", or None.

    """
    logger.debug("attempting to check table status...")
    if table_empty:
        logger.debug("table is empty, returning new")
        return "new"
    try:
        return assess_status(table, job_id, last_updated)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ValidationException":
            return "new"
        log_client_error(error=e, func="get_status")
    return None


def assess_status(table: Any, job_id: str, last_updated: datetime):
    """
    Assesses the status of a job based on its presence in the database and last update times.

    Args:
        table: The table object used for database operations.
        job_id: The ID of the job to assess.
        last_updated: The timestamp of the last update.

    Returns:
        str: The status of the job, which can be "new", "updated", or None.
    """
    response = table.get_item(Key={"id_icims": job_id})
    logger.debug(f"checking if job {job_id} is in database: {response}")
    if not response or not response.get("Items", []):
        return "new"
    status = None
    if last_scrape := response["Items"][0].get("last_scrape"):
        last_updated = (
            last_updated
            if isinstance(last_updated, datetime)
            else datetime.fromisoformat(last_updated)
        )
        if datetime.fromisoformat(last_scrape) < last_updated:
            logger.debug(f"job {job_id} is updated, returning updated")
            status = "updated"
        if sent := response["Items"][0].get("sent_on"):
            if datetime.fromisoformat(sent) <= last_updated:
                status = "updated"
    return status


def stringify_dates(job: dict[str, Any]) -> dict[str, Any]:
    """
    Convert all date fields to strings.

    Args:
        job: The job dictionary to convert.

    Returns:
        The job dictionary with all date fields converted to strings.
    """
    return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in job.items()}


def store_job(job: dict[str, Any], table: Any) -> None:
    """
    Store the job in the specified table.

    The function attempts to put the job item into the table using the provided table object.
    If an error occurs during the operation, an error message is logged.

    Args:
        job: The job item to store.
        table: The table object to perform the put operation on.

    Returns:
        None

    """
    logger.debug(f"attempting to store job {job['id_icims']}")
    try:
        job = stringify_dates(job=job)
        response = table.put_item(
            Item=job,
            ReturnItemCollectionMetrics="SIZE",
            ReturnValues="ALL_OLD",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
            ReturnConsumedCapacity="INDEXES",
        )
        logger.debug(
            f"Response from attempt to store job {job['id_icims']}: \n {response}"
        )
    except ClientError as e:
        logger.error(f"Error storing job {job['id_icims']}:")
        log_client_error(error=e, func="store_job")


def execute_threads(jobs: list[dict[str, Any]], table: Any) -> list[dict[str, Any]]:
    """
    Execute multiple job storage tasks concurrently using a thread pool.

    Args:
        jobs: List of jobs to update and store.
        table: The table to store the jobs in.

    Returns:
        list: List of results from the job storage tasks.
    """
    logger.debug("Initiating threadpool for job storage tasks")
    with ThreadPoolExecutor(thread_name_prefix="job_storage_worker") as executor:
        futures: Iterator[dict[str, Any]] = executor.map(
            update_and_store_job, iter(jobs), [table] * len(jobs)
        )
    return list(list(futures))


def parse_qualifications(qualifications: str) -> list[str]:
    """
    Parses and extracts qualifications from a given string of qualifications.

    Args:
        qualifications (str): A string containing qualifications to be parsed.

    Returns:
        list: A list of cleaned and extracted qualifications from the input string.
    """

    if not qualifications:
        return []

    cleaned_str: str = re.sub(
        pattern=r"[\s\t]*?[-•][\s\t]*?", repl="", string=qualifications
    )
    qualifications_list: list[str | Any] = re.split(
        pattern=r"<br/>[\s\t]?<br/>|<br/>", string=cleaned_str
    )

    return [
        item.strip()
        for item in qualifications_list
        if (item.strip() and not item.strip().startswith("Amazon is committed to"))
    ]
    # we remove the equal opportunity and reasonable accommodation statement.
    # While it's important, it's not a qualification for the job.
    # It seems an odd place to shove a boilerplate statement into...
    # We'll add the accommodations link to the bottom of our email instead


def normalize_job(job: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize job data by adjusting locations, URLs, and labels.

    Args:
        job: Dictionary containing job details to be normalized.

    Returns:
        dict[str, Any]: Normalized job data with adjusted locations, URLs, and labels.
    """
    if locations := [json.loads(item) for item in job.get("locations", []) if item]:
        job["adjusted_locations"] = "; ".join(
            sorted(
                list(
                    {
                        f"{item.get('normalizedCityName')}, {item.get('region')} [{item.get('type').lower()}])"
                        for item in locations
                    }
                )
            )
        )
        job["email_locations"] = [
            {
                "city": item.get("normalizedCityName"),
                "region": item.get("region"),
                "coordinates": item.get("coordinates"),
                "type": item.get("type").lower(),
            }
            for item in sorted(
                locations, key=lambda x: (x.get("normalizedCityName"), x.get("region"))
            )
        ]
        if buildings := {
            building
            for item in locations
            for building in item.get("buildingCodeList", [])
            if building
        }:
            job["buildings"] = ", ".join(sorted(list(buildings)))
    if search_labels := [
        item for item in job.get("optional_search_labels", []) if item
    ]:
        job["optional_search_labels"] = ", ".join(search_labels)
    job.pop("team", None)
    job.pop("department_cost_center", None)
    job["last_scrape"] = today
    job["job_path"] = f"https://amazon.jobs{job['job_path']}"
    job["date_off_market"] = "NA"
    job["basic_qualifications"] = parse_qualifications(job.get("basic_qualifications"))
    job["preferred_qualifications"] = parse_qualifications(
        job.get("preferred_qualifications")
    )
    return job


def update_and_store_job(
    job: dict[str, Any], table: Any
) -> dict[str, Any] | Any | None:
    """
    Update and store a job record in the specified table.

    Args:
        job: The job record to update and store.
        table: The table to store the job record in.

    Returns:
        dict: The updated job record if successfully stored, None otherwise.
    """

    job = normalize_job(job=job)
    if status := get_status(
        job_id=job["id_icims"],
        table=table,
        last_updated=job["last_updated"],
    ):
        if status == "new":
            job["type"] = "scrape_record"
            store_job(job=job, table=table)
            return job
        elif status == "updated":
            if changes := get_diff(new_job=job, table=table):
                job["changes"] = changes
                store_job(job=job, table=table)
                try:
                    return table.get_item(Key={"id_icims": job["id_icims"]})
                except ClientError as e:
                    log_client_error(e, "update_and_store_job")
                    if e.response["Error"]["Code"] not in [
                        "RequestLimitExceeded",
                        "Throttling",
                    ] and e.response.get("ResponseMetadata", {}).get(
                        "HTTPStatusCode"
                    ) not in [
                        429,
                        503,
                    ]:
                        raise e
                    time.sleep(random.uniform(1, 5))
                    return table.get_item(Key={"id_icims": job["id_icims"]})
            else:
                logger.debug(f"no changes found for job {job['id_icims']}")


def find_newest_scrape(table: Any) -> datetime:
    """
    Finds the newest last_scrape date that isn't today. We use this for finding how far
    back to go with subsequent scrapes. If the databse is empty, we set the value to 1.5
    years ago. With Amazon's "Hire and Develop the Best" core principal, some jobs can
    stay on the market for an extraordinary time while hiring managers wait out for the
    perfect candidate.

    We only set this on the first pass since after that we'll be getting newer dates. We retain the value for subsequent runs.

    Args:
        table: dynamodb table

    Returns:
        datetime: The capture limit.
    """
    start_date: str = (today - timedelta(days=30)).isoformat()
    logger.debug(f"attempting to find most recent scrape with start date {start_date}")
    with contextlib.suppress(ClientError):
        if table.item_count == 0 or table.table_size_bytes == 0:
            logger.debug("table empty")
            globals()["table_empty"] = True
            return today - timedelta(days=540)
    try:
        response = table.query(
            IndexName="last_scrape-index",
            KeyConditionExpression=Key("type").eq("scrape_record"),
            ScanIndexForward=False,  # Descending order
            Limit=1,
        )
        logger.debug(f"last_scrape query response: {response}")
        if items := response.get("Items", []):
            logger.debug(f"found last_scrape: {items[0]['last_scrape']}")
            return datetime.fromisoformat(items[0]["last_scrape"])
    except ClientError as e:
        log_client_error(error=e, func="find_newest_scrape")
    return today - timedelta(days=540)


def keep_keys(
    new_jobs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Filters the given list of new jobs and keeps only the specified keys in each job dictionary.

    Args:
        new_jobs (list[dict[str, str | int | dict | list | None]]): A list of dictionaries representing new jobs.

    Returns:
        dict: A dictionary containing the filtered job dictionaries with only the specified keys.

    """
    keepers = []
    for job in new_jobs:
        new: dict[str, Any] = {
            k: job[k]
            for k in job.keys()
            if k
            in [
                "id_icims",
                "title",
                "company",
                "adjusted_locations",
                "email_locations",
                "location",
                "last_updated",
                "posted_date",
                "city",
                "description_short",
                "basic_qualifications",
                "job_category",
                "job_path",
                "url_next_step",
            ]
        }
        keepers.append(new)
    return keepers


def store_to_db(event: dict[str, Any]) -> dict[str, dict[str, Any] | Any]:
    """
    Stores jobs in the database based on the provided event parameters.

    Args:
        event: A dictionary containing the event parameters.

    Returns:
        dict: A dictionary containing the status code, new/updated jobs, more jobs flag, and newest scrape date.

    """
    jobs, remaining_hits, table, more_jobs, newest_scrape = set_vars(event=event)
    logger.info(
        f"Job Store execution started. New jobs found: {len(jobs)}, remaining_hits: {remaining_hits}, newest_scrape: {newest_scrape}"
    )
    newest_scrape: datetime = (
        newest_scrape
        if isinstance(newest_scrape, datetime)
        else datetime.fromisoformat(newest_scrape)
    )
    if new_jobs := execute_threads(jobs, table):
        new_jobs = keep_keys(new_jobs=new_jobs)
        if (
            len(new_jobs) == len(jobs)
            or min(datetime.fromisoformat(date) for date in new_jobs["last_updated"])
            >= newest_scrape
        ) and remaining_hits:
            more_jobs = True
            logger.info(
                "Either all results were new/updated or last_updated date precedes earliest scraped date. Sending signal to get more jobs."
            )
        if not more_jobs:
            logger.info(
                "All available new/updated jobs have been stored in the database"
            )
        logger.info(
            f"Job Store execution completed. New/updated jobs stored in database: {len(new_jobs)} stored or updated"
        )

        return {
            "status": {"statusCode": 200, "state": "InvokeJobStore"},
            "data": {
                "jobs_to_send": True,
                "new_jobs": [stringify_dates(job) for job in new_jobs],
                "more_jobs": more_jobs,
                "newest_scrape": newest_scrape.isoformat(),
                "remaining_hits": remaining_hits,
            }
            | {
                k: v
                for k, v in get_data(event=event).items()
                if k
                not in [
                    "new_jobs",
                    "more_jobs",
                    "remaining_hits",
                    "newest_scrape",
                    "jobs",
                ]
            },
        }
    logger.info(
        "Job Store function execution completed. No new/updated jobs found, informing state machine."
    )
    return {
        "status": {"statusCode": 200, "state": "InvokeJobStore"},
        "data": {
            "more_jobs": more_jobs,
            "newest_scrape": None,
            "remaining_hits": 0,
        }
        | {
            k: v
            for k, v in get_data(event=event).items()
            if k
            not in ["new_jobs", "more_jobs", "remaining_hits", "newest_scrape", "jobs"]
        },
    }


def job_store_handler(event: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """
    Lambda function handler that fetches job data from Amazon.jobs and sends an email.

    Args:
        event: The event data passed to the Lambda function.
        context: The runtime information of the Lambda function.

    Returns:
        None
    """

    logger.debug(f"job_store function execution started with event:\n {event}")
    try:
        return store_to_db(event=event)

    except Exception as e:
        logger.error(f"Error in {context.function_name}: {e}")
        logger.error("State: InvokeJobStore")
        logger.error(traceback.format_exc())
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Entry event for error: {event}")
        raise e
