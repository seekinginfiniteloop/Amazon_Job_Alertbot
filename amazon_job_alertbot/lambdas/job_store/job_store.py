import contextlib
import difflib
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(name="amzn_job_store")
logger.setLevel(level="INFO")


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


def get_data(
    event: dict[str, Any],
) -> dict[str, Any]:
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
        logger.exception(e)
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
    job_id: int, table, last_updated: datetime, last_scrape: datetime
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
        response = table.get_item(Key={"id_icims": job_id})
        logger.debug(f"checking if job {job_id} is in database: {response}")
        if not response or not response.get("Items", []):
            return "new"
        if last_scrape := response["Items"][0].get("last_scrape"):
            if datetime.fromisoformat(last_scrape) < last_updated:
                logger.debug(f"job {job_id} is updated, returning updated")
                return "updated"

    except ClientError as e:
        if e.response["Error"]["Code"] == "ValidationException":
            return "new"
        log_client_error(error=e, func="get_status")
    return None


def stringify_dates(job: dict[str, Any]) -> dict[str, Any]:
    """
    Convert all date fields to strings.

    Args:
        job: The job dictionary to convert.

    Returns:
        The job dictionary with all date fields converted to strings.
    """
    for key, value in job.items():
        if key in ["last_updated", "posted_date", "last_scrape"]:
            job[key] = value.isoformat()
        if isinstance(value, datetime):
            job[key] = value.isoformat()
    return job


def store_job(job: dict[str, Any], table) -> None:
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


def set_dates(job: dict[str, Any]) -> datetime:
    """
    Sets the dates for a job.

    Args:
        job (dict): The job dictionary containing information about the job.

    Returns:
        Tuple[datetime.date, datetime.date, datetime.date]: A tuple containing the posted date, last updated date (or posted date if unavailable), and the current date.
    """

    posted_date: datetime = parse_date(job["posted_date"])
    if updated_time_str := job.get("updated_time"):
        updated_time = (
            int(updated_time_str.replace(" days", "").strip())
            if (updated_time_str.endswith("days") or updated_time_str.endswith("day"))
            else 0
        )
        last_updated: datetime = today - timedelta(days=updated_time)
        return posted_date, last_updated, today
    return posted_date, posted_date, today


def parse_date(date_str: str) -> datetime:
    """
    Parses a date string into a datetime object.

    Args:
        date_str (str): The date string to be parsed.

    Returns:
        datetime.date: The parsed datetime object.

    Raises:
        ValueError: If the date string is not in the expected format.
    """

    for fmt in ("%Y-%m-%d", "%B %d, %Y"):
        with contextlib.suppress(ValueError):
            return datetime.strptime(date_str, fmt).date()
    raise ValueError(f"Date string {date_str} is not in an expected format")


def update_and_store_jobs(
    data: dict[str, str | int | dict | list], table
) -> list[dict[str, str | int | dict | list | None]] | None:
    """
    Extract and store the jobs from the provided data into the specified table.
    The function retrieves the last scrape time from the table and iterates over each
    job in the data.If the last scrape time exists, it checks the status of the job
    using the get_status function.If the status is "new" or "updated", the job is
    stored in the table using the store_job function. If the status is "updated", the
    job is also stored with the differences using the store_job_with_diff function.
    If the last scrape time does not exist, all jobs are stored in the table.
    The function returns a list of new or updated jobs.

    Args:
        data: The data containing the jobs to extract and store.
        table: The table object to store the jobs in.

    Returns:
        A list of new or updated jobs.

    """
    new_or_updated_jobs: list[Any] = []
    for job in data:
        job["posted_date"], job["last_updated"], job["last_scrape"] = set_dates(job=job)
        job["job_path"] = f"https://amazon.jobs{job['job_path']}"
        job["date_off_market"] = "NA"
        if status := get_status(
            job_id=job["id_icims"],
            table=table,
            last_updated=job["last_updated"],
            last_scrape=job["last_scrape"],
        ):
            if status == "new":
                job["type"] = "scrape_record"
                store_job(job=job, table=table)
                new_or_updated_jobs.append(job)
            elif status == "updated":
                if changes := get_diff(new_job=job, table=table):
                    job["changes"] = changes
                    store_job(job=job, table=table)
                    try:
                        new_or_updated_jobs.append(
                            table.get_item(Key={"id_icims": job["id_icims"]})
                        )
                    except ClientError as e:
                        log_client_error(error=e, func="update_and_store_jobs")
    return new_or_updated_jobs


def find_newest_scrape(table) -> datetime:
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
    new_jobs: list[dict[str, str | int | dict | list | None]],
) -> list[dict[str, str | int | dict | list | None]]:
    """
    Filters the given list of new jobs and keeps only the specified keys in each job dictionary.

    Args:
        new_jobs (list[dict[str, str | int | dict | list | None]]): A list of dictionaries representing new jobs.

    Returns:
        dict: A dictionary containing the filtered job dictionaries with only the specified keys.

    """
    keepers = []
    for job in new_jobs:
        new = {
            k: job[k]
            for k in job.keys()
            if k
            in [
                "id_icims",
                "title",
                "company",
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
    newest_scrape: datetime = newest_scrape if isinstance(newest_scrape, datetime) else datetime.fromisoformat(newest_scrape)
    if new_jobs := update_and_store_jobs(data=jobs, table=table):
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
                "new_jobs": new_jobs,
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
        logger.error(f"Error occurred in Lambda var_replacer: {str(e)}")

        return {
            "status": {
                "statusCode": 500,
                "state": "InvokeJobStore",
                "errorFunc": context.function_name,
                "errorType": type(e).__name__,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc(),
            },
            "data": get_data(event=event),
        }
