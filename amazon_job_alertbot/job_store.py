import contextlib
import difflib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(name="amzn_job_store")
logger.setLevel(level="DEBUG")


today = datetime.now(timezone.utc).date()
today_str: str = today.isoformat()

def scan_all_items(table_name, key_to_check) -> list[Any]:
    """
    Scans all items in a DynamoDB table and returns a list of items using a Paginator.

    Args:
        table_name: The name of the DynamoDB table to scan.
        key_to_check: The key to include in the projection expression.

    Returns:
        list[Any]: A list of items scanned from the table.
    """
    dynamodb = boto3.client('dynamodb')
    paginator = dynamodb.get_paginator('scan')

    all_items = []
    for page in paginator.paginate(TableName=table_name, ProjectionExpression=key_to_check):
        all_items.extend(page.get('Items', []))

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
        from_lines=old,
        to_lines=new,
        fromdesc="Old Ad",
        to_desc="New Ad",
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
        logger.info(f"Item not in database, {e}, storing as new job")
        store_job(job=new_job, table=table)
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
    job_id: int, table, last_updated: datetime.date
) -> Literal["new", "updated"] | None:
    """
    Get the status of the job with the specified ID.

    The function checks if the job exists in the table. If it does not exist, the status is "new".
    If the job exists and its last scraped time is greater than the provided last scraped time,
    the status is "updated". Otherwise, the status is None.

    Args:
        job_id: The ID of the job.
        table: The table object to perform the get operation on.
        last_scrape: The last scraped time to compare against.

    Returns:
        The status of the job, which can be "new", "updated", or None.

    """

    try:
        response = table.get_item(Key={"id_icims": job_id})
        if "Item" not in response or not response:
            return "new"
        if last_scrape := response["Item"]["last_scrape"]:
            if datetime.fromisoformat(last_scrape) < last_updated:
                return "updated"
    except ClientError as e:
        logger.exception(e.response["Error"]["Message"])
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
    try:
        job = stringify_dates(job=job)
        table.put_item(Item=job)
        logger.info(f"Stored job with ID {job['id_icims']}")
    except ClientError as e:
        logger.exception(e.response["Error"]["Message"])


def set_dates(job: dict[str, Any]) -> datetime.date:
    """
    Sets the dates for a job.

    Args:
        job (dict): The job dictionary containing information about the job.

    Returns:
        Tuple[datetime.date, datetime.date, datetime.date]: A tuple containing the posted date, last updated date (or posted date if unavailable), and the current date.
    """

    posted_date: datetime = parse_date(job["posted_date"])
    if updated_time_str := job.get("updated_time"):
        updated_time = int(updated_time_str.replace(" days", "").strip()) if (updated_time_str.endswith("days") or updated_time_str.endswith("day")) else 0
        last_updated = datetime.now(timezone.utc).date() - timedelta(days=updated_time)
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


def update_and_store_jobs(data: dict[str, str | int | dict | list], table) -> list[dict[str, str | int | dict | list | None]] | None:
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
        if status := get_status(
            job_id=job["id_icims"], table=table, last_updated=job["last_updated"]
        ):
            if status == "new":
                store_job(job=job, table=table)
                new_or_updated_jobs.append(job)
            elif status == "updated":
                if changes := get_diff(new_job=job, table=table):
                    job["changes"] = changes
                    store_job(job=job, table=table)
                    new_or_updated_jobs.append(
                        table.get_item(Key={"id_icims": job['id_icims']})
                    )
    return new_or_updated_jobs

def find_newest_scrape(table) -> datetime:
    """
    Finds the newest last_scrape date that isn't today. We use this for finding how far back to go with subsequent scrapes.

    Args:
        table: dynamodb table

    Returns:
        datetime: The capture limit.
    """
    has_data = table.scan(ProjectionExpression= 'id_icims', Limit=1):
        if not 'Items' in has_data or len(has_data['Items']) == 0:
        # if db empty, we go back up to 1.5 yrs - with 'Hire and Develop the Best' as a
        # core principle, some jobs can stay on the market for a long time while HMs
        # wait for the perfect candidate.
            return today - timedelta(days = 540)

        response = table.scan(
        IndexName='last_scrape-index',
        ProjectionExpression='last_scrape',
        ScanIndexForward=False,  # False for descending order
        Limit=1
    )

    items = response.get('Items', [])
    if not items:
        return today - timedelta(days = 540)

    return datetime.fromisoformat(items[0]['last_scrape']).date()


def clean_db(table) -> None:
    # keeping this here as a TODO, but I plan to implement in a separate function
    pass


def lambda_handler(event, context) -> dict[str, dict[]]:
    """
    Lambda function handler that fetches job data from Amazon.jobs and sends an email.

    Args:
        event: The event data passed to the Lambda function.
        context: The runtime information of the Lambda function.

    Returns:
        None
    """

    logger.info("job_store function execution started")
    db = boto3.resource("dynamodb")
    (
        db_param,
        jobs,
        remaining_hits,
    ) = event[:2]
    table = db.Table(db_param["table"])
    more_jobs = False
    newest_scrape = event[3] or find_newest_scrape(table=table)
    if new_jobs := update_and_store_jobs(data=jobs, table=table):
        /
        if (
            len(new_jobs) == len(jobs)
            or min(
                datetime.fromisoformat(date)
                for date in new_jobs["last_updated"]
            )
            >= newest_scrape
        ) and remaining_hits:
            more_jobs = True
            logger.info("Either all results were new/updated or last_updated date precedes earliest scraped date. Sending signal to get more jobs.")
        if not more_jobs:
            logger.info(
                "All available new/updated jobs have been stored in the database"
            )
        logger.info(
            f"Job Store execution completed. New/updated jobs stored in database: {len(new_jobs)} stored or updated"
        )
        return {"new_jobs": new_jobs, "more_jobs": more_jobs, "newest_scrape": newest_scrape}
    logger.info(
        "Job Store function execution completed. No new/updated jobs found, informing state machine."
    )
    return {"new_jobs": [], "more_jobs": more_jobs, "newest_scrape": None}
