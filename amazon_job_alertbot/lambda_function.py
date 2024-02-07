import difflib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Literal

import boto3
import requests
from botocore.exceptions import ClientError
from requests import Response, Session

logging.basicConfig(level=logging.INFO)


def get_parameters_by_path(path_prefix: str) -> dict[Any, Any]:
    """
    Retrieves AWS SSM parameters that have names starting with the specified path prefix.

    Args:
        path_prefix: The prefix of the parameter names to filter.

    Returns:
        dict[Any, Any]: A dictionary containing the retrieved parameter names and their corresponding values.

    """

    ssm = boto3.client("ssm")
    paginator = ssm.get_paginator("get_parameters_by_path")
    parameters = {}

    for page in paginator.paginate(
        Path=path_prefix, Recursive=True, WithDecryption=False
    ):
        for param in page["Parameters"]:
            parameters[param["Name"]] = param["Value"]

    return parameters


def process_string(string: str) -> str:
    """
    Processes a string by removing leading and trailing whitespace and replacing
    space with a plus sign.

    :param string: String to be processed.
    :return: Processed string.
    """
    return (
        string.strip().replace(" ", "+").replace('"', "%22")
        if isinstance(string, str)
        else string
    )


def create_search_url(
    base_url: str,
    facets: dict[str, list[str | bool]],
    criteria: dict[str, int | str | list[str]],
) -> str:
    """
    Creates a search URL for Amazon.jobs based on the provided base URL, facets, and criteria.

    Args:
        base_url: The base URL for the search.
        facets: A dictionary of facets to include in the search URL.
        criteria: A dictionary of criteria to include in the search URL.

    Returns:
        str: The generated search URL.

    Examples:
        # Example usage
        url = create_search_url(
            base_url="https://www.amazon.jobs/search.json",
            facets={"location": ["Seattle", "New York"], "category": "Software Development"},
            criteria={"keywords": "Python", "experience_level": "Entry Level"},
        )
    """

    query_params = []

    for key, values in facets.items():
        if isinstance(values, list):
            query_params.extend(
                f"{key}%5B%5D={process_string(value)}" for value in values
            )
        else:
            query_params.append(f"facets%5B%5D={key}")

    for key, value in criteria.items():
        if isinstance(value, list):
            query_params.extend(f"{key}={process_string(v)}" for v in value)
        else:
            query_params.append(
                f"{key}={process_string(value)}" if value else f"{key}="
            )

    query_string = "&".join(query_params)
    return f"{base_url}?{query_string}"


def fetch_job_data(
    url: str, session: Session, headers: dict[str, str], lang_code: str
) -> Any | None:
    """
    Fetch job data from the specified URL using the provided session, headers, and language code.

    Args:
        url: The URL to fetch the job data from.
        session: The session object to use for the HTTP request.
        headers: The headers to include in the HTTP request.
        lang_code: The language code to use for the request.

    Returns:
        The fetched job data as a JSON object, or None if there was an error.

    """

    response: Response = session.get(url=url, headers=headers)
    if response.status_code != 200:
        logging.error(
            f"Error: Received status code {response.status_code} from the server."
        )
        return None

    try:
        return response.json(strict=False)
    except ValueError as e:
        logging.error(f"Error: Unable to parse JSON response. {e}")
        return None


def get_diff(old_string: str, new_string: str, context: int = 3) -> str:
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

    old: list[bytes] = old_string.encode(encoding="utf-8").splitlines(keepends=True)
    new: list[bytes] = new_string.encode(encoding="utf-8").splitlines(keepends=True)

    diff = list(difflib.unified_diff(a=old, b=new, n=context))

    return "".join(diff.decode("utf-8") for diff in diff)


def update_job(job, table) -> None:
    """
    Update the specified job in the table with the provided changes and last scraped time.

    Args:
        job: The job to update.
        table: The table object to perform the update on.

    Returns:
        None

    """

    table.update_item(
        Key={"id_icims": job["id_icims"]},
        UpdateExpression="SET #changes = :changes, last_scraped_time = :last_scraped_time",
        ExpressionAttributeNames={"#changes": "changes"},
        ExpressionAttributeValues={
            ":changes": job["changes"],
            ":last_scraped_time": datetime.now(tz=timezone.utc),
        },
    )
    logging.info(f"Updated job with ID {job['id_icims']}")


def store_job_with_diff(job: dict[str, str | int], table: str) -> None:
    """
    Store the job in the specified table with the differences between the existing job and the new job.

    If the job with the same ID already exists in the table, the function compares the values of each attribute
    between the existing job and the new job. If there are any differences, the changes are stored in the "changes"
    attribute of the job and the job is updated in the table. If the job does not exist, it is inserted into the table.

    Args:
        job: The job to store with differences.
        table: The table to store the job in.

    Returns:
        None

    """

    existing_job = get_existing_job(job_id=job["id_icims"], table=table)
    if existing_job is None:
        table.put_item(Item=job)
        return

    changes = {}
    for key, new_value in job.items():
        old_value = existing_job.get(key, "")
        if str(old_value) != str(new_value):
            changes[key] = get_diff(
                old_string=str(old_value), new_string=str(new_value)
            )

    if changes:
        job["changes"] = changes
        update_job(job=job, table=table)


def get_status(
    job_id: int, table, last_scraped_time
) -> Literal["new", "updated"] | None:
    """
    Get the status of the job with the specified ID.

    The function checks if the job exists in the table. If it does not exist, the status is "new".
    If the job exists and its last scraped time is greater than the provided last scraped time,
    the status is "updated". Otherwise, the status is None.

    Args:
        job_id: The ID of the job.
        table: The table object to perform the get operation on.
        last_scraped_time: The last scraped time to compare against.

    Returns:
        The status of the job, which can be "new", "updated", or None.

    """

    try:
        response = table.get_item(Key={"id_icims": job_id})
        today = datetime.now(tz=timezone.utc)
        if "Item" not in response:
            return "new"
        if response["Item"]["last_scraped_time"] > last_scraped_time:
            return "updated"
    except ClientError as e:
        print(e.response["Error"]["Message"])
    return None


def get_existing_job(job_id: int, table) -> Any | None:
    """
    Get the existing job with the specified ID from the table.

    The function retrieves the item from the table using the provided job ID as the key.
    If the item exists, it is returned. Otherwise, None is returned.

    Args:
        job_id: The ID of the job.
        table: The table object to perform the get operation on.

    Returns:
        The existing job item if it exists, or None if it does not.

    """

    try:
        response = table.get_item(Key={"id_icims": job_id})
        return response.get("Item")
    except ClientError as e:
        logging.error(e.response["Error"]["Message"])
        return None


def store_job(job, table) -> None:
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
        table.put_item(Item=job)
    except ClientError as e:
        logging.error(e.response["Error"]["Message"])


def store_last_scrape_time(table, last_scrape_time: datetime) -> None:
    """
    Store the last scrape time in the specified table.

    The function puts the last scrape time into the table as an item with the ID "last_scrape_time"
    and the provided timestamp value.

    Args:
        table: The table object to perform the put operation on.
        last_scrape_time: The timestamp of the last scrape time.

    Returns:
        None

    """
    table.put_item(
        Item={"id": "last_scrape_time", "timestamp": last_scrape_time.isoformat()}
    )


def get_last_scrape_time(table) -> datetime | None:
    """
    Get the last scrape time from the specified table.

    The function retrieves the item with the ID "last_scrape_time" from the table.
    If the item exists, the timestamp value is extracted and returned as a datetime object.
    If the item does not exist, None is returned.

    Args:
        table: The table object to perform the get operation on.

    Returns:
        The last scrape time as a datetime object, or None if it does not exist.

    """
    response = table.get_item(Key={"id": "last_scrape_time"})
    if "Item" in response:
        return datetime.fromisoformat(response["Item"]["timestamp"])
    return None


def extract_and_store_jobs(data, table) -> list[Any]:
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
    last_scraped_time: datetime | None = get_last_scrape_time(table=table)
    new_or_updated_jobs = []
    for job in data["jobs"]:
        if last_scraped_time:
            status: Literal["new", "updated"] | None = get_status(
                job_id=job["id_icims"], table=table, last_scraped_time=last_scraped_time
            )
            if status in ("new", "updated"):
                store_job(job=job, table=table)
                new_or_updated_jobs.append(job)
                if status == "updated":
                    store_job_with_diff(job=job, table=table)
        else:
            store_job(job=job, table=table)
            new_or_updated_jobs.append(job)
    return new_or_updated_jobs


def fetch_all_jobs(
    base_url: str,
    headers: dict[str, str],
    facets: dict[str, list[str | bool]],
    criteria: dict[str, int | str | list[str]],
    table_name: str,
    lang_code: str = "en",
) -> list[dict[str, str | int]]:
    """
    Fetch all jobs from the specified base URL using the provided headers, facets, criteria, database name,
    table name, and language code.

    The function initializes a session, sets initial headers, and retrieves the initial
    page to establish a session. It then iteratively fetches job data, extracts and
    stores the jobs, and updates the criteria to fetch the next page.
    The function continues this process until all jobs are fetched or no new jobs are
    found. Finally, it stores the last scrape time, logs the number of collected jobs,
    and returns all the collected jobs.

    Args:
        base_url: The base URL for job search.
        headers: The headers to include in the HTTP requests.
        facets: The facets to include in the job search.
        criteria: The criteria to include in the job search.
        table_name: The name of the table.
        lang_code: The language code for the job search. Defaults to "en".

    Returns:
        A list of dictionaries representing the fetched jobs.

    """

    session = requests.Session()
    init_headers = {
        key: headers.get(key, "keep-alive" if key == "Connection" else None)
        for key in ["User-Agent", "Connection"]
    }
    session.get(url=f"https://amazon.jobs/{lang_code}", headers=init_headers)
    all_jobs = []
    total_hits = None
    db = boto3.resource("dynamodb")
    table = db.Table(table_name)
    while total_hits is None or len(all_jobs) < total_hits:
        search_url = create_search_url(
            base_url=base_url, facets=facets, criteria=criteria
        )
        data = fetch_job_data(
            url=search_url, session=session, headers=headers, lang_code=lang_code
        )
        if not data:
            break
        total_hits = total_hits or data["hits"]

        new_jobs = extract_and_store_jobs(data=data, table=table)
        all_jobs.extend(new_jobs)

        if not new_jobs:
            break

        criteria["offset"] += 1
    store_last_scrape_time(table=table, last_scrape_time=datetime.now(tz=timezone.utc))
    logging.info(f"collected {len(all_jobs)} to send")
    return all_jobs


def send_email(
    jobs_data: list[dict[str, str | int]],
    sender_email: str,
    recipient_email: str,
    sender_arn: str,
    template_arn: str,
    **kwargs,
) -> None:
    """
    Send an email with the job data to the recipient email address.

    The function uses AWS Simple Email Service (SES) to send the email.
    It constructs the email subject, body text, and body HTML based on the provided job
    data. The email is sent to the recipient email address using the sender email
    address.

    Args:
        jobs_data: A list of dictionaries representing the job data.
        sender_email: The email address of the sender.
        recipient_email: The email address of the recipient.
        email_subject: The subject of the email.

    Returns:
        None

    """
    aws_region = os.environ["AWS_REGION"]

    ses_client = boto3.client("sesv2", region_name=aws_region)

    template_data = {
        "current_date": datetime.now().strftime("%d %B %y"),
        "jobs_count": len(jobs_data),
        "jobs_data": jobs_data,
    }
    try:
        response = ses_client.send_email(
            FromEmailAddress=sender_email,
            FromEmailAddressIdentityArn=sender_arn,
            Destination={
                "ToAddresses": recipient_email,
            },
            Content={
                "Template": {
                    "TemplateName": "AmznJobsNotice",
                    "TemplateArn": template_arn,
                    "TemplateData": json.dumps(template_data),
                }
            },
            ConfigurationSetName="amznjobsender",
        )
    except ClientError as e:
        logging.error(f"email send failed: {e.response['Error']['Message']}")
    else:
        logging.info("Email sent! Message ID:", response["MessageId"])


def lambda_handler(event, context) -> None:
    """
    Lambda function handler that fetches job data from Amazon.jobs and sends an email.

    Args:
        event: The event data passed to the Lambda function.
        context: The runtime information of the Lambda function.

    Returns:
        None
    """

    logging.info("Lambda function execution started")
    config = get_parameters_by_path(path_prefix="/amznalertbot/")
    for key, value in config:
        logging.info(f"captured paramters {key}: {value}")
    lang_code = config.get("lang_code") or "en"
    base_url = f"https://www.amazon.jobs/{lang_code}/search.json"
    if data := fetch_all_jobs(
        base_url=base_url,
        facets=config.get("facets", {}),
        criteria=config.get("criteria", {}),
        headers=config.get("headers", {}),
        table_name=config.get("db", {}).get("table_name"),
        lang_code=lang_code,
    ):
        send_email(jobs_data=data, **config.get("email", {}))
    logging.info("Lambda function execution completed")


lambda_handler(event=None, context=None)
