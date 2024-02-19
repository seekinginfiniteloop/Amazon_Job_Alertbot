import logging
from datetime import datetime, timezone
from logging import Logger
from typing import Any

import boto3
import requests

logger: Logger = logging.getLogger(name="job_closer")
logger.setLevel(level="INFO")

today = datetime.now(timezone.utc).date()
today_str: str = today.isoformat()


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


def gen_search_url(
    url: str,
    facets: dict[str, list[str | bool]],
    criteria: dict[str, int | str | list[str]],
) -> str:
    """
    Creates a search URL for Amazon.jobs based on the provided
    base URL, facets, and criteria.

    Args:
        base_url: The base URL for the search.
        facets: A dictionary of facets to include in the search URL.
        criteria: A dictionary of criteria to include in the search URL.

    Returns:
        str: The generated search URL.

    Examples:
        # Example usage
        url = gen_search_url(
            base_url="https://www.amazon.jobs/search.json",
            facets={"location": ["Seattle", "New York"], "category": "Software
            Development"},
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
    return f"{url}?{query_string}"


def set_params(
    search_params: dict[str, str | int | None],
) -> tuple[
    dict[str, list[str | bool]],
    dict[str, int | str | list[str]],
    dict[str, str],
    str,
    requests.Session,
    dict[str, str],
]:
    """
    Sets the search parameters for the scrape.

    Args:
        search_params: A dictionary containing the search parameters.

    Returns:
        A tuple containing the search parameters with default values set.
    """
    lang_code = search_params.get("lang_code", "en")
    facets = search_params.get("facets", {})
    criteria = search_params.get("criteria", {})
    headers = search_params.get("headers", {})
    base_url = f"https://amazon.jobs/{lang_code}"
    session = requests.Session()
    init_headers = {"User-Agent": headers.get("User-Agent"), "Connection": "keep-alive"}
    return facets, criteria, headers, base_url, session, init_headers


def find_unclosed(table_name) -> list[Any]:
    dynamodb = boto3.client("dynamodb")

    scan_params = {
        "TableName": table_name,
        "FilterExpression": "attribute_not_exists(date_closed) OR date_closed = :empty",
        "ExpressionAttributeValues": {":empty": {"S": ""}},
    }

    response = dynamodb.scan(**scan_params)
    items = response.get("Items", [])

    return [item["id_icims"]["S"] for item in items]


def get_closed_jobs(query_str: str, event) -> list[str]:
    facets, criteria, headers, base_url, session, init_headers = set_params(
        search_params=event.get("searchparams", {})
    )
    logger.info(f"Establishing session with {base_url}")
    session.get(url=base_url, headers=init_headers)
    search_url: str = gen_search_url(
        url=f"{base_url}/search.json", facets=facets, criteria=criteria
    )
    all_jobs = []
    if data := fetch_job_data(url=search_url, session=session, headers=headers):
        remainder: int = max(
            remaining_hits - int(criteria["result_limit"])
            if remaining_hits
            else (int(data["hits"]) - len(data)),
            0,
        )
        all_jobs.extend(data["jobs"])
    return all_jobs, remainder, (criteria["offset"] + criteria["result_limit"])


def lambda_handler(event, context) -> None:
    logger.info("Starting Lambda job_closer execution")
    db = boto3.resource("dynamodb")
    table = db.Table(event.get("table_name", "amznjobs"))
    open_jobs = find_unclosed(table_name=table.name)
    query_str = "".join([f"{str(job)} OR " for job in open_jobs])[:-4]
    closed_jobs = get_closed_jobs(query_str=query_str, event=event)
