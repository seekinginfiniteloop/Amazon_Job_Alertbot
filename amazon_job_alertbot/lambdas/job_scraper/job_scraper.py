import logging
import re
import traceback
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Any

import requests
from requests import Response, Session

logger: Logger = logging.getLogger(name="job_scraper")
logger.setLevel(level="DEBUG")


def set_vars(event: dict[str, Any]) -> tuple[Any | None, Any]:
    """
    Sets the variables based on the given event dictionary.

    Args:
        event (dict[Any, Any]): A dictionary representing the event.

    Returns:
        tuple[Any | None, Any]: A tuple containing the values of
        "params" and "remaining_hits" from the event dictionary.
        If "remaining_hits" is not present, the value of
        "newest_scrape" is returned instead.

    """
    if payload := event.get("Payload)"):
        if payload.get("searchparams"):
            event = payload
    params = event.get("searchparams")
    params["criteria"]["offset"] = event.get("next_offset", 0)
    return params, event.get("remaining_hits"), event.get("newest_scrape")


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
    Session,
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


def fetch_job_data(
    url: str, session: Session, headers: dict[str, str]
) -> list[dict[str, str | None]] | None:
    """
    Fetch job data from the specified URL using the provided session, headers, and
    language code.

    Args:
        url: The URL to fetch the job data from.
        session: The session object to use for the HTTP request.
        headers: The headers to include in the HTTP request.

    Returns:
        The fetched job data as a JSON object, or None if there was an error.

    """

    response: Response = session.get(url=url, headers=headers)
    if response.status_code != 200:
        logger.error(
            f"Error: Received status code {response.status_code} from the server."
        )
        return None

    try:
        return response.json(strict=False)
    except ValueError as e:
        logger.exception(f"Error: Unable to parse JSON response. {e}")
        return None


def fetch_jobs(
    search_params: dict[str, str | int | None], remaining_hits: int = 0, offset: int = 0
) -> (
    tuple[list[dict[str, str | int | None]], dict[str, int | None]] | tuple[None, None]
):
    """
    Fetch all jobs from the specified base URL using the provided
    headers, facets, criteria, database name, table name, and
    language code.

    The function initializes a session, sets initial headers, and
    retrieves the initial page to establish a session. It then
    iteratively fetches job data, extracts and stores the jobs,
    and updates the criteria to fetch the next page. The function
    continues this process until all jobs are fetched or no new
    jobs are found. Finally, it stores the last scrape time, logs
    the number of collected jobs, and returns all the collected
    jobs.

    Args:
        search_params: Search parameters for the scrape.
        remaining_hits: The number of remaining hits, if any.
        offset: The offset to use for the search. Defaults to 0.

    Returns:
        tuple containing:
        - A list of dictionaries representing the fetched jobs.
        - remaining hits
        - next_offset

    """
    facets, criteria, headers, base_url, session, init_headers = set_params(
        search_params=search_params
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


def get_date_updated(job: dict[str, str | int | None]) -> datetime.date:
    """
    Gets the date updated for the job if present, else sets it to the posted date.

    Args:
        job (dict[str, str | int | None]): The job dictionary containing information
        about the job.

    Returns:
        datetime.date: The update date of the job.
    """
    posted_date = datetime.strptime(job["posted_date"], "%B %d, %Y").date()
    if updated_time_str := job.get("updated_time"):
        if match := re.match(
            pattern=r"^(\d{1,3})\s[days]{3,4}$", string=updated_time_str
        ):
            updated_days = int(match[0])
        else:
            updated_days = 0
        return datetime.now(timezone.utc).date() - timedelta(days=updated_days)
    return posted_date


def check_for_stop_signal(
    data: list[dict[str, str | int | None]],
    remaining_hits: int = 0,
    limit_date: datetime.date | None = None,
) -> bool:
    """
    Checks if the stop signal should be set based on the provided data, remaining hits, and limit date.

    Args:
        data (list[dict[str, str | int | None]]): The data to check for stop signal.
        remaining_hits (int): The remaining hits to check for stop signal.
        limit_date (datetime.date): The limit date to check for stop signal.

    Returns:
        bool: True if the stop signal should be set, False otherwise.
    """
    logger.info(f"Checking for stop signal with {remaining_hits} remaining hits")
    stop_signal = False
    if limit_date:
        logger.info(f"Checking for stop signal with limit date {limit_date}")
    if data and remaining_hits and limit_date:
        dates = [get_date_updated(job=job) for job in data]
        stop_signal = any(date < limit_date for date in dates)
    elif remaining_hits and data:
        stop_signal = False
    return stop_signal


def scrape(
    event: dict[str, Any], context: dict[str, Any]
) -> dict[str, int | list[dict[str, str | int | None]] | Any]:
    stop_signal = False
    params, remaining_hits, limit_date = set_vars(event=event)
    data, remainder, next_offset = fetch_jobs(
        search_params=params, remaining_hits=remaining_hits
    )
    stop_signal: bool = check_for_stop_signal(
        data=data, remaining_hits=remainder, limit_date=limit_date
    )
    jobs = data or []
    remainder: int = 0 if stop_signal else remainder
    if data and stop_signal:
        logger.info(
            f"Scrape found {len(data)} new jobs; returning to state machine"
            "with stop signal"
        )
    elif stop_signal:
        logger.info(
            "Scrape found no new jobs, informing state machine with stop signal"
        )
    elif data:
        logger.info(
            f"Scrape found {len(data)} new jobs with {remaining_hits}"
            "remaining; returning to state machine"
        )
    else:
        logger.info("Scrape found no new jobs, informing state machine")
    return {
        "status_code": 200,
        "jobs": jobs,
        "remaining_hits": remainder,
        "next_offset": next_offset,
    }


def job_scraper_handler(
    event: dict[str, Any], context: dict[str, Any]
) -> dict[str, list[dict[str, str | int | None]] | str | int | None]:
    """
    Lambda function handler that fetches job data from Amazon.jobs and sends an email.

    Args:
        event: The event data passed to the Lambda function. Event
        includes the search params for the scrape sent by the
        State Machine.
        context: The runtime information of the Lambda function.

    Returns:
        dict containing list of jobs as dicts, remaining hits, and stop_signal.
    """
    logger.info("Scrape function execution started")

    try:
        return scrape(event=event, context=context)

    except Exception as e:
        logger.exception(f"Error occurred in Lambda var_replacer: {str(e)}")
        return {
            "status_code": 500,
            "state": "InvokeJobScraper",
            "errorFunc": context.get("function_name"),
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "stackTrace": traceback.format_exc(),
        }
