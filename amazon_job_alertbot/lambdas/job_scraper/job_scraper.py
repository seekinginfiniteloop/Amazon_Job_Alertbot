import logging
import re
import time
import traceback

from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Any
from urllib.parse import urlencode

import requests

from requests import Response, Session

logger: Logger = logging.getLogger()


def get_data(
    event: dict[str, Any],
) -> dict[str, Any]:
    """
        Retrieves a dictionary for key
        from event.
    from functools import partial

        Args:
            event (dict[Any, Any]): A dictionary to parse.

        Returns:
            dict[Any, Any]: The parsed dictionary.

    """
    payload = event.get("Payload", {}) or event
    return payload.get("data", {})


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
    data = get_data(event)
    params = data.get("searchparams", {})
    logger.debug(f"params: {params}")
    remaining_hits = data.get("remaining_hits")
    newest_scrape = data.get("newest_scrape")
    if next_offset := data.get("next_offset"):
        params["criteria"]["offset"] = next_offset

    return params, remaining_hits, newest_scrape


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

    query_params = {}
    for key, values in facets.items():
        if not values:
            continue
        new_key = f"{key}[]"
        query_params[new_key] = values

    for key, value in criteria.items():
        query_params[key] = value or ""

    query_string: str = urlencode(query=query_params, doseq=True)
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
    logger.debug(f"search_params: {search_params}")
    lang_code = search_params.get("lang_code", "en")
    facets = search_params["facets"]
    criteria = search_params["criteria"]
    headers = search_params["headers"]
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
    try:
        logger.debug(f"Fetching response from url: {url} with headers: {headers}")
        response: Response = session.get(url=url, headers=headers, timeout=30)
        if response.status_code != 200:
            logger.error(
                f"Error: Received status code {response.status_code} from the server."
            )
        try:
            logger.debug(f"Response: {response.json(strict=False)}")

            return response.json(strict=False)
        except ValueError as e:
            logger.exception(f"Error: Unable to parse JSON response. {e}")
            return None
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        logger.exception(
            f"Error: Connection or timeout error. Routing to handler for retry: {e}"
        )
        return e
    except requests.exceptions.RequestException as e:
        logger.exception(f"Error: Unexpected error. {e}")
        raise e

    finally:
        session.close()


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
    if new_offset := criteria.pop("next_offset", None):
        criteria["offset"] = new_offset if new_offset >= offset else criteria["offset"]

    logger.debug(f"Establishing session with {base_url}")
    try:
        session.get(url=base_url, headers=init_headers)
        search_url: str = gen_search_url(
            url=f"{base_url}/search.json", facets=facets, criteria=criteria
        )
        all_jobs = []
        if data := fetch_job_data(url=search_url, session=session, headers=headers):
            if isinstance(data, Exception):
                return data, None, None

            remainder: int = max(
                remaining_hits - int(criteria["result_limit"])
                if remaining_hits
                else (int(data["hits"]) - len(data)),
                0,
            )
            all_jobs.extend(data["jobs"])
        return all_jobs, remainder, (criteria["offset"] + criteria["result_limit"])

    finally:
        session.close()


def set_dates(
    jobs: list[dict[str, Any]], limit_date: str | datetime | None = None
) -> tuple[list[dict[str, str | int | datetime | None]], datetime]:
    """
    Sets and adjusts dates for job postings based on the provided jobs data and a limit date.

    Args:
        jobs (list): A list of job postings with date information to be processed.
        limit_date (datetime): A limit date to be used for date adjustments.

    Returns:
        tuple: A tuple containing the updated job postings list and the adjusted limit date.
    """

    now_time = datetime.now(timezone.utc)
    if limit_date:
        limit_date = (
            datetime.fromisoformat(limit_date)
            if isinstance(limit_date, str)
            else limit_date
        )
        limit_date = (
            limit_date if limit_date.tzinfo else limit_date.replace(tzinfo=timezone.utc)
        )
    if not jobs:
        return ([], limit_date) if limit_date else ([], None)
    for job in jobs:
        if posted := job.get("posted_date"):
            job["posted_date"] = datetime.strptime(posted, "%B %d, %Y").replace(
                tzinfo=timezone.utc
            )
        if updated := job.get("updated_time"):
            if time_match := re.search(
                r"(\d{1,3})\s(day|hour|min|month|week)", updated
            ):
                time_value, time_unit = int(time_match[1]), time_match[2]
                time_delta_args = {
                    "day": "days",
                    "hour": "hours",
                    "min": "minutes",
                    "week": "weeks",
                    "month": "days",
                }
                if time_unit == "month":
                    time_value *= 30
                last_updated = now_time - timedelta(
                    **{time_delta_args[time_unit]: time_value}
                )
            job["last_updated"] = (
                last_updated
                if last_updated >= job["posted_date"]
                else job["posted_date"]
            )
        if not updated:
            job["last_updated"] = job["posted_date"]
    return (jobs, limit_date) if limit_date else (jobs, None)


def stringify_dates(job: dict[str, Any]) -> dict[str, Any]:
    """
    Convert all date fields to strings.

    Args:
        job: The job dictionary to convert.

    Returns:
        The job dictionary with all date fields converted to strings.
    """
    return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in job.items()}


def check_for_stop_signal(
    jobs: list[dict[str, str | int | None]],
    remaining_hits: int = 0,
    limit_date: datetime | None = None,
) -> bool:
    """
    Checks if the stop signal should be set based on the provided data, remaining hits, and limit date.

    Args:
        jobs (list[dict[str, str | int | None]]): The data to check for stop signal.
        remaining_hits (int): The remaining hits to check for stop signal.
        limit_date (datetime.date): The limit date to check for stop signal.

    Returns:
        bool: True if the stop signal should be set, False otherwise.
    """
    logger.debug(f"Checking for stop signal with {remaining_hits} remaining hits")
    stop_signal = False
    if limit_date:
        logger.debug(f"Checking for stop signal with limit date {limit_date}")
    if jobs and remaining_hits and limit_date:
        updated: list[datetime] = [job["last_updated"] for job in jobs]
        stop_signal = any(date < limit_date for date in updated)
    elif remaining_hits and jobs:
        stop_signal = False
    return stop_signal


def retry(params: dict[str, Any], remaining_hits: int) -> tuple[list[dict[str, str | int | None]] | None, dict[str, int | None], int]:
    """
    Retries fetching jobs if a timeout or connection error occurs.

    Args:
        params (dict): The search parameters for fetching jobs.
        remaining_hits (int): The number of remaining API hits.

    Returns:
        tuple: A tuple containing the fetched jobs dictionaries, remainder, and next offset.
    """

    logger.warning("Received timeout or connection error. Retrying in 10 seconds.")
    time.sleep(10)
    logger.info("Retrying scrape.")
    jobs, remainder, next_offset = fetch_jobs(
        search_params=params, remaining_hits=remaining_hits
    )

    return jobs, remainder, next_offset


def scrape(
    event: dict[str, Any], context
) -> dict[str, int | list[dict[str, str | int | None]] | Any]:
    """
    Scrapes jobs based on the provided event parameters.

    Args:
        event: A dictionary containing the event parameters.
        context: The context object.

    Returns:
        dict: A dictionary containing the scraped jobs, remaining hits, and next offset.

    """
    jobs, remainder, next_offset, stop_signal, jobs_found = [], 0, 0, False, 0
    params, remaining_hits, limit_date = set_vars(event=event)
    jobs, remainder, next_offset = fetch_jobs(
        search_params=params, remaining_hits=remaining_hits
    )
    if isinstance(
        jobs, (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError)
    ):
        jobs, remainder, next_offset = retry(params, remaining_hits)
    jobs, limit_date = set_dates(jobs, limit_date)
    if jobs:
        stop_signal: bool = check_for_stop_signal(jobs, remainder, limit_date)
    remainder: int = 0 if stop_signal else remainder
    jobs_found: int = len(jobs)
    if jobs and stop_signal:
        logger.info(
            f"Scrape found {jobs_found} new jobs; returning to state machine"
            "with stop signal"
        )
        jobs = [job for job in jobs.copy() if job["last_updated"] > limit_date]
    elif stop_signal:
        logger.info(
            "Scrape found no new jobs, informing state machine with stop signal"
        )
    elif jobs:
        logger.info(
            f"Scrape found {jobs_found} new jobs with {remaining_hits}"
            "remaining; returning to state machine"
        )
    else:
        logger.info("Scrape found no new jobs, informing state machine")
    if jobs:
        jobs = [stringify_dates(job) for job in jobs.copy()]
    return {
        "status": {"statusCode": 200, "state": "InvokeJobScraper"},
        "data": {
            k: v
            for k, v in get_data(event=event).items()
            if k not in ["jobs", "next_offset", "remaining_hits"]
        }
        | {
            "jobs_found": jobs_found,
            "remaining_hits": remainder,
            "jobs": jobs,
            "next_offset": next_offset,
        },
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
    logger.debug(f"Scrape function execution started with event: \n {event}")

    try:
        return scrape(event=event, context=context)

    except Exception as e:
        logger.error(f"Error in {context.function_name}: {e}")
        logger.error("State: InvokeJobScraper")
        logger.error(traceback.format_exc())
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Entry event for error: {event}")
        raise e
