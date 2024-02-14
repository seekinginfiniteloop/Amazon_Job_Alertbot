import logging
import re
from datetime import datetime, timedelta, timezone
from logging import Logger

import requests
from requests import Response, Session

logger: Logger = logging.getLogger(name="jobs_scraper")
logger.setLevel(level="DEBUG")


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
    Creates a search URL for Amazon.jobs based on the provided base URL, facets, and criteria.

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
    search_params: dict[str, str | int | None],
) -> (
    tuple[list[dict[str, str | int | None]], dict[str, int | None]] | tuple[None, None]
):
    """
    Fetch all jobs from the specified base URL using the provided headers, facets,
    criteria, database name, table name, and language code.

    The function initializes a session, sets initial headers, and retrieves the initial
    page to establish a session. It then iteratively fetches job data, extracts and
    stores the jobs, and updates the criteria to fetch the next page.
    The function continues this process until all jobs are fetched or no new jobs are
    found. Finally, it stores the last scrape time, logs the number of collected jobs,
    and returns all the collected jobs.

    Args:
        search_params: Search parameters for the scrape.

    Returns:
        tuple containing:
        - A list of dictionaries representing the fetched jobs.
        - remaining hits

    """
    facets, criteria, headers, base_url, session, init_headers = set_params(
        search_params=search_params
    )
    logger.info(f"Establishing session with {base_url}")
    session.get(url=base_url, headers=init_headers)
    search_url: str = gen_search_url(
        url=f"{base_url}/search.json", facets=facets, criteria=criteria
    )
    if data := fetch_job_data(url=search_url, session=session, headers=headers):
        remaining_hits = int(data["hits"]) - (
            (int(criteria["offset"]) + 1) * (int(criteria["result_limit"]))
        )
        all_jobs = []
        all_jobs.extend(data["jobs"])
        return all_jobs, remaining_hits
    logger.info("Scrape returned with no data for provided search criteria.")
    return None, None


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
        if match := re.match(pattern=r"^(\d{1-3})", string=updated_time_str):
            updated_days = int(match[0])
        else:
            updated_days = 0
        return datetime.now(timezone.utc).date() - timedelta(days=updated_days)
    return posted_date


def fetch_to_date(
    search_params: dict[str, str | int | None],
    remaining_hits: int | str,
    limit_date: str,
) -> tuple[list[dict[str, str | int | None]] | None, int, bool]:
    limit_date = datetime.fromisoformat(limit_date)
    remaining_hits = int(remaining_hits)
    search_params["criteria"]["offset"] += min(remaining_hits, 10)
    stop_signal = False
    data, _ = fetch_jobs(search_params=search_params)
    if data:
        for job in data:
                update_date = get_date_updated(job=job)
                if update_date < limit_date:
                    stop_signal = True
                    break
        remaining_hits -= min(remaining_hits, 10)
    else:
        stop_signal = True
    return data, remaining_hits, stop_signal


def lambda_handler(
    event, context
) -> dict[str, list[dict[str, str | int | None]] | str | int | None]:
    """
    Lambda function handler that fetches job data from Amazon.jobs and sends an email.

    Args:
        event: The event data passed to the Lambda function. Event includes the search params for the scrape sent by the State Machine.
        context: The runtime information of the Lambda function.

    Returns:
        dict containing list of jobs as dicts, remaining hits, and stop_signal.
    """
    logger.info("Scrape function execution started")
    stop_signal = False
    if payload := event.get("Payload"):
        data, remaining_hits, stop_signal = fetch_to_date(**payload)
    else:
        data, remaining_hits = fetch_jobs(search_params=event.get("params"))

    jobs = data or []
    remaining_hits: int = 0 if stop_signal else remaining_hits
    response = {"jobs": jobs, "remaining_hits": remaining_hits}

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
    return response
