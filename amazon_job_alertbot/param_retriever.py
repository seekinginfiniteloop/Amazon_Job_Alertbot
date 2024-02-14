import json
import logging
from typing import Any

import boto3

logger = logging.getLogger(name="parameter_retriever")
logger.setLevel(level=logging.INFO)


def get_parameters_by_path(path_prefix: str) -> dict[Any, Any]:
    """
    Retrieves AWS SSM parameters that have names starting with the specified path prefix.

    Args:
    path_prefix : string for the ParamStore path_prefix to retrieve parameters from.

    Returns:
    dict containing parameters
    """
    ssm = boto3.client("ssm")
    paginator = ssm.get_paginator("get_parameters_by_path")
    parameters = {}

    for page in paginator.paginate(
        Path=path_prefix, Recursive=True, WithDecryption=False
    ):
        for param in page["Parameters"]:
            name = str(param["Name"]).split("/")[-1]
            try:
                parameters[name] = json.loads(param["Value"])
            except json.JSONDecodeError:
                logging.info(f"failed to parse key {name}, setting to existing value")
                parameters[name] = param["Value"]
                continue

    return parameters


def lambda_handler(event, context) -> dict[str, int | str | None]:
    """
    Lambda function handler for retrieving parameters from Systems Manager ParamStore.

    Args:
    event: payload containing path_prefix to fetch parameters.

    Returns:
    dict containing status code and JSON string of parameters if found
    """
    logger.info(f"Received event: {event}")
    logger.info("Retrieving parameters from Systems Manager ParamStore")
    path_prefix = event.get("path_prefix", "/*")
    logger.info(f"Retrieving parameters with path prefix: {path_prefix}")
    if params := get_parameters_by_path(path_prefix=path_prefix):
        return {"statusCode": 200, "payload": params}
    logger.info("Parameters retrieved successfully. Returning params")

    return {"statusCode": 404, "payload": None}
