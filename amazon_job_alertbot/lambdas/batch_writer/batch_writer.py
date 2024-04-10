import logging
import time
import traceback

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterator

import boto3

from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()
config = Config(retries={"max_attempts": 10, "mode": "adaptive"})


def log_client_error(error: ClientError, func: str) -> None:
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
    Retrieves a dictionary for key from event.

    Args:
        event (dict[Any, Any]): A dictionary to parse.

    Returns:
        dict[Any, Any]: The parsed dictionary.

    """
    payload = event.get("Payload", {}) or event
    return payload.get("data", {})


def execute_threads(table: Any, update_requests: list[dict[str, Any]]) -> list[Any]:
    """
    Execute multiple threads to write updates to a DynamoDB table.

    Args:
        table: The DynamoDB table to write items to.
        update_requests (list[dict[str, Any]]): List of put requests to write to the table.

    Returns:
        list[Any]: List of responses from the write operations.
    """

    with ThreadPoolExecutor() as executor:
        responses: Iterator[Any] = executor.map(
            update, iter(update_requests), [table] * len(update_requests)
        )

    return [
        response.get("Attributes")
        for response in list(responses)
        if (response and response.get("Attributes"))
    ]


def update(table: Any, update_request: dict[str, str | bool]) -> dict[str, Any]:
    """
    Update an item in a DynamoDB table.

    This function updates an item in a DynamoDB table based on the provided update request.

    Args:
        table: The DynamoDB table to update the item in.
        update_request (dict[str, str | bool]): The update request containing the item to update and the update expression.

    Returns:
        dict[str, Any]: The response from the update operation.
    """
    update_expression = ", ".join(
        [
            f"SET {k} = {v}" if v is not True else ":sentval"
            for k, v in update_request.items()
            if k != "id_icims"
        ]
    )
    attribute_values: dict[str, bool] = {":sentval": True}
    logger.debug(
        f"Updating item with id_icims {update_request['id_icims']} using update expression {update_expression}"
    )
    try:
        return table.update_item(
            Key=update_request["id_icims"],
            UpdateExpression=update_expression,
            ExpressionAttributeValues=attribute_values,
            ReturnValues="UPDATED_NEW",
        )
    except ClientError as e:
        log_client_error(e, "update")
        if e.response["Error"]["Code"] not in [
            "RequestLimitExceeded",
            "Throttling",
        ] and e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") not in [
            429,
            503,
        ]:
            raise e
        time.sleep(10)
        return table.update_item(
            Key=update_request["id_icims"],
            UpdateExpression=update_expression,
            ReturnValues="UPDATED_NEW",
        )


def update_batch(event: dict[str, Any]) -> list[Any]:
    """
    Updates a batch of items in a DynamoDB table based on the provided event data.

    Args:
        event (dict): The event data containing information needed to update the items.

    Returns:
        list: A list of responses from updating the items in the DynamoDB table.

    Raises:
        ClientError: If there is an error during the batch update process.
    """

    data = get_data(event)
    responses = []
    table = boto3.resource("dynamodb", config=config).Table(data["TableName"])
    if update_requests := data.get("UpdateRequests", []):
        try:
            responses = execute_threads(table, update_requests)

        except ClientError as e:
            log_client_error(e, "batch_writer")
            raise e
    return responses


def batch_writer_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Handle the batch writing process based on the event data.

    Args:
        event: Dictionary containing event data.
        context: Context object for the Lambda function.

    Returns:
        dict[str, Any]: Response containing status code, state, and data after batch writing.
    """
    logger.debug(f"Beginning batch writer lambda with event:\n{event}")
    try:
        batch = update_batch(event)
        logger.debug(f"Batch processing finished. Batch writer response:\n{batch}")
        return {
            "status": {"statusCode": 200, "state": "InvokeBatchWriter"},
            "data": batch,
        }

    except Exception as e:
        logger.error(f"Error in {context.function_name}: {e}")
        logger.error("State: InvokeBatchWriter")
        logger.error(traceback.format_exc())
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Entry event for error: {event}")
        raise e
