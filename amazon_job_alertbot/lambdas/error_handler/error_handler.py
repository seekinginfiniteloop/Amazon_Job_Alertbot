import logging
import traceback
from logging import Logger
from typing import Any

logger: Logger = logging.getLogger()
logger.setLevel(level=logging.ERROR)

def get_payload(
    event: dict[str, Any],
) -> tuple[dict[str, Any]]:
    """
    Retrieves a tuple of dictionaries for data from the originating function, status report from the function, and Cause object if available.

    Args:
        event (dict[Any, Any]): A dictionary to parse.

    Returns:
        tuple[dict[Any, Any]]: The parsed dictionary.

    """
    payload = event.get("Payload", {}) or event
    return (payload.get("data", {}),, payload.get("status", {}), payload.get("cause", {}))


lambda_errors = {
    "ServiceErrors": {
        "InvocationErrors": [
            "AccessDeniedException",
            "ResourceNotFoundException",
            "InvalidRequestContentException",
            "InvalidRuntimeException",
        ],
        "ThrottlingErrors": ["TooManyRequestsException", "Rate Exceeded", "Throttling"],
        "AWSServiceLimits": [
            "ServiceLimitExceeded",
            "ConcurrentInvocationLimitExceeded",
        ],
    },
    "FunctionErrors": {
        "HandledErrors": ["ConnectionError", "ConnectTimeout"],
        "UnhandledErrors": {
            "TimeoutErrors": ["Task timed out", "Timeout"],
            "MemoryErrors": ["OutOfMemoryError", "Memory limit exceeded"],
            "RuntimeErrors": [
                "IndexError",
                "KeyError",
                "TypeError",
                "ValueError",
                "RuntimeError",
                "NameError",
                "ZeroDivisionError",
                "AttributeError",
                "ImportError",
                "SyntaxError",
                "IndentationError",
            ],
            "ConfigurationErrors": [
                "InvalidParameterValueException",
                "EnvironmentError",
                "HandlerNotFound",
            ],
        },
    },
    "LogAndMonitoringErrors": {
        "CloudWatchLogs": [
            "CloudWatchLogsException",
            "InsufficientPermissionsException",
        ]
    },
}
retryable_errors: list[str] = (
    lambda_errors["ServiceErrors"]["ThrottlingErrors"]
    + lambda_errors["ServiceErrors"]["AWSServiceLimits"]
    + lambda_errors["FunctionErrors"]["HandledErrors"]
    + lambda_errors["FunctionErrors"]["UnhandledErrors"]["TimeoutErrors"]
    + lambda_errors["FunctionErrors"]["UnhandledErrors"]["MemoryErrors"]
    + lambda_errors["LogAndMonitoringErrors"]["CloudWatchLogs"]
)


def handle_error(event) -> dict[str, int | Any | str]:
    """
    Handles the error information from the event and returns the appropriate response.

    Args:
        event: The event dictionary containing the error information.

    Returns:
        dict[str, int | Any | str]: A dictionary with the response data, including the status code, state, action, and error details.
    """
    data, status, cause = get_payload(event=event)
    if cause:
        logger.error(f"Function failed with reported Cause: \n {cause}")
    state = status.get("state", "Unknown State")
    error_message = status.get("errorMessage", "error message not found")
    error_type = status.get("errorType", "error type not found")
    logger.error(f"Error Handler handling error {error_type} from {state}. Received error message: \n {error_message}")
    if error_type in retryable_errors:
        status["action"] = "retry"
        status["error_details"] = (
                f"error_handler handled error {error_type} from {state}."
                "Retrying."
                f"Error details: {error_message}."
            ),
        status["statusCode"] = 200
        status["state"] = state
        return {'status': status | cause, 'data': data}

    status["action"] = "end"
    status["error_details"] = (f"terminating based on error {error_type}. Error details:\n error message: {error_message}\n\n stack trace:"
    f"{status.get("stackTrace")}")
    logger.error(status["error_details"])
    status = status | cause
    status["statusCode"] = 500
    status["state"] = f"ErrorHandler unhandled error from {state}"
    return status

def error_handler_handler(
    event: dict[str, Any], context: dict[str, Any]
) -> dict[str, Any]:
    """
    Handles errors in the Lambda function and returns the appropriate response.

    Args:
        event: The event dictionary containing the error information.
    context: The context object.

    Returns:
        dict[str, Any]: A dictionary with the response data, including the status code, action, and error details.
    """
    logger.debug(f"Error Handler invoked with event: \n {event}")
    try:
        return handle_error(event=event)


    except Exception as e:
        error = {
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "stackTrace": traceback.format_exc(),
        }
        data, status, cause = get_payload(event=event)
        error_details = f"An error occurred when attempting to handle error in HandleError state. Error: {e}\n\nError details: {error}\n\n "
        logger.exception(error_details)
        logger.exception(f"statusCode: 500, action: end, error_details: error_details, state: HandleError, error_handled_information: data: {data}, status_info: {status}, cause: {cause}")
        raise e
