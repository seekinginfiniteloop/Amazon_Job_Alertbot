import logging
import traceback
from logging import Logger
from typing import Any

logger: Logger = logging.getLogger()
logger.setLevel(level=logging.ERROR)

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
        "HandledErrors": [],
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
    errors = event.get("errorInfo", {})
    state = errors.get("state", "Unknown State")
    error_message = errors.get("errorMessage", "error message not found")
    error_type = errors.get("errorType", "error type not found")

    if error_type in retryable_errors:
        return {
            "status_code": 200,
            "state": state,
            "action": "retry",
            "error_details": (
                f"error_handler handled error {error_type} from {state}."
                "Retrying."
                f"Error details: {error_message}."
            ),
        }
    return {
        "status_code": 200,
        "state": state,
        "action": "end",
        "error_details": (
            f"terminating based on error {error_type}."
            f"Error details:\n error message: {error_message}\n\n stack trace:"
            f"{errors.get("stackTrace")}"
        ),
    }


def error_handler_handler(
    event: dict[str, Any], context: dict[str, Any]
) -> dict[str, Any]:
    """
    Handles errors in the Lambda function and returns the appropriate response.

    Args:
        event: The event dictionary containing the error information.
        context: The context dictionary containing additional information.

    Returns:
        dict[str, Any]: A dictionary with the response data, including the status code, action, and error details.
    """
    logger.info(f"Error Handler invoked with event: {event}")
    try:
        response = handle_error(event=event)
        logger.info(
            "Error handled. Occurred in "
            f"{event.get('errorInfo').get('errorState')}"
            f": {event.get('errorInfo')}. Routing to action: {response.get("action", "action not found")}"
        )
        return response

    except Exception as e:
        error = dict(
            zip(
                ["errorType", "errorMessage", "stackTrace"],
                [type(e).__name__, str(e), traceback.format_exc()],
            )
        )
        logger.exception(
            f"An error occurred when attempting to handle error in error_handler Lambda. Error: {e}\n\nError details: {error}"
        )
        return {
            "status_code": 500,
            "action": "end",
            "error_details": (
                f"An error occurred when attempting to handle error in error_handler Lambda. Error: {e}.\n\n"
                f"Error details: {error}. Error occurred while trying to handle "
                f"{event.get('errorInfo', {}).get('errorType')} from {event.get('errorState')} with message "
                f"{event.get('errorInfo', {}).get('errorMessage')}"
            ),
        }
