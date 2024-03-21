import logging
import traceback
from datetime import datetime, timezone
from logging import Logger
from typing import Any

logger: Logger = logging.getLogger(name="var_replacer")
logger.setLevel(level="INFO")


def get_data(
    event: dict[str, Any],
) -> dict[str, Any]:
    """
    Retrieves a dictionary for the data key of the Payload
    from event.

    Args:
        event (dict[Any, Any]): A dictionary to parse.

    Returns:
        dict[Any, Any]: The parsed dictionary.

    """
    payload = event.get("Payload", {}) or event
    return payload.get("data", {})


def parse_arn(arn: str) -> tuple[str, str, str]:
    """
    Parses an ARN (Amazon Resource Name) and extracts the region and account ID.

    Args:
        arn: The ARN to parse.

    Returns:
        tuple[str, str]: A tuple containing the partition, region and account ID
        extracted from the function ARN.
    """
    parts: list[str] = arn.split(":")
    return parts[1], parts[3], parts[4]


def recursive_replace(
    obj: Any, replacements: dict[str, Any]
) -> str | dict[Any, Any] | list[Any] | Any:
    """
    Recursively traverse the object and replace placeholders in strings.

    Args:
    obj: object to recursively transverse

    Returns:
    object with placeholders replaced
    """
    if isinstance(obj, str):
        for key, value in replacements.items():
            obj: str = obj.replace(f"{{{{ {key} }}}}", value)
        return obj
    elif isinstance(obj, dict):
        return {
            k: recursive_replace(obj=v, replacements=replacements)
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [recursive_replace(obj=elem, replacements=replacements) for elem in obj]
    else:
        return obj


def replace_vars(event, context) -> dict[str, int | str | Any]:
    """
    Replaces variables in the given event dictionary with their corresponding values.

    Args:
        event (dict): The event dictionary containing the variables to be replaced.
        context (LambdaContext): The LambdaContext object containing additional information.

    Returns:
        dict[str, int | str | Any]: The modified event dictionary with variables
        replaced.
    """

    logger.debug(f"Event Data:\n {event}")
    data = get_data(event=event)
    replacements: dict[str, Any] = data.get("replacements", {})
    now = datetime.now(timezone.utc).date()
    replacements["today"] = now.strftime(format="%d %B %y")
    (
        replacements["partition"],
        replacements["region"],
        replacements["account_id"],
    ) = parse_arn(arn=context.invoked_function_arn)

    substituted_data = recursive_replace(
        obj={k: v for k, v in data.items() if k != "replacements"},
        replacements=replacements,
    )
    logger.info(f"Finished replacing variables. Substituted data: {substituted_data}")

    return {
        "data": dict(replacements, **substituted_data),
        "status": (
            {
                "statusCode": 200,
                "state": "ReplaceParams",
            },
        ),
    }
def var_replacer_handler(
    event: dict[str, Any], context: dict[str, Any]
) -> str | dict[str, Any] | list[Any] | Any:
    """
    Handles the Lambda event and performs replacements in the event object.

    Args:
        Any: The modified event object after performing replacements.
    """
    logger.info(f"Starting var_replacer Lambda with event:\n{event}")
    try:
        return replace_vars(event=event, context=context)

    except Exception as e:
        logger.error(f"Error occurred in Lambda var_replacer: {e}")

        return {
            "status": {
                "statusCode": 500,
                "state": "ReplaceParams",
                "errorType": type(e).__name__,
                "errorFunc": context.function_name,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc(),
            },
            "data": get_data(event=event),
        }
