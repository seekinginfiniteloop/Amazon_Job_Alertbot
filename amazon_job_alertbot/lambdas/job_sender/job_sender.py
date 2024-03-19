import json
import logging
import re
import traceback
from datetime import datetime, timezone
from logging import Logger
from re import Match, Pattern
from typing import Any

import boto3

logger: Logger = logging.getLogger(name="sender")
logger.setLevel(level="DEBUG")

today = datetime.now(timezone.utc).date()

sqs = boto3.client("sqs")
sns = boto3.client("sns")


class Message:
    """
    Represents a message to be published to an SNS topic.

    Args:
        jobs: The jobs to include in the message.
        params: A dictionary containing parameters for the message.

    Attributes:
        jobs: The jobs included in the message.
        params: The parameters for the message.
        other_vars: A dictionary containing additional variables for replacement.
        topic_arn: The ARN of the SNS topic to publish the message to.
        subject: The subject of the message.
        default: The default content of the message.
        email: The email content of the message.
        sms: The SMS content of the message.

    Methods:
        message: Returns a dictionary containing different message formats.
        publish: Publishes the message to the specified SNS topic.
        replacer: Replaces placeholders in a string with corresponding values.
        assemble_message: Assembles a message by replacing placeholders in the provided strings.

    """

    def __init__(self, jobs: list[dict[str, Any]], params: dict[str, Any]) -> None:
        """
        Initializes a new Message instance.

        Args:
            jobs: The jobs to include in the message.
            params: A dictionary containing parameters for the message.
        """
        self.jobs: list[dict[str, Any]] = jobs
        self.params: dict[str, Any] = params
        self.other_vars: dict[str, str] = {
            "today": today.strftime(format="%d %B %y"),
            "job_count": str(len(self.jobs)),
        }
        self.topic_arn: str = params["topic_arn"]
        self.subject: str = self.assemble_message(params.get("subject", ""))
        self.default = self.assemble_message(params.get("default_intro", "")) + "".join(
            [self.assemble_message(params.get("default_entry", "")) for _ in jobs]
        )
        self.email = (
            self.assemble_message(params.get("email_intro", ""))
            + "".join(
                [self.assemble_message(params.get("email_entry", "")) for _ in jobs]
            )
            + self.assemble_message(params.get("email_outro", ""))
        )
        self.sms: str = "".join(
            [self.assemble_message(params.get("sms", "")) for _ in self.jobs]
        )

    @property
    def message(self) -> dict[str, Any]:
        """
        Returns a dictionary containing different message formats.

        Returns:
            dict[str, Any]: A dictionary with keys representing different message formats
            (default, email, sms, sqs) and corresponding values.
        """
        return {
            "default": self.default,
            "email": self.email,
            "sms": self.sms,
            "sqs": self.jobs,
        }

    def publish(self) -> Any:
        """
        Publishes the message to the specified SNS topic.

        Returns:
            Any: The response from the publish operation.
        """
        return sns.publish(
            topicArn=self.topic_arn,
            Message=self.message,
            MessageStructure="json",
        )

    def replacer(self, match: Match[str]) -> str:
        """
        Replaces a matched pattern with the corresponding value from `other_vars` or `self.jobs`.

        Args:
            match: A regular expression match object.

        Returns:
            str: The replacement value for the matched pattern.
        """

        key = match.group(1)
        if key in self.other_vars:
            return self.other_vars[key]

        job_values = [job.get(key, "") for job in self.jobs]
        return ", ".join(filter(None, job_values))

    def assemble_message(self, *parts: str) -> str:
        """
        Assembles a message by replacing placeholders in the provided strings.

        Args:
            *parts: The parts of the message to assemble.

        Returns:
            str: The assembled message.
        """
        placeholder_pattern: Pattern[str] = re.compile(pattern=r"\{\{(\w+)\}\}")

        def replace_placeholders(part: str) -> str:
            """
            Replaces placeholders in a string with corresponding values.

            Args:
                part: The string to replace placeholders in.

            Returns:
                str: The string with replaced placeholders.

            """
            return placeholder_pattern.sub(repl=self.replacer, string=part)

        return "".join(filter(None, map(replace_placeholders, parts)))


def retrieve_queue(params: dict[str, Any]) -> list[Any]:
    """
    Retrieve jobs from the Amazon.jobs queue.

    Args:
        params (dict): Dictionary containing the queue name and the number of jobs to retrieve.

    Returns:
        list: List of jobs retrieved from the queue.
    """
    queue_url = params["queue_url"]
    jobs = []
    logger.info("Polling Amazon jobs SQS queue")
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
            VisibilityTimeout=360,
        )

        messages = response.get("Messages", [])
        if not messages:
            break

        for message in messages:
            message_body = json.loads(message["Body"])
            jobs.extend(message_body)
            sqs.delete_message(
                QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
            logger.info(f"Retrieved and deleted message {message['ReceiptHandle']}")
    logger.info(
        f"Retrieved {len(jobs)} jobs from Amazon jobs queue. Checking for empty queue."
    )
    return jobs


def messages_in_queue(queue_url: str) -> bool:
    """
    Checks if there are any messages in the specified queue.

    Args:
        queue_url: The URL of the queue to check.

    Returns:
        bool: True if there are messages in the queue, False otherwise.
    """
    queue = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesDelayed",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )
    return any(queue["Attributes"].values())


def purge_queue(queue_url: str) -> None:
    """
    Purges the specified queue.

    Args:
        queue_url: The URL of the queue to purge.
    """
    logger.info("Purging Amazon jobs SQS queue")
    sqs.purge_queue(QueueUrl=queue_url)
    logger.info("Amazon jobs SQS queue purged")


def send_jobs(event: dict[str, Any]) -> dict[str, int]:
    """
    Sends jobs based on the provided event dictionary.

    Args:
        event: The event dictionary containing the necessary parameters.

    Returns:
        dict[str, int]: A dictionary with the status code indicating the result of the job sending.
    """
    params = event.get("sendparams", {})
    jobs = retrieve_queue(params=params)
    if messages_in_queue(queue_url=params["queue_url"]):
        jobs.extend(retrieve_queue(params=params))
    purge_queue(queue_url=params["queue_url"])
    Message(jobs=jobs, params=params).publish()
    logger.info("Lambda Sender execution completed")
    return {"status_code": 200}


def job_sender_handler(
    event: dict[str, Any], context: dict[str, Any]
) -> dict[str, int | str, dict[str, str]]:
    """
    Lambda function handler that sends fetched jobs using SNS.

    Args:
        event: The event data passed to the Lambda function.
        context: The runtime information of the Lambda function.

    Returns:
        dict with status and/or errors
    """

    logger.info(f"Lambda Sender execution started with event: \n {event}")
    try:
        return send_jobs(event=event)

    except Exception as e:
        logger.error(f"Error occurred in Lambda var_replacer: {str(e)}")
        return {
            "status_code": 500,
            "state": "InvokeSender",
            "errorType": type(e).__name__,
            "errorFunc": context.function_name,
            "errorMessage": str(e),
            "stackTrace": traceback.format_exc(),
        }
