import json
import logging
import re
import sys
import traceback
from datetime import datetime, timezone
from logging import Logger
from re import Match, Pattern
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger: Logger = logging.getLogger(name="sender")
logger.setLevel(level="DEBUG")

today = datetime.now(timezone.utc).date()

config = Config(retries={"max_attempts": 10, "mode": "adaptive"})

sqs = boto3.client("sqs", config=config)
sns = boto3.client("sns", config=config)


def log_client_error(error: Exception, func: str) -> None:
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


def get_params(
    event: dict[str, Any],
) -> dict[str, Any]:
    """
    Retrieves a dictionary for key
    from event.

    Args:
        event (dict[Any, Any]): A dictionary to parse.

    Returns:
        dict[Any, Any]: The parsed dictionary.

    """
    return event.get("Payload", {}) or event


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
        logger.debug(
            f"initializing Message with {len(self.jobs)}, \n  params: {self.params}, \n  topic_arn: {self.topic_arn}"
        )
        self.subject: str = self.assemble_message(params.get("subject", ""))
        self.default: str = self.build_default()
        self.email = self.build_email()
        logger.debug(f"Message assembled email: {self.email}")
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
        return json.dumps(
            {
                "default": self.default,
                "email": self.email,
                "sms": self.sms,
                "sqs": self.jobs,
            }
        )

    def publish(self) -> Any:
        """
        Publishes the message to the specified SNS topic.

        Returns:
            Any: The response from the publish operation.
        """
        logger.debug(f"Publishing message to topic {self.topic_arn}")
        if sys.getsizeof(self.message) > 256000:
            logger.warning(
                "Message size exceeds 256KB; attempting to publish with extended publishing to S3."
            )
            return self.publish_to_s3()

        try:
            return sns.publish(
                TopicArn=self.topic_arn,
                Message=self.message,
                MessageStructure="json",
            )
        except ClientError as e:
            log_client_error(e, "publish")
            raise e

    def get_s3_params(self) -> tuple[Any, str]:
        """
        Returns the S3 parameters for publishing the message to S3.

        Returns:
            tuple[Any, str]: A tuple containing the S3 object, and tag key.
        """
        s3 = boto3.resource("s3")
        bucket_name = self.params["s3_bucket"]
        bucket_key_prefix = self.params["s3_key_prefix"]
        key = f"{bucket_key_prefix}message_{today.strftime(format='%Y%m%d')}.html"
        s3_object = s3.Object(bucket_name, key)
        tag_key = self.params["tag_key"]
        return s3_object, tag_key

    def save_to_s3(self, s3_object, tag_key) -> str:
        s3_object.put(
            Body=self.email,
            ContentType="application/json",
            BucketKeyEnabled=True,
            Tagging=f"{tag_key}={tag_key}",
        )
        logger.debug(
            f"Message published to S3 bucket {s3_object.bucket_name} with key {s3_object.key}."
        )
        return s3_object.get()["ResponseMetadata"]["HTTPHeaders"]["location"]

    def extended_message(self, s3_location) -> str:
        """
        Returns a message indicating that the job notification exceeded the maximum size and provides the S3 location to view it.

        Args:
            s3_location (str): The S3 location where the full job notification can be viewed.

        Returns:
            str: The message with the S3 location.
        """
        return f"Your job notification exceeded the maximum message size. You may see it at: {s3_location}"

    def publish_to_s3(self) -> str:
        """
        Publishes the message to an S3 bucket.

        Returns:
            str: The URL of the S3 object containing the message.
        """
        try:
            logger.debug("Attempting to publish message to S3.")
            s3_object, tag_key = self.get_s3_params()
            location = self.save_to_s3(s3_object, tag_key)
            message = self.extended_message(location)
            return sns.publish(
                TopicArn=self.topic_arn,
                Message=message,
            )

        except ClientError as e:
            log_client_error(e, "publish_to_s3")
            raise e

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

    def build_email(self) -> str:
        """
        Builds an email message by assembling the email intro, each email entry for the jobs, and the email outro.

        Returns:
            str: The email message string.
        """
        return (
            self.assemble_message(self.params.get("email_intro", ""))
            + "".join(
                [
                    self.assemble_message(self.params.get("email_entry", ""))
                    for _ in self.jobs
                ]
            )
            + self.assemble_message(self.params.get("email_outro", ""))
        )

    def build_default(self) -> str:
        """
        Builds a default message by assembling the default intro with each default entry for the jobs.

        Returns:
            str: The default message string.
        """
        return self.assemble_message(self.params.get("default_intro", "")) + "".join(
            [
                self.assemble_message(self.params.get("default_entry", ""))
                for _ in self.jobs
            ]
        )


def retrieve_queue(params: dict[str, Any]) -> list[Any]:
    """
    Retrieve jobs from the Amazon.jobs queue.

    Args:
        params (dict): Dictionary containing the queue name and the number of jobs to retrieve.

    Returns:
        list: List of jobs retrieved from the queue.
    """
    queue_url = params["sqs_queue_url"]
    jobs = []
    logger.debug("Polling Amazon jobs SQS queue")
    while True:
        try:
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

                try:
                    sqs.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                    )
                    logger.debug(
                        f"Retrieved and deleted message {message['ReceiptHandle']}"
                    )
                except ClientError as e:
                    log_client_error(e, "retrieve_queue-delete_message")

        except ClientError as e:
            log_client_error(e, "retrieve_queue")
            raise e

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
    try:
        queue = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesDelayed",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )
        return any(queue["Attributes"].values())

    except ClientError as e:
        log_client_error(e, "messages_in_queue")


def purge_queue(queue_url: str) -> None:
    """
    Purges the specified queue.

    Args:
        queue_url: The URL of the queue to purge.
    """
    try:
        logger.debug("Purging Amazon jobs SQS queue")
        response = sqs.purge_queue(QueueUrl=queue_url)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            logger.error(f"Error purging Amazon jobs SQS queue: {response}")
        else:
            logger.debug("Amazon jobs SQS queue purged")

    except ClientError as e:
        log_client_error(e, "purge_queue")


def send_jobs(event: dict[str, Any]) -> dict[str, dict[int | str] | dict[str, None]]:
    """
    Sends jobs based on the provided event dictionary.

    Args:
        event: The event dictionary containing the necessary parameters.

    Returns:
        dict[str, int | None | str]: A dictionary with the status code indicating the result of the job sending.
    """
    params = get_params(event=event)

    jobs = retrieve_queue(params=params)
    if messages_in_queue(queue_url=params["sqs_queue_url"]):
        jobs.extend(retrieve_queue(params=params))
    purge_queue(queue_url=params["sqs_queue_url"])
    Message(jobs=jobs, params=params).publish()
    logger.info("Lambda Sender execution completed")
    status = {"statusCode": 200, "state": "InvokeJobSender"}
    return {"status": status, "data": None}


def job_sender_handler(
    event: dict[str, Any], context: dict[str, Any]
) -> dict[str, int | str, dict[str, str]]:
    """
    Lambda function handler that retrieves jobs from SQS and
    sends fetched jobs using SNS.

    Args:
        event: The event data passed to the Lambda function.
        context: The runtime information of the Lambda function.

    Returns:
        dict with status and/or errors
    """

    logger.debug(f"Lambda Sender execution started with event: \n {event}")
    try:
        return send_jobs(event=event)

    except Exception as e:
        logger.error(f"Error occurred in Lambda job_sender: {e}")
        return {
            "status": {
                "statusCode": 500,
                "state": "InvokeSender",
                "errorType": type(e).__name__,
                "errorFunc": context.function_name,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc(),
            },
            "data": get_params(event=event),
        }
