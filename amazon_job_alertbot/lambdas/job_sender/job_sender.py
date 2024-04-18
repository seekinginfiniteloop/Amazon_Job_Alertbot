import json
import logging
import re
import sys
import time
import traceback

from datetime import datetime, timezone
from logging import Logger
from re import Pattern
from typing import Any, Generator, Optional

import boto3

from botocore.config import Config
from botocore.exceptions import ClientError

logger: Logger = logging.getLogger()

today = datetime.now(timezone.utc).date()

config = Config(retries={"max_attempts": 10, "mode": "adaptive"})

sqs: Any = boto3.client("sqs", config=config)
sns: Any = boto3.client("sns", config=config)

# Helper functions:


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


def fill_placeholders(string: str, kvs: dict[str, str]):
    """
    Fills placeholders in a string with values from a dictionary.

    Args:
        string: The string to fill placeholders in.
        kvs: A dictionary containing placeholder keys and values.

    Returns:
        str: The string with filled placeholders.
    """
    placeholder_pattern: Pattern[str] = re.compile(pattern=r"\{\{(\w+)\}\}")
    return placeholder_pattern.sub(
        repl=lambda match: kvs.get(match.group(1), ""), string=string
    )


def should_retry(error: ClientError) -> bool:
    """
    Check if the error indicates a need to retry the operation.

    Args:
        error: The error object containing response details.

    Returns:
        bool: True if the error code suggests a retry is needed, False otherwise.
    """
    error_code = error.response.get("Error", {}).get("Code")
    return error_code in ["RequestLimitExceeded", "Throttling"] or error.response.get(
        "ResponseMetadata", {}
    ).get("HTTPStatusCode") in [429, 503]


# Objects for generating SNS message: Template, Message
class Template:
    """
    Represents a template for a message.

    Attributes:
        intro: The introductory part of the template.
        body: The main body content of the template.
        outro: Optional closing part of the template.

    Methods:
        assemble: Assembles the template with provided replacements and values.
    """

    def __init__(
        self, intro: str, body: str, outro: str | None = None, **kwargs
    ) -> None:
        """
        Initialize a Template object with intro, body, and optional outro.

        Args:
            intro: The introductory part of the template.
            body: The main body content of the template.
            outro: Optional closing part of the template.

        Returns:
            None
        """
        self.intro: str = intro
        self.body: str = body
        self.outro: str | None = outro

        if "other_vals" in kwargs:
            self.template: str = self.assemble(
                body_replacements=kwargs["body_replacements"],
                other_vals=kwargs["other_vals"],
            )

    def assemble(
        self,
        body_replacements: dict[str, Any],
        other_vals: dict[str, Any],
        separator: str = "\n\n",
    ) -> str:
        """
        Assemble the template with provided replacements and values.

        Args:
            body_replacements: Dictionary of replacements for the body content.
            other_vals: Additional values for placeholders in the intro and outro.
            separator: Separator to use between sections of the assembled template.

        Returns:
            str: The assembled template with replacements and values.
        """
        intro: str = fill_placeholders(string=self.intro, kvs=other_vals)
        body_list = [
            fill_placeholders(string=self.body, kvs=body_replacement)
            for body_replacement in body_replacements
        ]
        body: str = separator.join(body_list)
        outro = fill_placeholders(string=self.outro, kvs=other_vals)
        return (
            f"{intro}{separator}{body}{separator}{outro}"
            if outro
            else f"{intro}{separator}{body}"
        )

class Email:
    def __init__(self, jobs: list[dict[str, Any]], identity: str, template: str, subject: str, configuration_set: str, tagkey: str) -> None:
        self.client: Any = boto3.client("sesv2")
        self.identity: str = identity
        self.replacements: dict[str, str] = self._get_replacements(jobs)
        self.template: dict[str, str] = self.client.get_email_template(TemplateName=template)
        self.rendered_template: dict[str, str] = self._render_template()
        self.subject: str = subject
        self.plain_text: str = self._get_plain_text()
        self.html: str = self._get_html()
        self.sender: str = self.identity
        self.recipient: str = self.identity
        self.configuration_set: str = configuration_set
        self.tagkey: str = tagkey

    def send(self) -> None:
        self.client.send_email(
            FromEmailAddress=self.sender,
            Destination={"ToAddresses": [self.recipient]},
            Content={
                "Template": {
                    "TemplateName": self.template["TemplateName"],
                    "TemplateArn": self.template["TemplateArn"],
                    "TemplateData": self.replacements,
                    },
                },
            EmailTags=[{'Name': self.tagkey, 'Value': self.tagkey}],
            ConfigurationSetName=self.configuration_set,
        )

    def _get_replacements(jobs: list[dict, str])

class Message:
    def __init__(self, jobs: list[dict[str, Any]], params: dict[str, Any]) -> None:
        self.jobs: list[dict[str, Any]] = jobs
        self.params: dict[str, Any] = params
        self.other_vals: dict[str, str] = {
            "today": today.strftime(format="%d %B %y"),
            "job_count": str(len(self.jobs)),
        }
        self.topic_arn: str = params["topic_arn"]

        template_params = {
            "body_replacements": self.jobs,
            "other_vals": self.other_vals,
        }

        self.default: str = Template(
            intro=params["default_intro"],
            body=params["default_entry"],
            outro="",
            **template_params,
        ).template

        self.sms: str = Template(
            intro="", body=params["sms"], outro="", **template_params
        ).template

        self.sqs = self.jobs

        self.subject: str = fill_placeholders(
            params.get("subject", ""), self.other_vals
        )

        self.publisher = Publisher(self.topic_arn, self.params)

    @property
    def message(self) -> dict[str, Any]:
        return json.dumps(
            {
                "default": self.default,
                "sms": self.sms,
                "sqs": self.sqs,
            }
        )

    def publish(self) -> Any:
        return self.publisher.publish(self.message)


class Publisher:
    """
    A class responsible for publishing messages to an SNS topic or an S3 bucket and sending notifications.

    Attributes:
        topic_arn (str): The ARN of the SNS topic.
        params (dict[str, Any]): Dictionary of parameters for publishing messages.

    Methods:
        __init__(self, topic_arn: str, params: dict[str, Any]) -> None:
            Initialize a Publisher object with the specified SNS topic ARN and parameters.

        publish(self, message: str) -> Any:
            Publish a message to either an SNS topic or an S3 bucket based on the message size.

        publish_to_sns(self, message: str) -> Any:
            Publish a message to an SNS topic.

        publish_to_s3(self, message: str) -> Any:
            Publish a message to an S3 bucket and send a notification via SNS.
    """

    def __init__(self, topic_arn: str, params: dict[str, Any]) -> None:
        """
        Initialize a Publisher object with the specified SNS topic ARN and parameters.

        Args:
            topic_arn (str): The ARN of the SNS topic.
            params (dict[str, Any]): Dictionary of parameters for publishing messages.

        Returns:
            None
        """
        self.topic_arn = topic_arn
        self.params = params

    def publish(self, message: str) -> Any:
        """
        Publish a message to either an SNS topic or an S3 bucket based on the message size.

        If the message size exceeds 256,000 bytes, it will be published to an S3 bucket and a notification will be sent via SNS.
        Otherwise, it will be directly published to the SNS topic.

        Args:
            message (str): The message content to be published.

        Returns:
            Any: Response from publishing the message to the SNS topic or the S3 bucket.
        """
        return (
            self.publish_to_s3(message)
            if sys.getsizeof(message) > 256_000
            else self.publish_to_sns(message)
        )

    def publish_to_sns(self, message: str) -> Any:
        """
        Publish a message to an SNS topic.

        Args:
            message (str): The message content to be published.

        Returns:
            Any: Response from publishing the message to the SNS topic.
        """
        try:
            return sns.publish(
                TopicArn=self.topic_arn,
                Message=message,
                MessageStructure="json",
            )
        except ClientError as e:
            log_client_error(e, "publish")
            raise e

    def publish_to_s3(self, message: str) -> Any:
        """
        Publish a message to an S3 bucket and send a notification via SNS.

        Args:
            message (str): The message content to be published.

        Returns:
            Any: Response from publishing the message to S3 and sending the SNS notification.
        """
        try:
            s3 = boto3.resource("s3")
            bucket_name = self.params["s3_bucket"]
            bucket_key_prefix = self.params["s3_key_prefix"]
            key = f"{bucket_key_prefix}message_{today.strftime(format='%Y%m%d')}.html"
            s3_object = s3.Object(bucket_name, key)
            tag_key = self.params["tag_key"]

            s3_object.put(
                Body=message,
                ContentType="application/json",
                BucketKeyEnabled=True,
                Tagging=f"{tag_key}={tag_key}",
            )

            s3 = boto3.client("s3")
            url = s3.generate_presigned_url(
                ClientMethod="get_object",
                Params={
                    "Bucket": s3_object.bucket_name,
                    "Key": s3_object.key,
                },
                ExpiresIn=432_000,  # Link expires in 5 days
            )

            message = f"Your job notification exceeded the maximum message size. You can get it at: {url}"
            return sns.publish(
                TopicArn=self.topic_arn,
                Message=message,
            )

        except ClientError as e:
            log_client_error(e, "publish_to_s3")
            raise e


def update_job_status(jobs, message_id: str) -> dict[str, Any]:
    """
    Updates the status of a job in the jobs list.

    Args:
        job: The job to update.
    """
    for j in jobs:
        j["sent"] = True
        j["sent_on"] = datetime.now(timezone.utc).isoformat()
        j["message_id"] = message_id
    return jobs


def get_sent_jobs(jobs) -> Generator[dict[str, Optional[Any]], None, None]:
    """
    Iterates over the jobs and yields a dictionary for each job that has been marked as sent.

    Yields:
        A dictionary for each job marked sent that includes the job's ic_icims number, MessageId for sent message, and date sent.
    """
    for job in jobs:
        if job.get("sent"):
            yield {
                k: v
                for k, v in job.items()
                if k in ["id_icims", "sent", "sent_on", "message_id"]
            }


class Queue:
    """
    A class that serves as a proxy for a sqs.Queue object by possessing a Queue object
    (Queue.queue). Boto3 SQS Queue object attributes are accessable through Queue.attributes. Our Queue, however, auto-initializes by retrieving all available messages and processing them into the Queue.retrieved_messages and Queue.processed_items attributes.

    Attributes:
        queue (sqs.Queue): The sqs.Queue object.
        attributes (dict): The attributes of the sqs.Queue object.
        arn (str): The ARN of the sqs.Queue object.
        url (str): The URL of the sqs.Queue object.
        name (str): The name of the sqs.Queue object.
        max_size (str): The maximum size of the sqs.Queue object.
        available_messages (int): The number of available messages in the sqs.Queue
            object.
        retrieved_messages (list[Any]): The list of retrieved sqs.Message objects from
            the sqs.Queue object.
        message_count (int): The number of messages in the retrieved_messages list.
        processed_items (list[Any]): The list of processed items from the
            retrieved_messages list. This is a flattened list of all top-level objects from the retrieved messages' body attributes.
        item_count (int): The number of items in the processed_items list.

    """

    def __init__(self, queue_obj) -> None:
        """
        Initialize a Queue object with attributes from the provided boto3 queue object.

        Args:
            queue_obj: The queue object to load attributes from.

        Returns:
            None
        """
        queue_obj.load()
        self.queue = queue_obj
        self.attributes = self.queue.attributes
        self.arn = self.attributes["QueueArn"]
        self.url = self.queue.url
        self.name = self.arn.split(":")[-1]
        self.max_size = self.attributes["MaximumMessageSize"]
        self.available_messages = int(self.attributes["ApproximateNumberOfMessages"])
        self.retrieved_messages: list[Any] = self._get_messages() or []
        self.message_count: int = len(self.retrieved_messages)
        self.processed_items: list[Any] = self._process_messages() or []
        self.item_count: int = len(self.processed_items)
        logger.debug(f"Queue {self.name} initialized")
        logger.debug(f"Queue {self.name} has {self.available_messages} messages")
        logger.debug(f"Queue {self.name} has {self.message_count} retrieved messages")
        logger.debug(f"Queue {self.name} has {self.item_count} processed items")

        if not self.retrieved_messages:
            logger.warning(f"No messages found in queue {self.name}; Queue empty")

    def _get_messages(self) -> list[Any]:
        """
        Retrieve and process messages from the queue with retries.

        Returns:
            list[Any]: List of processed messages fetched from the queue.
        """
        tries: int = 0
        poll_time = 10
        messages = []
        available = self.available_messages
        if not available:
            return []
        logger.debug(f"Fetching messages from queue {self.name}")
        while tries < (8 + (available // 10)) and not (
            messages and len(messages) == available
        ):
            tries += 1
            poll_time += 5
            time.sleep(3)
            params = {
                "MaxNumberOfMessages": 10,
                "VisibilityTimeout": 480,
                "WaitTimeSeconds": poll_time,
                "MessageAttributeNames": ["All"],
            }
            logger.debug(f"Starting attempt {tries} to retrieve messages")
            try:
                if msg := self._retrieve_message(**params):
                    messages.extend(msg)
                else:
                    logger.debug(f"No messages found on attempt {tries}")
                    continue
            except ClientError as e:
                log_client_error(e, "Queue.messages")
                if not should_retry(e):
                    raise e
                if not (id := e.response.get("ResponseMetadata", {}).get("RequestId")):
                    continue
                logger.warning(f"Retrying request {id}")
                time.sleep(10)
                if msg := self._retrieve_message(ReceiveRequestAttemptId=id, **params):
                    messages.extend(msg)
                else:
                    logger.debug(f"No messages found on attempt {tries}")
                    continue
            logger.debug(
                f"Retrieved {len(msg)} messages on attempt {tries}. Total: {len(messages)}"
            )
        return messages

    def _retrieve_message(self, **kwargs) -> Any | None:
        """
        Retrieve a message from the queue based on provided arguments.

        Args:
            **kwargs: Additional keyword arguments for message retrieval.

        Returns:
            Any | None: The retrieved message if found, None otherwise.
        """
        msg = self.queue.receive_messages(**kwargs)
        logger.debug(f" Characteristics and type of message: {type(msg)}")
        if not msg:
            return None
        if isinstance(msg, list) and all(hasattr(item, "body") for item in msg):
            logger.debug(
                f"Characteristics and type of message list: {[type(m) for m in msg]}. Dict: {msg[0].__dict__}"
            )
            return msg
        if hasattr(msg, "body"):
            return [msg]
        logger.debug(
            f"Message structure doesn't seem to be what we expected. Message type {type(msg)}; item types: {[type(item) for item in msg]}, message dict: {msg.__dict__}\n item[0] dict: {msg[0].__dict__}"
        )

    def _process_messages(self) -> list[Any]:
        """
        Process retrieved messages by extracting, parsing, and concatenating their content.

        Returns:
            list[Any]: a concatenated list of the objects within the messages
        """
        bodies = [msg.body for msg in self.retrieved_messages]
        decoded = [json.loads(body) for body in bodies]
        if any(isinstance(item, list) for item in decoded):
            return [item for sublist in decoded for item in sublist]
        return decoded


def create_response(sent_jobs):
    """
    Creates a response object with status and data based on the processed jobs sent.

    Args:
        sent_jobs: A list of jobs to be processed for creating updates.

    Returns:
        dict: A response dictionary containing status information and updates data.
    """
    return [
        {
            "Item": {
                "id_icims": job["id_icims"],
                "sent": job["sent"],
                "sent_on": job["sent_on"],
                "message_id": job["message_id"],
            }
        }
        for job in sent_jobs
    ]


def send_jobs(event: dict[str, Any]) -> dict[str, dict[int | str] | dict[str, None]]:
    """
    Sends jobs based on the provided event dictionary.

    Args:
        event: The event dictionary containing the necessary parameters.

    Returns:
        dict[str, int | None | str]: A dictionary with information on the job's publication to SNS -- to add to the database.
    """
    params = get_params(event=event)
    sqs = boto3.resource("sqs")
    queue = sqs.Queue(params["sqs_queue_url"])
    q = Queue(queue)

    jobs: list[Any] = q.processed_items
    logger.debug(
        f"Received messages: {jobs}; \n\ntype: {type(jobs)}, content type: {type(jobs[0])}"
    )
    if not jobs:
        logger.info("No jobs to send")
        return {"status": {"statusCode": 200, "state": "InvokeJobSender"}, "data": None}
    logger.info(f"Sending {len(jobs)} jobs")
    message = Message(jobs=jobs, params=params)
    response = message.publish()
    logger.debug(f"Response from SNS: {response}")
    jobs = update_job_status(jobs=jobs, message_id=response.get("MessageId", ""))
    filtered_jobs = list(get_sent_jobs(jobs))
    q.queue.purge()

    logger.info("Lambda Sender execution completed")
    return (
        {
            "status": {"statusCode": 200, "state": "InvokeJobSender"},
            "data": {"UpdateRequests": create_response(filtered_jobs)},
        }
        if filtered_jobs
        else {"status": {"statusCode": 200, "state": "InvokeJobSender"}, "data": None}
    )


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
        logger.error(f"Error in {context.function_name}: {e}")
        logger.error("State: InvokeJobSender")
        logger.error(traceback.format_exc())
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Entry event for error: {event}")
        raise e
