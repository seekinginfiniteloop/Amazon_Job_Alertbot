import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(name="alertbot")
logger.setLevel(level="DEBUG")


today = datetime.now(timezone.utc)


def send_email(
    jobs_data: list[dict[str, str | int]],
    sender_email: str,
    recipient_email: str,
    sender_arn: str,
    template_arn: str,
    **kwargs,
) -> None:
    """
    Send an email with the job data to the recipient email address.

    The function uses AWS Simple Email Service (SES) to send the email.
    It constructs the email subject, body text, and body HTML based on the provided job
    data. The email is sent to the recipient email address using the sender email
    address.

    Args:
        jobs_data: A list of dictionaries representing the job data.
        sender_email: The email address of the sender.
        recipient_email: The email address of the recipient.
        email_subject: The subject of the email.

    Returns:
        None

    """
    aws_region = os.environ["AWS_REGION"]

    ses_client = boto3.client("sesv2", region_name=aws_region)

    template_data = {
        "current_date": today.strftime(format="%d %B %y"),
        "jobs_count": len(jobs_data),
        "jobs_data": jobs_data,
    }
    try:
        response = ses_client.send_email(
            FromEmailAddress=sender_email,
            FromEmailAddressIdentityArn=sender_arn,
            Destination={
                "ToAddresses": recipient_email,
            },
            Content={
                "Template": {
                    "TemplateName": "AmznJobsNotice",
                    "TemplateArn": template_arn,
                    "TemplateData": json.dumps(template_data),
                }
            },
            ConfigurationSetName="amznjobsender",
        )
    except ClientError as e:
        logger.exception(f"email send failed: {e.response['Error']['Message']}")
    else:
        logger.info("Email sent! Message ID:", response["MessageId"])


def lambda_handler(event, context) -> None:
    """
    Lambda function handler that fetches job data from Amazon.jobs and sends an email.

    Args:
        event: The event data passed to the Lambda function.
        context: The runtime information of the Lambda function.

    Returns:
        None
    """

    logger.info("Lambda function execution started")

    send_email(jobs_data=data, **config.get("email", {}))
    logger.info("Lambda function execution completed")
