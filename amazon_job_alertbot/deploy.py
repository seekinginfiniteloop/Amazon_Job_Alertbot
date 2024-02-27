import glob
import hashlib
import os
import re
import secrets
import time
import zipfile
from typing import Any

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
cf = boto3.client("cloudformation")
sts = boto3.client("sts")


def get_templates(directory: str) -> tuple[str, str]:
    """
    Retrieves a list of YAML templates from the specified directory.

    Args:
        directory: The directory path.

    Returns:
        tuple with the child, root and statemachine template paths
    """
    yamls = glob.glob(pathname=os.path.join(directory, "*.yaml"))
    for y in yamls:
        if "child" in y:
            child: str = y
        if "root" in y:
            root: str = y
        if "state_machine" in y:
            statemachine: str = y
    return child, root, statemachine


def gen_hash_suffix() -> str:
    """
    Generates a random hash suffix of length 5.

    Returns:
        str: The generated hash suffix.

    Examples:
        gen_hash_suffix()
    """

    return secrets.token_hex(5)


def set_stack_name(stack_name: str, suffix: str) -> str:
    """
    Sets the stack name with a random hash suffix.

    Args:
        stack_name: The stack name.
        suffix: The random hash suffix.

    Returns:
        str: The stack name with the random hash suffix.
    """
    if stacks := cf.describe_stacks()["Stacks"]:
        return next(
            (
                stack["StackName"]
                for stack in stacks
                if stack["StackName"].startswith(stack_name)
                and not stack["StackStatus"].startswith("DELETE")
            ),
            f"{stack_name}-{suffix}",
        )
    return f"{stack_name}-{suffix}"


def get_perm_user_arn(arn) -> str:
    """
    Retrieves the Amazon Resource Name (ARN) of the permission role associated with the provided assumed-role arn.

    Args:
        arn: The assumed-role arn extracted from GetCallerIdentity

    Returns:
        str: The ARN of the permission role.
    """
    role = arn.split("/")[1]
    iam_client = boto3.client("iam")
    return iam_client.get_role(RoleName=role)["Role"]["Arn"]


def get_user_arns(sts) -> str:
    """
    Retrieves the Amazon Resource Names (ARNs) of the user associated with the provided STS client.

    Args:
        sts: The STS client used to retrieve the caller identity.

    Returns:
        str: The ARNs of the user.
    """

    identity = sts.get_caller_identity()
    user_arn = identity["Arn"]
    root_arn = identity["Account"]
    if re.match(pattern=r"^arn:.*:.*::\d+:(assumed-role)/*.", string=user_arn):
        perm_arn = get_perm_user_arn(arn=user_arn)
        return f"{root_arn},{perm_arn}"
    return f"{root_arn},{user_arn}"


def read_template(template_path: str) -> str:
    """
    Reads the contents of a template file.

    Args:
        template_path (str): The path to the template file.

    Returns:
        str: The contents of the template file.
    """

    with open(file=template_path, mode="r") as file:
        return file.read()


def await_response(stack_name: str) -> None | Any:
    """
    Waits for the stack creation to complete and handles the response.

    Args:
        stack_name (str): The name of the stack.

    Returns:
        None

    Raises:
        ValueError: If the stack creation fails.
    """
    time.sleep(10)
    seen_events = []
    status = ""
    while True:
        stack_details = cf.describe_stacks(StackName=stack_name)["Stacks"][0]
        response = stack_details.get("StackStatus")
        events = list(
            reversed(cf.describe_stack_events(StackName=stack_name)["StackEvents"])
        )
        for event in events:
            event_id = event.get("EventId")
            if (
                event_id not in seen_events
                and event.get("ResourceStatusReason")
                and event.get("ResourceStatus")
                in ["CREATE_COMPLETE", "UPDATE_COMPLETE"]
            ):
                seen_events.append(event_id)
                print(
                    f"""Update:
                    \tStack: {event.get('StackName')}  |  Time: {event.get('Timestamp').strftime('%d-%m-%y %H:%M:%S')}
                    \tLogicalId: {event.get('LogicalResourceId')}  |  Resource Type: {event.get('ResourceType').split('::')[2]}
                    \tResourceStatusReason: {event.get('ResourceStatusReason')}\n
                    \t STATUS: {event.get("ResourceStatus")}"""
                )
        if "PROGRESS" in response and "ROLLBACK" not in response:
            if response != status:
                status = response
                print(f"Stack status: {status}")
            time.sleep(5)
        elif "ROLLBACK" in response or "FAILED" in response:
            raise ValueError(
                f"Stack creation failed: {stack_details.get('StackStatusReason')} for {stack_details.get('LogicalResourceId')}"
            )
        elif response.endswith("COMPLETE"):
            print("Stack creation complete.")
            break
        else:
            break
    return stack_details.get("Outputs")


def deploy_cloud(cf_params) -> None | list[dict[str, str]]:
    """
    Deploys a CloudFormation stack using the specified parameters. If it already exists, updates the stack.

    Args:
        cf_params: The parameters for creating the stack.

    Returns:
        returns stack outputs if any

    """

    try:
        cf.create_stack(**cf_params)
        print("Waiting for stack to be created...")

        return await_response(stack_name=cf_params["StackName"])

    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise e
        print("Stack already exists...updating stack...")
        return update_stacks(cf_params=cf_params)


def update_stacks(cf_params) -> None | list[dict[str, str]]:
    """
    Updates the CloudFormation stack with the given parameters.

    Args:
        cf_params: The parameters for updating the stack.

    Returns:
        None | list[dict[str, str]]: If the stack is a child stack, returns None. Otherwise, returns a list of outputs from the stack update.

    Raises:
        None
    """
    for param in cf_params["Parameters"]:
        if (
            param["ParameterKey"]
            in ["BaseName", "BaseNameTitleCase", "TagKey", "UserArns"]
            and param["ParameterValue"]
        ):
            param.pop("ParameterValue")
            param["UsePreviousValue"] = True

    cf_params.pop("EnableTerminationProtection")
    cf_params["DisableRollback"] = False
    new_key = {"ParameterKey": "ChildEnabled", "ParameterValue": "true"}
    cf_params["Parameters"].append(new_key)
    deployment_arn = get_output_value(
        output=cf.describe_stacks(StackName=cf_params["StackName"])["Stacks"][0][
            "Outputs"
        ],
        key_value="DeploymentRoleArn",
    )
    cf_params["RoleARN"] = deployment_arn
    cf.update_stack(**cf_params)
    return await_response(stack_name=cf_params["StackName"])


def zip_directory(folder_path, output_filename) -> None:
    """
    Zips the contents of a directory into a zip file.

    Args:
        folder_path: The path to the directory to be zipped.
        output_filename: The name of the output zip file.

    Returns:
        None
    """
    with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                zipf.write(
                    filename=os.path.join(root, file),
                    arcname=os.path.relpath(
                        path=os.path.join(root, file),
                        start=os.path.join(folder_path, ".."),
                    ),
                )


def check_and_upload_to_s3(
    bucket: str, file_path: str | list[str], s3_path: str | list[str]
) -> None:
    """
    Checks if a file exists in an S3 bucket and uploads it if it doesn't.

    Args:
        bucket: The name of the S3 bucket.
        file_path: The path to the file to be uploaded (or list of paths)
        s3_path: The destination(s) path in the S3 bucket.

    Returns:
        None
    """
    if isinstance(file_path, str):
        file_path = [file_path]
    if isinstance(s3_path, str):
        s3_path = [s3_path]
    for file, s3 in zip(file_path, s3_path):
        s3object = s3.Object(bucket, s3)
        if not s3object.load() or check_diff(file_path=file, s3object=s3object):
            upload_to_s3(bucket=bucket, file_path=file, s3_path=s3)


def check_diff(file_path: str, s3object: str) -> bool:
    """
    Compares the hash of the local file with the hash of the S3 object and returns True if they are different.

    Args:
        file_path (str): The path to the local file.
        s3object: The S3 object.

    Returns:
        bool: True if the hash of the local file is different from the hash of the S3 object, False otherwise.
    """
    s3object_hash = s3object.e_tag.strip('"')
    with open(file=file_path, mode="rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            file_hash = hashlib.md5(string=chunk).hexdigest()
            if file_hash != s3object_hash:
                return True


def upload_to_s3(bucket, file_path, s3_path) -> None:
    """
    Uploads a file to an S3 bucket.

    Args:
        bucket: The name of the S3 bucket.
        file_path: The path to the file to be uploaded.
        s3_path: The destination path in the S3 bucket.

    Returns:
        None
    """
    s3.put_object(Body=file_path, Bucket=bucket, Key=s3_path)


def get_output_value(output: list[dict[str, str]], key_value: str) -> str | None:
    """
    Returns the value associated with the given key in the output list of dictionaries.

    Args:
        output (list[dict[str, str]]): The list of dictionaries containing output information. (The result of describe_stacks(StackName='YourStack')["Stacks"][0]["Outputs"])
        key_value (str): The key to search for in the dictionaries.

    Returns:
        str | None: The value associated with the given key, or None if the key is not found.
    """
    for result in output:
        if result["OutputKey"] == key_value:
            return result["OutputValue"]


def cleanup(folder_path) -> None:
    """
    Deletes all zip files in the specified folder and its subfolders.

    Args:
        folder_path (str): The path to the folder to be cleaned up.

    Returns:
        None

    Examples:
        cleanup("/path/to/folder")
    """
    for file_path in glob.glob(pathname=f"{folder_path}/**/*.zip", recursive=True):
        os.remove(file_path)


def main() -> None:
    """
    Executes the main deployment process.

    Returns:
        None

    Examples:
        main()
    """

    child_template, root_template, states_template = get_templates(
        directory="./templates"
    )

    if not child_template or not states_template:
        raise ValueError("Couldn't locate child and statemachine templates.")

    child_filename, root_filename, states_filename = (
        os.path.basename(child_template),
        os.path.basename(root_template),
        os.path.basename(states_template),
    )
    # TODO: add argparse for variables
    base_name = "job-agent"
    base_name_title = "JobAgent"
    suffix = gen_hash_suffix()
    root_stack_name = set_stack_name(stack_name="job-agent-root-stack", suffix=suffix)
    user_arns = get_user_arns(sts=sts)

    root_params = {
        "StackName": root_stack_name,
        "TemplateBody": read_template(template_path=root_template),
        "Capabilities": ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
        "RetainExceptOnCreate": True,
        "StackPolicyBody": """
        {
        "Statement" : [
            {
            "Effect" : "Allow",
            "Action" : "Update:*",
            "Principal": "*",
            "Resource" : "*"
            }
        ]
        }
        """,
        "Parameters": [
            {
                "ParameterKey": "HashSuffix",
                "ParameterValue": suffix,
            },
            {
                "ParameterKey": "UserArns",
                "ParameterValue": user_arns,
            },
            {
                "ParameterKey": "YourEmail",
                "ParameterValue": "youremail@example.com",
            },
            {
                "ParameterKey": "TemplateFileName",
                "ParameterValue": child_filename,
            },
            {
                "ParameterKey": "BaseName",
                "ParameterValue": base_name,
            },
            {
                "ParameterKey": "BaseNameTitleCase",
                "ParameterValue": base_name_title,
            },
        ],
        "Tags": [{"Key": "JobAlertsAgent", "Value": f"{root_stack_name}-root"}],
        "DisableRollback": True,
        "EnableTerminationProtection": True,
    }
    output = deploy_cloud(cf_params=root_params)
    bucket = get_output_value(output=output, key_value="JobAgentS3Name")

    for lambda_folder in os.listdir(path="lambdas"):
        lambda_path = os.path.join("lambdas", lambda_folder)
        zip_file: str = f"{lambda_folder}.zip"
        zip_directory(folder_path=lambda_path, output_filename=zip_file)
        check_and_upload_to_s3(
            bucket=bucket, file_path=zip_file, s3_path=f"lambdas/{zip_file}"
        )

    files = [child_template, states_template, root_template]
    filenames = [child_filename, states_filename, root_filename]
    destinations = [f"templates/{filename}" for filename in filenames]
    check_and_upload_to_s3(bucket=bucket, file_path=files, s3_path=destinations)
    time.sleep(10)
    if get_output_value(output=output, key_value="ChildEnabled") == "false":
        update_stacks(cf_params=root_params)

    cleanup(folder_path=os.listdir(path="."))


if __name__ == "__main__":
    main()
