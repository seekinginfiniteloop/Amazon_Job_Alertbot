import argparse
import contextlib
import glob
import hashlib
import json
import os
import re
import secrets
import time
import zipfile
from datetime import datetime
from typing import Any
from urllib.parse import quote

import boto3
from botocore.exceptions import ClientError, WaiterError

s3 = boto3.client("s3")
s3r = boto3.resource("s3")
cf = boto3.client("cloudformation")
sts = boto3.client("sts")
ec2 = boto3.client("ec2")
session = boto3.session.Session()

ParamDictType = dict[str, str | list[str | dict[str, str | None]] | bool | None]


def parse_cli() -> argparse.Namespace | None:
    """
    Parses command line arguments.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Deploy a child stack to the root stack."
    )
    parser.add_argument(
        "-e",
        "--email",
        type=str,
        help="Email address to send notifications to - REQUIRED.",
        required=True,
    )
    parser.add_argument(
        "-b",
        "--basename",
        type=str,
        help="Base name of the stacks, defaults to: job-agent",
        default="job-agent",
    )
    return parser.parse_args()


def find_lost_dir(directory: str) -> str | None:
    """
    Finds the specified directory in the directory tree.

    Args:
        directory: The directory to search for.

    Returns:
        str: The lost directory's path.
    """
    return next(
        os.path.join(root, directory)
        for root, dirs, _ in os.walk(".")
        if directory in dirs
    )


def get_templates(directory: str):
    """
    Retrieves the paths of YAML templates from the specified directory.

    Args:
        directory: The directory name to search for.

    Returns:
        A tuple with the paths of child, root, and statemachine templates, if found.
    """
    yamls: list[str] = glob.glob(os.path.join(directory, "*.yaml"))
    if not yamls:
        full_path = find_lost_dir(directory=directory)
        yamls = glob.glob(pathname=os.path.join(full_path, "*.yaml"))
    for y in yamls:
        if "child" in y:
            child: str = y
        if "root" in y:
            root: str = y
        if "state_machine" in y:
            statemachine: str = y
    if not (child or root or statemachine):
        print(
            "Did not locate at least one template locally; if the stacks already exist,"
            "this won't be a problem. If they don't... game over man."
        )
    return child, root, statemachine


def gen_hash_suffix(base_name: str) -> str:
    """
    Generates a random hash suffix of length 5.
    If one has not been generated. Otherwise, returns it.

    Returns:
        str: The generated hash suffix.

    """
    for stack in cf.describe_stacks()["Stacks"]:
        stackname = stack["StackName"]
        if stackname.startswith(base_name) and (len(stackname.split("-")[-1]) == 12):
            return (
                get_output_value(
                    output=cf.describe_stacks(StackName=stackname)["Stacks"][0].get(
                        "Outputs"
                    ),
                    key_value="Suffix",
                )
                or stackname.split("-")[-1]
            )
    return secrets.token_hex(6)


def set_stack_name(stack_name: str, suffix: str) -> str:
    """
    Sets the stack name with a random hash suffix.

    Args:
        stack_name: The stack name.
        suffix: The random hash suffix.

    Returns:
        str: The stack name with the random hash suffix.
    """
    return next(
        (
            stack["StackName"]
            for stack in cf.describe_stacks()["Stacks"]
            if stack_name in stack["StackName"]
            and not stack.get("StackStatus").startswith("DELETE")
        ),
        f"{stack_name}-{suffix}",
    )


def get_url_suffix() -> str:
    """
    Returns the URL suffix for the current region.

    Returns:
        str: The URL suffix.
    """
    region: str = session.region_name
    endpt: str = ec2.describe_regions(
        Filters=[{"Name": "region-name", "Values": [region]}]
    )["Regions"][0].get("Endpoint")
    return endpt.replace(f"ec2.{region}.", "") or "amazonaws.com"


def get_template_url(base_name: str, hash_suffix: str, bucket: str) -> str:
    """
    Returns the URL of the template file for CloudFormation stack creation.

    Args:
        base_name (str): base name for the file
        hash_suffix (str): The suffix to be appended to the template file name.
        bucket (str): The name of the S3 bucket where the template file is stored.

    Returns:
        str: The URL of the template file.
    """
    url: str = quote(
        string=f"https://{bucket}.s3.{get_url_suffix()}/templates/{base_name}-{hash_suffix}.yaml",
        safe=":/",
    )
    """
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": f"templates/{base_name}-{hash_suffix}.yaml",
            },
            ExpiresIn=3600,
        )
    except ClientError as e:
        print(e.response["Error"]["Message"])
    """
    return url


def get_perm_user_arn(arn: str) -> str:
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


def get_user_arns() -> str:
    """
    Retrieves the Amazon Resource Names (ARNs) of the user associated with the provided STS client.

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


def stack_exists(stack_name: str) -> bool:
    """
    Checks if a parent stack with the specified name exists.

    Args:
        stack_name: The name of the parent stack.

    Returns:
        bool: True if the parent stack exists, False otherwise.
    """
    if stacks := cf.describe_stacks().get("Stacks"):
        return any(stack_name in stack.get("StackName") for stack in stacks)
    return False


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


def await_response(stack_name: str, update=False) -> None | Any:
    """
    Waits for the stack creation to complete and handles the response.

    Args:
        stack_name (str): The name of the stack.

    Returns:
    None | Any (stack output)

    Raises:
        ValueError: If the stack creation fails.
    """
    time.sleep(10)
    waiter_kwargs = {
        "StackName": stack_name,
        "WaiterConfig": {"Delay": 5, "MaxAttempts": 180},
    }
    waiter = "stack_update_complete" if update else "stack_create_complete"
    try:
        cf.get_waiter(waiter).wait(**waiter_kwargs)
        return cf.describe_stacks(StackName=stack_name)["Stacks"][0].get("Outputs")
    except WaiterError as e:
        val = "update" if update else "creation"
        raise ValueError(f"Stack {val} failed: {e}") from e


def deploy_cloud(cf_params: ParamDictType) -> None | list[dict[str, str]]:
    """
    Deploys a CloudFormation stack using the specified parameters. If it already
    exists, updates the stack.

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


def set_update_params(params: ParamDictType, stack: dict[str, Any]) -> None:
    """
    Set update parameters for the stack.

    Args:
        params: A dictionary containing the parameters for the stack update.
        stack: A dictionary representing the stack.

    Returns:
        None
    """
    child_exists = get_output_value(output=stack["Outputs"], key_value="ChildStackId")
    bucket_name: str | None = get_output_value(
        output=stack["Outputs"], key_value="JobAgentS3Name"
    )
    for param in params["Parameters"]:
        if param["ParameterKey"] == "HashSuffix":
            suffix = param["ParameterValue"]
        if param["ParameterKey"] not in [
            "EventSchedule",
            "LangCode",
            "LangCountryCode",
            "YourEmail",
            "ChildStackBody",
            "TemplateUrl",
        ] and param.get("ParameterValue"):
            param.pop("ParameterValue")
            param["UsePreviousValue"] = True

        if param["ParameterKey"] == "TemplateUrl" and not child_exists:
            param["ParameterValue"] = get_template_url(
                base_name="cf-child", hash_suffix=suffix, bucket=bucket_name
            )
            print(param["ParameterValue"])

    pop_if_present(params=params, key="EnableTerminationProtection")

    params["DisableRollback"] = False
    params["Parameters"].append(
        {"ParameterKey": "ChildEnabled", "ParameterValue": "true"}
    )

    if not child_exists:
        params["Parameters"].append(
            {"ParameterKey": "SelfArn", "ParameterValue": stack["StackId"]}
        )

    if child_exists:
        pop_if_present(params=params, key="TemplateBody")
        params["TemplateURL"] = get_template_url(
            base_name="cf-root", hash_suffix=suffix, bucket=bucket_name
        )
        params["RoleARN"] = get_output_value(
            output=stack["Outputs"],
            key_value="DeploymentRoleArn",
        )
    return params


def pop_if_present(params: ParamDictType, key: str) -> None:
    """
    Removes the specified key from the parameters if it exists.

    Args:
        params: The parameters dictionary.
        key: The key to remove.

    Returns:
        None
    """
    if key in params.keys():
        params.pop(key)
        return
    for param in params.get("Parameters"):
        if param.get("ParameterKey") == key:
            params["Parameters"].remove(param)
            break


def update_stacks(
    cf_params: ParamDictType,
) -> None | list[dict[str, str]]:
    """
    Updates the CloudFormation stack with the given parameters.

    Args:
        cf_params: The parameters for updating the stack.

    Returns:
        None | list[dict[str, str]]: If the stack is a child stack, returns None. Otherwise, returns a list of outputs from the stack update.

    Raises:
        None
    """
    stack = cf.describe_stacks(StackName=cf_params["StackName"])["Stacks"][0]
    set_update_params(params=cf_params, stack=stack)
    cf.update_stack(**cf_params)
    return await_response(stack_name=cf_params["StackName"], update=True)


def push_objects(bucket: str, files: list[str], destinations: list[str]) -> None:
    """
    Pushes the specified files to the specified S3 destinations.

    Args:
        bucket: The name of the S3 bucket.
        files: The list of files to be pushed.
        destinations: The list of S3 destinations.

    Returns:
        None
    """
    path = "lambdas"
    try:
        lambda_folders = os.listdir(path=path)
    except FileNotFoundError:
        path = find_lost_dir(directory=path)
        lambda_folders = os.listdir(path=path)
    for lambda_folder in lambda_folders:
        lambda_path = os.path.join(path, lambda_folder)
        zip_file: str = f"{lambda_folder}.zip"
        zip_directory(folder_path=lambda_path, output_filename=zip_file)
        check_and_upload_to_s3(
            bucket=bucket, file_path=zip_file, s3_path=f"{path}/{zip_file}"
        )
    check_and_upload_to_s3(bucket=bucket, file_path=files, s3_path=destinations)


def zip_directory(folder_path: str, output_filename: str) -> None:
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


def bucket_empty(bucket) -> bool:
    """
    Checks if the S3 bucket is empty.

    Args:
        bucket: The Bucket object.
    Returns:
        bool: True if the bucket is empty, False otherwise.
    """
    return not bucket.objects.all()


def object_exists(bucket, s3_path):
    """
    Checks if an object exists in the S3 bucket.

    Args:
        bucket: The Bucket object.
        s3_path: The path to the object.

    Returns:
        bool: True if the object exists, False otherwise.
    """

    try:
        return bool(bucket.Object(s3_path).load())
    except ClientError:
        return False


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
    s3bucket = s3r.Bucket(bucket)
    for file, s3_path in zip(file_path, s3_path):
        try:
            if bucket_empty(bucket=s3bucket):
                upload_to_s3(bucket=bucket, file_path=file, s3_path=s3_path)
                continue
            if object_exists(bucket=s3bucket, s3_path=s3_path):
                s3object = s3bucket.Object(s3_path)
                if check_diff(file_path=file, s3object=s3object):
                    upload_to_s3(bucket=bucket, file_path=file, s3_path=s3_path)
                    continue
                continue
            upload_to_s3(bucket=bucket, file_path=file, s3_path=s3_path)
            continue
        except Exception:
            try:
                upload_to_s3(bucket=bucket, file_path=file, s3_path=s3_path)
            except Exception as e:
                print(f"Error uploading {file} to S3: {e}")


def check_diff(file_path: str, s3object: str) -> bool:
    """
    Compares the hash of the local file with the hash of the S3 object and returns True if they are different.

    Args:
        file_path (str): The path to the local file.
        s3object: The S3 object.

    Returns:
        bool: True if the hash of the local file is different from the hash of the S3 object, False otherwise.
    """
    try:
        with contextlib.suppress(ClientError):
            s3object_hash = s3object.e_tag.strip('"')
            return get_hash(file_path=file_path) != s3object_hash
    except Exception:
        return False


def get_hash(file_path: str) -> str:
    """
    Calculates the MD5 hash of a file.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The MD5 hash of the file.

    """
    with open(file=file_path, mode="rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            return hashlib.md5(string=chunk).hexdigest()


def upload_to_s3(bucket: str, file_path: str, s3_path: str) -> None:
    """
    Uploads a file to an S3 bucket.

    Args:
        bucket: The name of the S3 bucket.
        file_path: The path to the file to be uploaded.
        s3_path: The destination path in the S3 bucket.

    Returns:
        None
    """
    waiter = s3.get_waiter("object_exists")
    now = datetime.now()
    s3.put_object(
        Body=file_path,
        Bucket=bucket,
        Key=s3_path,
        ContentType="application/zip"
        if file_path.endswith(".zip") or "lambdas" in file_path
        else "text/plain",
    )
    waiter.wait(
        Bucket=bucket, Key=s3_path, IfModifiedSince=now, WaiterConfig={"Delay": 2}
    )


def get_output_value(output: list[dict[str, str]], key_value: str) -> str | None:
    """
    Returns the value associated with the given key in the output list of dictionaries.

    Args:
        output (list[dict[str, str]]): The list of dictionaries containing output information. (The result of describe_stacks(StackName='YourStack')["Stacks"][0]["Outputs"])
        key_value (str): The key to search for in the dictionaries.

    Returns:
        str | None: The value associated with the given key, or None if the key is not found.
    """
    if output:
        for result in output:
            if result["OutputKey"] == key_value:
                return result["OutputValue"]


def update_deployment_permissions(
    deployment_arn: str, bucket_name: str, stack_arn: str
) -> None:
    """
    Updates the permissions for the specified deployment.

    Args:
        deployment_arn (str): The ARN of the deployment.
        bucket_name (str): The name of the bucket.
        stack_arn (str): The ARN of the stack.

    Returns:
        None
    """
    with contextlib.suppress(ClientError):
        iam = boto3.client("iam")
        role_base = deployment_arn.split("/")[-1].split("-")[-2]
        policy_base = re.match(
            pattern=r"^([a-zA-Z0-9]+)CFDeploymentRole", string=role_base
        )
        response = iam.get_role_policy(
            RoleName=deployment_arn.split("/")[-1],
            PolicyName=f"{policy_base}DeploymentPolicy",
        )
        policy_document = json.loads(response["Policy"]["PolicyDocument"])
        policy_document["Statement"][1]["Resource"].append(deployment_arn)
        iam.put_role_policy(
            RoleName=deployment_arn.split("/")[-1],
            PolicyName=f"{policy_base}DeploymentPolicy",
            PolicyDocument=json.dumps(policy_document),
        )

        response = s3.get_bucket_policy(Bucket=bucket_name)
        statements = json.loads(response)["Policy"]["Statement"]
        cf_aws_principal = statements[0]["Principal"]["AWS"]
        cf_aws_principal.append(stack_arn)
        cf_aws_principal.append(deployment_arn)
        print(f"Updated deployment permissions for {deployment_arn}, {stack_arn}")


def cleanup(folder_path: str) -> None:
    """
    Deletes all zip files in the specified folder and its subfolders.

    Args:
        folder_path (str): The path to the folder to be cleaned up.

    Returns:
        None

    """
    for file_path in glob.glob(pathname=f"{folder_path}/**/*.zip", recursive=True):
        os.remove(path=file_path)


def main() -> None:
    """
    Executes the main deployment process.

    Returns:
        None

    Examples:
        main()
    """
    args: dict[str, Any] = vars(parse_cli())
    child_template, root_template, states_template = get_templates(
        directory="templates"
    )
    if not child_template or not states_template:
        raise ValueError("Couldn't locate child and statemachine templates.")

    base_name: str = args.get("basename", "superdelicious")
    components: list[str] = base_name.split("-")
    titled = [item.title() for item in components]
    base_name_title: str = "".join(titled)
    suffix: str = gen_hash_suffix(base_name=base_name)
    root_stack_name: str = set_stack_name(
        stack_name=f"{base_name}-root-stack", suffix=suffix
    )
    user_arns: str = get_user_arns()

    files: list[str] = [child_template, states_template, root_template]
    prefix = "templates/"
    destinations = [
        f"{prefix}cf-child-{suffix}.yaml",
        f"{prefix}statemachine-{suffix}.yaml",
        f"{prefix}cf-root-{suffix}.yaml",
    ]

    root_params: ParamDictType = {
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
                "ParameterValue": args.get("email"),
            },
            {
                "ParameterKey": "TemplateFileName",
                "ParameterValue": destinations[0],
            },
            {
                "ParameterKey": "BaseName",
                "ParameterValue": base_name,
            },
            {
                "ParameterKey": "BaseNameTitleCase",
                "ParameterValue": base_name_title,
            },
            {
                "ParameterKey": "TemplateUrl",
                "ParameterValue": "none",
            },
        ],
        "Tags": [
            {"Key": "JobAlertsAgent", "Value": f"{root_stack_name}"},
            {"Key": "StackName", "Value": f"{base_name}-root-stack"},
        ],
        "DisableRollback": True,
        # "EnableTerminationProtection": True,
    }
    if stack_exists(stack_name=root_stack_name):
        bucket: str | None = get_output_value(
            output=cf.describe_stacks(StackName=root_stack_name)
            .get("Stacks")[0]
            .get("Outputs"),
            key_value="JobAgentS3Name",
        )
        push_objects(bucket=bucket, files=files, destinations=destinations)
        output = update_stacks(cf_params=root_params)
    else:
        output: list[dict[str, str]] | None = deploy_cloud(cf_params=root_params)
        if (
            cf.describe_stacks(StackName=root_stack_name)["Stacks"][0].get(
                "StackStatus"
            )
            != "CREATE_COMPLETE"
        ):
            raise ValueError("Stack creation failed.")
        bucket: str | None = get_output_value(
            output=cf.describe_stacks(StackName=root_stack_name)["Stacks"][0][
                "Outputs"
            ],
            key_value="JobAgentS3Name",
        )
        push_objects(bucket=bucket, files=files, destinations=destinations)

    if stack_params := cf.describe_stacks(StackName=root_stack_name)["Stacks"][0].get(
        "Parameters"
    ):
        for param in stack_params:
            if param.get("ParameterKey") == "ChildEnabled":
                if param.get("ParameterValue") == "true":
                    break
                update_stacks(cf_params=root_params)
    cleanup(folder_path=os.listdir(path="."))


if __name__ == "__main__":
    main()
