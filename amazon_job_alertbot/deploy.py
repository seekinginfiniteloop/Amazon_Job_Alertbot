import argparse
import contextlib
import hashlib
import io
import os
import re
import secrets
import time
import zipfile
from datetime import datetime
from pathlib import Path
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
        dest="youremail",
    )
    parser.add_argument(
        "-b",
        "--basename",
        type=str,
        help="Base name of the stacks, defaults to: job-agent. You do not need to supply a title case version; the script will construct one for you",
        default="job-agent",
        dest="basename",
    )
    parser.add_argument(
        "-l",
        "--langcode",
        type=str,
        help="ISO 639 set 1 language code for search results, defaults to en (English)",
        default="en",
        dest="langcode",
    )
    parser.add_argument(
        "-i",
        "--ietf",
        type=str,
        help="IETF language tag for search results, defaults to en-US (U.S. English) see https://gist.github.com/traysr/2001377 for details",
        default="en-US",
        dest="ietf_code",
    )
    parser.add_argument(
        "-r",
        "--rate",
        type=str,
        help="rate used by eventbridge to schedule job searches. Defaults to rate(24 hours) but accepts any event bridge rate.",
        default="rate(24 hours)",
        dest="event_schedule",
    )
    parser.add_argument(
        "-t",
        "--tag",
        type=str,
        help="tag key to apply to all resources in the stack. Defaults to 'JobAlertsAgent'.",
        default="JobAlertsAgent",
        dest="tagkey",
    )
    parser.add_argument(
        "-n",
        "--newstack",
        action="store_true",
        help="create a new stack, ignoring existing stacks",
        dest="newstack",
    )
    return parser.parse_args()


def assemble_params(args: argparse.Namespace, template: str = None) -> ParamDictType:
    """
    Assembles parameters for the stack.

    Args:
        args: The parsed command line arguments.
        template: stack template to insert into params; optional unless this is a create_stack run.

    Returns:
        dict: The assembled parameters.
    """
    args = vars(args)
    base_name: str = args.get("basename", "job-agent")
    components: list[str] = base_name.split("-")
    base_name_title: str = "".join([item.title() for item in components])
    suffix: str = gen_hash_suffix(base_name=base_name, newstack=args.get("newstack"))
    root_stack_name: str = set_stack_name(
        stack_name=f"{base_name}-root-stack",
        suffix=suffix,
        newstack=args.get("newstack"),
    )
    user_arns: str = get_user_arns()

    param_keys = [
        "YourEmail",
        "BaseName",
        "BaseNameTitleCase",
        "EventSchedule",
        "LangCode",
        "IETFCode",
        "TagKey",
        "HashSuffix",
        "UserArns",
    ]
    param_values = [
        args.get("youremail"),
        base_name,
        base_name_title,
        args.get("event_schedule"),
        args.get("langcode"),
        args.get("ietf_code"),
        args.get("tagkey"),
        suffix,
        user_arns,
    ]
    return {
        "StackName": root_stack_name,
        "TemplateBody": template,
        "Capabilities": [
            "CAPABILITY_IAM",
            "CAPABILITY_NAMED_IAM",
            "CAPABILITY_AUTO_EXPAND",
        ],
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
            {"ParameterKey": k, "ParameterValue": v}
            for k, v in zip(param_keys, param_values)
        ],
        "Tags": [
            {"Key": f"{args.get("tagkey")}", "Value": f"{root_stack_name}"},
            {"Key": "StackName", "Value": f"{base_name}-root-stack"},
        ],
        "DisableRollback": True,
        # "EnableTerminationProtection": True,
    }


def reorient(loop_count: int = 0) -> Path:
    """
    Returns the path of the directory containing the main execution script.

    Args:
        loop_count (int): The number of iterations performed.

    Returns:
        Path: The path of the directory containing the main execution script.

    Raises:
        ValueError: If the proper working directory cannot be located within 3 levels.
    """
    cwd = Path.cwd()
    while loop_count < 3:
        for path, dirs, files in cwd.walk(top_down=False):
            if any(file.endswith(".py") for file in files) and any(
                dir in {"lambdas", "templates"} for dir in dirs
            ):
                return path
        os.chdir(path="..")
        loop_count += 1
    raise ValueError(
        "Could not locate proper working directory within 3 levels, please change working directory to folder with main execution script."
    )


def get_templates() -> tuple[Path, Path]:
    """
    Retrieves the paths of templates from the templates directory.

    Returns:
        A tuple with the paths of root and statemachine templates, if found.
    """
    templates_dir = Path.cwd() / "templates"
    path = templates_dir if templates_dir.exists() else reorient() / "templates"
    stack_template = next(path.glob(pattern="*root*.yaml"), None)
    statemachine_template = next(path.glob(pattern="*machine*.json"), None)
    return stack_template, statemachine_template


def gen_hash_suffix(base_name: str, newstack: bool = False) -> str:
    """
    Generates a random hash suffix of length 6.
    If one has not been generated. Otherwise, returns it.

    If newstack, ignores existing stacks and generates a new suffix.

    Returns:
        str: The generated hash suffix.

    """
    if not newstack:
        for stack in cf.describe_stacks()["Stacks"]:
            stackname = stack["StackName"]
            if stackname.startswith(base_name) and (
                len(stackname.split("-")[-1]) == 12
            ):
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


def stack_name_matches(stack, base_name) -> Any | bool:
    """
    Checks if the stack name matches the base name, excluding the suffix and not marked for deletion.

    Args:
        stack: The stack dictionary.
        base_name: The base name to compare against.

    Returns:
        bool: True if the stack name matches the base name, False otherwise.
    """
    name_without_suffix = stack["StackName"].rsplit("-", 1)[0]
    return base_name == name_without_suffix and not stack.get("StackStatus").startswith(
        "DELETE"
    )


def set_stack_name(stack_name: str, suffix: str, newstack: bool = False) -> str:
    """
    Sets the stack name with a random hash suffix.

    Args:
        stack_name: The base stack name.
        suffix: The random hash suffix.
        newstack: if True, ignores existing stacks and generates a new stackname.

    Returns:
        str: The stack name with the random hash suffix.
    """
    return next(
        (
            stack["StackName"]
            for stack in cf.describe_stacks()["Stacks"]
            if stack_name_matches(stack=stack, base_name=stack_name) and not newstack
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


def get_template_url(s3key: str, bucket: str) -> str:
    """
    Returns the URL of the template file for CloudFormation stack creation.

    Args:
        s3key (str): Template s3 key
        bucket (str): The name of the S3 bucket where the template file is stored.

    Returns:
        str: The URL of the template file.
    """
    return quote(
        string=f"https://{bucket}.s3.{get_url_suffix()}/{s3key}",
        safe=":/",
    )


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


def stack_exists(stack_name: str) -> bool | dict:
    """
    Checks if a parent stack with the specified name exists.

    Args:
        stack_name: The name of the parent stack.

    Returns:
        The stack if it exists, else False
    """
    with contextlib.suppress(ClientError):
        if stacks := cf.describe_stacks(StackName=stack_name).get("Stacks"):
            return stacks[0]
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


def await_response(stack_name: str, update: bool = False) -> None | Any:
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
        "WaiterConfig": {"Delay": 5, "MaxAttempts": 250},
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
        return update_stacks(cf_params=cf_params, update=True)


def set_update_params(params: ParamDictType, stack: dict[str, Any], s3key: str) -> None:
    """
    Set update parameters for the stack.

    Args:
        params: A dictionary containing the parameters for the stack update.
        stack: A dictionary representing the stack.
        s3key: Key for the CF template

    Returns:
        None
    """
    child_exists = get_output_value(output=stack["Outputs"], key_value="TriggerEvent")
    bucket_name: str | None = get_output_value(
        output=stack["Outputs"], key_value="JobAgentS3Name"
    )

    for param in params["Parameters"]:
        if param["ParameterKey"] == "HashSuffix":
            suffix = param["ParameterValue"]
        if param.get("ParameterKey") not in [
            "EventSchedule",
            "LangCode",
            "LangCountryCode",
            "YourEmail",
        ] and param.get("ParameterValue"):
            param.pop("ParameterValue")
            param["UsePreviousValue"] = True

    pop_if_present(params=params, key="EnableTerminationProtection")

    params["DisableRollback"] = False
    params["Parameters"].append(
        {"ParameterKey": "ChildEnabled", "ParameterValue": "true"}
    )
    if child_exists:
        pop_if_present(params=params, key="TemplateBody")
        params["TemplateURL"] = get_template_url(s3key=s3key, bucket=bucket_name)
        params.pop("TemplateBody")
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


def update_stacks(cf_params: ParamDictType, s3key: str) -> None | list[dict[str, str]]:
    """
    Updates the CloudFormation stack with the given parameters.

    Args:
        cf_params: The parameters for updating the stack.
        s3key: key for the CF template

    Returns:
        None | list[dict[str, str]]: If the stack is a child stack, returns None. Otherwise, returns a list of outputs from the stack update.

    Raises:
        None
    """
    print("stack exists; updating stack...")
    stack = cf.describe_stacks(StackName=cf_params["StackName"])["Stacks"][0]
    set_update_params(params=cf_params, stack=stack, s3key=s3key)
    try:
        response = cf.update_stack(**cf_params)
        return await_response(stack_name=cf_params["StackName"], update=True)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ValidationError":
            raise e
        print("Stack update failed...")


def push_and_update(
    bucket: str | None, suffix: str, root_params: ParamDictType
) -> None:
    """
    Pushes the specified files to the specified S3 destinations and updates stacks if necessary.

    Args:
        bucket (str | None): The name of the S3 bucket.
        suffix (str): The random hash suffix.
        root_params (ParamDictType): The parameters for the root stack.

    Returns:
        None
    """
    files: list[tuple[str, Path | io.BytesIO, str]] = push_objects(
        bucket=bucket, suffix=suffix
    )
    for file in files:
        s3_key: str = file[0]
        if s3_key.endswith(".yaml") and "root" in s3_key:
            update_stacks(cf_params=root_params, s3key=s3_key)
            break


def push_objects(bucket: str, suffix: str) -> list[tuple[str, Path, str]]:
    """
    Pushes the specified files to the specified S3 destinations.

    Args:
        bucket (str): The name of the S3 bucket.
        suffix (str): The random hash suffix.

    Returns:
        list of tuples with values for s3_key, Path, and content-type of objects to upload
    """
    cwd: Path = Path.cwd()
    destinations = []
    templates_path: Path = cwd / "templates"
    if not templates_path.exists():
        templates_path = reorient() / "templates"

    template_s3 = "templates/"
    for file in templates_path.iterdir():
        if file.suffix == ".json" and "machine" in file.stem:
            destinations.append(
                (f"{template_s3}statemachine-{suffix}.json", file, "text/plain")
            )
        elif file.suffix == ".yaml" and "root" in file.stem:
            destinations.append(
                (f"{template_s3}cf-root-{suffix}.yaml", file, "text/plain")
            )

    lambda_path: Path = cwd / "lambdas"
    if not lambda_path.exists():
        lambda_path = reorient() / "lambdas"

    lambda_s3 = "lambdas/"
    for lambda_folder in lambda_path.iterdir():
        if lambda_folder.is_dir():
            zip_file: str = f"{lambda_folder.name}.zip"
            py_file: Path = next(
                file
                for file in lambda_folder.iterdir()
                if file.suffix == ".py" and file.stem == lambda_folder.name
            )
            zip_obj: Path = zip_script(script_path=py_file)
            destinations.append((f"{lambda_s3}{zip_file}", zip_obj, "application/zip"))

    check_and_upload_to_s3(bucket=bucket, files=destinations)
    return destinations


def zip_script(script_path: Path) -> Path:
    """
    Zips a single file in a virtual byte_buffer zip file for uploading

    Args:
        script_path: The path to the script file to zip
        output_filename: The path of the output zip file.

    Returns:
        zip file buffer object
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(script_path, arcname=script_path.name)
        if archive.testzip():
            raise ValueError(
                f"Error zipping file {script_path.name}: {archive.testzip()}"
            )
    buffer.seek(0)
    return buffer


def bucket_empty(bucket) -> bool:
    """
    Checks if the S3 bucket is empty.

    Args:
        bucket: The Bucket object.
    Returns:
        bool: True if the bucket is empty, False otherwise.
    """
    return not bucket.objects.all()


def object_exists(bucket, s3_key: str) -> bool:
    """
    Checks if an object exists in the S3 bucket.

    Args:
        bucket: The Bucket object.
        s3_key: The path to the object.

    Returns:
        bool: True if the object exists, False otherwise.
    """
    try:
        return bool(bucket.Object(s3_key).load())
    except ClientError as e:
        print(f"Client error prevented object check, {e}")
    return False


def check_and_upload_to_s3(
    bucket: str, files: list[tuple[str, Path | io.BytesIO, str]]
) -> None:
    """
    Checks if a file exists in an S3 bucket and uploads it if it doesn't.

    Args:
        bucket: The name of the S3 bucket.
        files: A list of files to check and upload in the form, s3 keyname, local file path, and content type.
    """
    s3bucket = s3r.Bucket(bucket)
    if bucket_empty(bucket=s3bucket):
        for file in files:
            s3_key, file_path, content_type = file
            upload_to_s3(
                bucket=bucket,
                file_path=file_path,
                s3_key=s3_key,
                content_type=content_type,
            )
    else:
        for file in files:
            s3_key, file_path, content_type = file
            if object_exists(bucket=s3bucket, s3_key=s3_key):
                s3object = s3bucket.Object(s3_key)
                if is_different(file_path=file_path, s3object=s3object):
                    upload_to_s3(
                        bucket=bucket,
                        file_path=file_path,
                        s3_key=s3_key,
                        content_type=content_type,
                    )
                    continue
                continue
            upload_to_s3(
                bucket=bucket,
                file_path=file_path,
                s3_key=s3_key,
                content_type=content_type,
            )


def is_different(file_path: Path | io.BytesIO, s3object: str) -> bool:
    """
    Compares the hash of the local file with the hash of the S3 object and returns True if they are different.

    Args:
        file_path (Path): The path to the local file or the BytesIO object
        s3object: The S3 object.

    Returns:
        bool: True if the hash of the local file is different from the hash of the S3 object, False otherwise.
    """
    try:
        with contextlib.suppress(ClientError):
            return get_hash(file_path=file_path) != s3object.e_tag.strip('"')
    except Exception:
        return False


def get_hash(file_path: Path | io.BytesIO) -> str:
    """
    Calculates the MD5 hash of a file.

    Args:
        file_path (Path): The path to the file or BytesIO object

    Returns:
        str: The MD5 hash of the file.

    """
    if isinstance(file_path, io.BytesIO):
        return hashlib.md5(file_path.getvalue()).hexdigest()
    with open(file=file_path, mode="rb") as f:
        hasher = hashlib.md5()
        while chunk := f.read(4096):
            hasher.update(chunk)
        return hasher.hexdigest()


def upload_to_s3(
    bucket: str,
    file_path: Path | io.BytesIO,
    s3_key: str,
    content_type: str | None = None,
) -> None:
    """
    Uploads a file to an S3 bucket.

    Args:
        bucket: The name of the S3 bucket.
        file_path: The path to the file to be uploaded or BytesIO object
        s3_key: The destination path in the S3 bucket.
        content_type (optional): content-type of object to upload

    Returns:
        None
    """
    waiter = s3.get_waiter("object_exists")
    base_name: str = vars(parse_cli()).get("basename")
    now = datetime.now()
    print(
        f"File upload: bucket: {bucket}\n, file_path: {file_path}\n, content_type: {content_type}\n"
    )
    if isinstance(file_path, Path):
        file_path = file_path.open(mode="rb")
    s3.upload_fileobj(
        Fileobj=file_path,
        Bucket=bucket,
        Key=s3_key,
        ExtraArgs={
            "Tagging": quote(string=f"JobAlertsAgent={base_name}-object-{s3_key}"),
            "ContentType": content_type,
        },
    )
    waiter.wait(
        Bucket=bucket, Key=s3_key, IfModifiedSince=now, WaiterConfig={"Delay": 2}
    )


def get_output_value(output: list[dict[str, str]], key_value: str) -> str | None:
    """
    Returns the value associated with the given key in the output list of dictionaries.

    Args:
        output (list[dict[str, str]]): The list of dictionaries containing output information.
        key_value (str): The key to search for in the dictionaries.

    Returns:
        str | None: The value associated with the given key, or None if the key is not found.
    """
    return next(
        (
            result["OutputValue"]
            for result in output
            if result["OutputKey"] == key_value
        ),
        None,
    )


def cleanup(folder_path: Path) -> None:
    """
    Deletes all zip files in the specified folder and its subfolders.

    Args:
        folder_path (Path): The path to the folder to be cleaned up.

    Returns:
        None

    """
    folder_path = (
        folder_path
        if folder_path.exists()
        and "lambdas" in [dir.name for dir in folder_path.iterdir() if dir.is_dir()]
        else reorient()
    )
    zips = set(folder_path.glob(pattern="*.zip"))
    for zip_file in zips:
        os.remove(path=zip_file)


def main() -> None:
    """
    Executes the main deployment process.

    Returns:
        None

    Raises:
        ValueError: If the statemachine template cannot be located or if stack creation fails.
    """
    correct_dir: Path = reorient()
    if Path.cwd() != correct_dir:
        os.chdir(path=correct_dir)
    root_template, states_template = get_templates()
    if not states_template:
        raise ValueError("Couldn't locate statemachine template.")
    root_params = assemble_params(
        args=parse_cli(), template=read_template(template_path=root_template)
    )
    params = root_params.get("Parameters")
    root_stack_name = root_params.get("StackName")
    suffix = next(
        param.get("ParameterValue")
        for param in params
        if param.get("ParameterKey") == "HashSuffix"
    )

    if stack := stack_exists(stack_name=root_stack_name):
        bucket = get_output_value(
            output=stack.get("Outputs"), key_value="JobAgentS3Name"
        )
    else:
        output = deploy_cloud(cf_params=root_params)
        if (
            cf.describe_stacks(StackName=root_stack_name)["Stacks"][0].get(
                "StackStatus"
            )
            != "CREATE_COMPLETE"
        ):
            raise ValueError("Stack creation failed.")
        bucket = get_output_value(output=output, key_value="JobAgentS3Name")
    push_and_update(bucket=bucket, suffix=suffix, root_params=root_params)
    root_key = f"templates/cf-root-{suffix}.yaml"
    if stack := cf.describe_stacks(StackName=root_stack_name)["Stacks"][0]:
        stack_params = stack.get("Parameters")
        for param in stack_params:
            if (
                param.get("ParameterKey") == "ChildEnabled"
                and param.get("ParameterValue") == "true"
            ):
                if not stack.get("RoleARN"):
                    update_stacks(cf_params=root_params, s3key=root_key)
                    break
                else:
                    cleanup()
                    exit()
        else:
            update_stacks(cf_params=root_params, s3key=root_key)
    cleanup(folder_path=Path.cwd())


if __name__ == "__main__":
    main()
