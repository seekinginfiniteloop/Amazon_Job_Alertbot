import argparse
import base64
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
from boto3.resources import factory
from botocore import client
from botocore.exceptions import ClientError, WaiterError

s3: client = boto3.client("s3")
s3r: factory = boto3.resource("s3")
cf: client = boto3.client("cloudformation")
sts: client = boto3.client("sts")
ec2: client = boto3.client("ec2")
session = boto3.session.Session()

ParamDictType = dict[str, str | list[str | dict[str, str | None]] | bool | None]

update_lambdas = False


def parse_cli() -> dict[str, str]:
    """
    Parses command line arguments.

    Returns:
        kwarg-style dictionary The parsed arguments.
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
        help="tag key to apply to all resources in the stack. Defaults to 'jobalertsagent'.",
        default="jobalertsagent",
        dest="tagkey",
    )
    parser.add_argument(
        "-n",
        "--newstack",
        action="store_true",
        help="create a new stack, ignoring existing stacks",
        dest="newstack",
    )
    return vars(parser.parse_args())


def gen_temp_s3(base_name: str, suffix: str) -> str:
    """
    Generates a temporary S3 bucket for the stack.

    Args:
        base_name: The base name of the stack.
        suffix: The hash suffix for the stack

    Returns:
        str: The name of the temporary S3 bucket.
    """
    with contextlib.suppress(ClientError):
        buckets = s3.list_buckets()
        if existing_buckets := buckets.get("Buckets"):
            found_bucket: str | None = next(
                (
                    bucket.get("Name")
                    for bucket in existing_buckets
                    if bucket.get("Name").startswith(base_name)
                    and bucket.get("Name").endswith("buildbucket")
                ),
                None,
            )
    bucket_name: str = found_bucket or f"{base_name}-temp-{suffix}-buildbucket"
    try:
        waiter = s3.get_waiter("bucket_exists")
        response = s3.create_bucket(Bucket=bucket_name)
        waiter.wait(Bucket=bucket_name)
        return response.get("Location")
    except ClientError as e:
        print(f"failed to create a temporary bucket for managing the build, error: {e}")
        raise e


def empty_and_delete_bucket(bucket_name: str) -> None:
    """
    Empties and deletes a temporary S3 bucket.

    Args:
        bucket_name: The name of the temporary S3 bucket.
    """
    bucket = s3r.Bucket(bucket_name)
    bucket.objects.all().delete()
    bucket.delete()


def clean_unfinished(base_name: str) -> None:
    """
    Cleans up unfinished stacks.

    Args:
        base_name: The base name of the stack.
    """
    with contextlib.suppress(ClientError):
        if stacks := cf.list_stacks(
            StackStatusFilter=[
                "DELETE_FAILED",
                "CREATE_FAILED",
                "ROLLBACK_FAILED",
                "DELETE_IN_PROGRESS",
            ]
        ).get("StackSummaries"):
            for stack in stacks:
                if base_name in stack.get("StackName"):
                    if bucket := next(
                        (
                            r
                            for r in cf.describe_stack_resources(
                                StackName=stack.get("StackName")
                            ).get("StackResources")
                            if r.get("ResourceType").endswith("Bucket")
                        ),
                        None,
                    ):
                        s3r.Bucket(
                            bucket.get("PhysicalResourceId")
                        ).objects.all().delete()
                    with contextlib.suppress(WaiterError):
                        roll_wait = cf.get_waiter("stack_rollback_complete")
                        cf.rollback_stack(
                            StackName=stack.get("StackName"), RetainExceptOnCreate=True
                        )
                        roll_wait.wait(StackName=stack.get("StackName"))
                        if still_there := cf.describe_stacks(
                            StackName=stack.get("StackName")
                        ).get("Stacks"):
                            stack = still_there[0]
                            waiter = cf.get_waiter("stack_delete_complete")
                            cf.delete_stack(StackName=stack.get("StackName"))
                            waiter.wait(StackName=stack.get("StackName"))
                        break


def assemble_params(
    args: dict[str, str],
) -> ParamDictType:
    """
    Assembles parameters for the stack.

    Args:
        args: The parsed command line arguments.
    Returns:
        dict: The assembled parameters.
    """
    base_name: str = args["basename"]
    clean_unfinished(base_name=base_name)
    components: list[str] = base_name.split("-")
    base_name_title: str = "".join([item.title() for item in components])
    suffix: str = gen_hash_suffix(base_name=base_name, newstack=args["newstack"])
    root_stack_name: str = set_stack_name(
        stack_name=f"{base_name}-root-stack",
        suffix=suffix,
        newstack=args["newstack"],
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
        "StackName",
    ]
    parameters = {
        "YourEmail": args["youremail"],
        "BaseName": base_name,
        "BaseNameTitleCase": base_name_title,
        "EventSchedule": args["event_schedule"],
        "LangCode": args["langcode"],
        "IETFCode": args["ietf_code"],
        "TagKey": args["tagkey"].lower(),
        "HashSuffix": suffix,
        "UserArns": user_arns,
        "StackName": root_stack_name,
    }

    return {
        "StackName": root_stack_name,
        "TemplateURL": "",  # We'll add this later
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
        "Parameters": [get_kv(k=k, v=parameters[k]) for k in param_keys],
        "Tags": [
            get_kv(k=args["tagkey"].lower(), v=args["tagkey"].lower(), prefix=""),
            get_kv(k="StackName", v=f"{base_name}-root-stack", prefix=""),
        ],
        "DisableRollback": True,
        # "EnableTerminationProtection": True,
    }


def fix_tag_keys(tag_key: str, template: Path) -> Path:
    """
    Fixes tag keys in the template. Because map keys can't contain intrinsic functions, we have to do this the old fashioned way.

    Args:
        tag_key: The tag key to fix.
        template: The template to fix.

    Return:
        the new Path of the adjusted template
    """
    with open(file=template, mode="r") as f:
        data: str = f.read()
    start_pt: int = re.search(pattern=r"(Resources[:])", string=data).end()
    static, updated = data[:start_pt], data[start_pt:]
    updated: str = updated.replace("jobalertsagent", tag_key)
    new_data: str = static + updated
    new_path: Path = template.with_name(f"{template.stem}_adjusted{template.suffix}")
    with open(file=new_path, mode="w") as f:
        f.write(new_data)
    return new_path


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


def get_template() -> tuple[Path, Path]:
    """
    Retrieves the paths of templates from the templates directory.

    Returns:
        A tuple with the paths of root and statemachine templates, if found.
    """
    templates_dir = Path.cwd() / "templates"
    path = templates_dir if templates_dir.exists() else reorient() / "templates"
    return next(path.glob(pattern="*root*.yaml"), None)


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
                    get_v_for_k(
                        kv_list=stack.get("Outputs"),
                        kval="Suffix",
                        prefix="Output",
                    )
                    or stackname.split("-")[-1]
                )
    return secrets.token_hex(6)


def stack_name_matches(stack: dict[str, Any], base_name: str) -> Any | bool:
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


def stack_exists(stack_name: str) -> bool | dict[str, Any]:
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


def deploy_cloud(params: ParamDictType) -> None | list[dict[str, str]]:
    """
    Deploys a CloudFormation stack using the specified parameters. If it already
    exists, updates the stack.

    Args:
        params: The parameters for creating the stack.

    Returns:
        returns stack outputs if any

    """

    try:
        cf.create_stack(**params)
        print("Waiting for stack to be created...")

        return await_response(stack_name=params["StackName"])

    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise e
        return update_stacks(params=params, update=True)


def get_kv(k: str, v: str, prefix: str = "Parameter") -> dict[str, str | bool]:
    """
    Returns a dictionary containing the specified key-value pair.

    Args:
        k: The key of the parameter.
        v: The value of the parameter.
        prefix: Prefix for the key and value keys, defaults to "Parameter"

    Returns:
        dict: A dictionary containing the key-value pair.
    """
    return {f"{prefix}Key": k, f"{prefix}Value": v}


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
    child_exists = get_v_for_k(
        kv_list=stack["Outputs"], kval="TriggerEvent", prefix="Output"
    )
    bucket_name: str | None = get_v_for_k(
        kv_list=stack["Outputs"], kval="JobAgentS3Name", prefix="Output"
    )
    params["TemplateURL"] = get_template_url(s3key=s3key, bucket=bucket_name)

    for param in params["Parameters"]:
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
    params["Parameters"].append(get_kv(k="ChildEnabled", v="true"))
    if child_exists:
        pop_if_present(params=params, key="TemplateBody")
        params["Tags"].append(
            get_kv(k="AppManagerCFNStackKey", v=stack.get("StackId"), prefix="")
        )
        # params["RoleARN"] = get_v_for_k(
        #    kv_list=stack["Outputs"], kval="DeploymentRoleArn", prefix="Output"
        # )
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


def update_stacks(params: ParamDictType, s3key: str) -> None | list[dict[str, str]]:
    """
    Updates the CloudFormation stack with the given parameters.

    Args:
        params: The parameters for updating the stack.
        s3key: key for the CF template

    Returns:
        None | list[dict[str, str]]: If the stack is a child stack, returns None. Otherwise, returns a list of outputs from the stack update.

    Raises:
        None
    """
    print("stack exists; updating stack...")
    stack = cf.describe_stacks(StackName=params["StackName"])["Stacks"][0]
    set_update_params(params=params, stack=stack, s3key=s3key)
    try:
        response = cf.update_stack(**params)
        return await_response(stack_name=params["StackName"], update=True)
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
        if not s3_key.endswith(".zip"):
            update_stacks(params=root_params, s3key=s3_key)
            break


def push_objects(bucket: str, suffix: str) -> None:
    """
    Pushes the specified files to the specified S3 destinations.

    Args:
        bucket (str): The name of the S3 bucket.
        suffix (str): The random hash suffix.
    """
    cwd: Path = Path.cwd()
    destinations = []
    templates_path: Path = cwd / "templates"
    if not templates_path.exists():
        templates_path = reorient() / "templates"

    template_s3 = "templates/"
    for file in templates_path.iterdir():
        if file.suffix == ".yaml" and "root" in file.stem:
            destinations.append(
                (f"{template_s3}cf-root-{suffix}.yaml", file, "text/plain")
            )
            break

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


def zip_script(script_path: Path) -> io.BufferedReader:
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


def object_exists(s3object) -> bool:
    """
    Checks if the S3 object exists.

    Args:
        s3object: The S3 Object object.
    Returns:
        bool: True if the object exists, False otherwise.
    """
    with contextlib.suppress(ClientError, AttributeError):
        s3object.load()
        return bool(s3object.key)


def check_and_upload_to_s3(
    bucket: str, files: list[tuple[str, Path | io.BufferedReader, str]]
) -> None:
    """
    Checks if a file exists in an S3 bucket and uploads it if it doesn't.

    Args:
        bucket: The name of the S3 bucket.
        files: A list of files to check and upload in the form, s3 keyname, local Path object or BufferedReader, and content type.
    """
    s3bucket = s3r.Bucket(bucket)
    for file in files:
        s3_key, file_obj, content_type = file
        s3obj = s3bucket.Object(s3_key)
        upload_kwargs = {
            "bucket": bucket,
            "file_obj": file_obj,
            "s3_key": s3_key,
            "content_type": content_type,
        }
        if bucket_empty(bucket=s3bucket):
            upload_to_s3(**upload_kwargs)
        elif (existing_object := object_exists(s3object=s3obj)) and is_different(
            file_obj=file_obj, s3object=s3obj
        ):
            upload_to_s3(**upload_kwargs)
            if content_type == "application/zip" and not globals().get(
                "update_lambdas"
            ):
                globals()["update_lambdas"] = True

        elif not existing_object:
            upload_to_s3(**upload_kwargs)


def is_different(file_obj: Path | io.BufferedReader, s3object: str) -> bool:
    """
    Compares the hash of the local file with the hash of the S3 object and returns True if they are different.

    Args:
        file_obj (Path | BufferedReader): The path to the local file or the BytesIO BufferedReader object
        s3object: The S3 Object object.

    Returns:
        bool: True if the hash of the local file is different from the hash of the S3 object, False otherwise.
    """
    with contextlib.suppress(ClientError, AttributeError):
        s3object.load()
        if object_sha := s3object.checksum_sha1:
            return get_hash_digest(file_obj=file_obj) != object_sha.strip('"')
    return True


def get_hash_digest(file_obj: Path | io.BufferedReader, algo: str = "sha1") -> str:
    """
    Calculates the base64-encoded hash of a file. Defaults to SHA1.

    Args:
        file_obj (Path | BufferedReader): The path to the file or BytesIO object

    Returns:
        str: The base64-encoded hash digest of the file, decoded to utf-8.

    """
    buff: io.BufferedReader = (
        file_obj.open(mode="rb") if isinstance(file_obj, Path) else file_obj
    )
    buff.seek(0)
    h = hashlib.new(name=algo, data=buff.read())
    return base64.b64encode(h.digest()).decode("utf-8")


def upload_to_s3(
    bucket: str,
    file_obj: Path | io.BufferedReader,
    s3_key: str,
    content_type: str | None = None,
) -> None:
    """
    Uploads a file to an S3 bucket.

    Args:
        bucket: The name of the S3 bucket.
        file_obj: The path to the file to be uploaded or BytesIO object
        s3_key: The destination path in the S3 bucket.
        content_type (optional): content-type of object to upload

    Returns:
        None
    """
    waiter = s3.get_waiter("object_exists")
    args = parse_cli()
    tagkey: str = args["tagkey"].lower()
    now = datetime.now()
    print(
        f"File upload: bucket: {bucket}\n, file_obj: {file_obj}\n, content_type: {content_type}\n"
    )
    body: io.BufferedReader = (
        file_obj.open(mode="rb") if isinstance(file_obj, Path) else file_obj
    )
    body.seek(0)
    sha_hash = get_hash_digest(file_obj=body)
    body.seek(0)
    kwargs_dict: dict[str, bytes | str | bool | None] = {
        "Body": body,
        "Bucket": bucket,
        "Key": s3_key,
        "ContentMD5": get_hash_digest(file_obj=file_obj, algo="md5"),
        "ContentType": content_type,
        "ChecksumSHA1": get_hash_digest(file_obj=file_obj),
        "BucketKeyEnabled": True,
        "Tagging": quote(string=f"{tagkey}={tagkey}"),
    }
    response = s3.put_object(**kwargs_dict)
    waiter.wait(
        Bucket=bucket,
        Key=s3_key,
        ChecksumMode="ENABLED",
        IfModifiedSince=now,
        WaiterConfig={"Delay": 2, "MaxAttempts": 6},
    )
    if (
        response.get("ResponseMetadata").get("HTTPStatusCode") == 200
        and response.get("ResponseMetadata").get("ChecksumSHA1") == sha_hash
    ):
        print(f"File uploaded to {bucket}/{s3_key} and checksum validated.\n\n")


def get_v_for_k(kv_list: list[dict[str, Any]], kval: str, prefix: str = "") -> Any:
    """
    Returns the next value for the given key in the dictionary list.

    Args:
        kv_list (list[dict[str, Any]]): The list of dictionaries containing
            key-value pairs. Arranged in format:
            {'Key': 'key_val, 'Value': 'value'}
        kval (str): Name of the key to retrieve a value for.
        prefix (str): The key prefix to append to keys 'Key' and 'Value'. Default is an empty string. For example, 'Parameter' will result in 'ParameterKey': 'key', 'ParameterValue': 'value'.

    Returns:
        Any: The next value for the given key, or None if the key is not found.
    """
    return next(
        (
            kvdict.get(f"{prefix}Value")
            for kvdict in kv_list
            if kvdict.get(f"{prefix}Key") == kval
        ),
        None,
    )


def cleanup() -> None:
    """
    Deletes any temporarily created files.

    """
    folder_path = (
        Path.cwd()
        if "templates" in [dir.name for dir in Path.cwd().iterdir() if dir.is_dir()]
        else reorient()
    )
    templates = set(folder_path.glob(pattern="*.yaml"))
    for template in templates:
        if template.stem.endswith("adjusted"):
            os.remove(path=template)


def update_lambda_functions(stack, bucket) -> None:
    """
    Updates the lambda functions in the specified stack.

    Args:
        stack (dict): The CloudFormation stack.
        bucket (str): The name of the S3 bucket.

    Returns:
        None
    """
    lambda_arn_string: str = get_v_for_k(
        kv_list=stack["Outputs"], kval="lambda_funcs", prefix="Output"
    )
    if not lambda_arn_string:
        return
    lambda_functions: list[str] = lambda_arn_string.split(",")
    keys = [
        f"lambdas/{name}"
        for name in [
            "var_replacer.zip",
            "job_scraper.zip",
            "job_store.zip",
            "job_sender.zip",
            "error_handler.zip",
        ]
    ]
    l = boto3.client("lambda")
    funcs = zip(lambda_functions, keys)
    for func_arn, key in funcs:
        l.update_function_code(
            FunctionName=func_arn, S3Bucket=bucket, S3Key=key, Publish=True
        )
    print("Lambda functions updated.\n\n")


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
    root_template = get_template()
    cli_args = parse_cli()
    root_params: ParamDictType = assemble_params(args=cli_args)
    params = root_params.get("Parameters")
    if (
        tag_key := get_v_for_k(kv_list=params, kval="TagKey", prefix="Parameter")
    ) != "jobalertsagent":
        root_template: Path = fix_tag_keys(tag_key=tag_key, template=root_template)
    root_stack_name = root_params.get("StackName")
    suffix = get_v_for_k(kv_list=params, kval="HashSuffix", prefix="Parameter")
    base_name = get_v_for_k(kv_list=params, kval="BaseName", prefix="Parameter")

    if stack := stack_exists(stack_name=root_stack_name):
        if not cli_args.get("newstack"):
            bucket = get_v_for_k(
                kv_list=stack.get("Outputs"),
                kval="JobAgentS3Name",
                prefix="Output",
            )
    else:
        temp_bucket = gen_temp_s3(base_name=base_name, suffix=suffix).strip("/")
        upload_to_s3(
            bucket=temp_bucket,
            file_obj=root_template,
            s3_key=f"build-temp-{root_template.name}",
            content_type="text/plain",
        )
        root_params["TemplateURL"] = get_template_url(
            s3key=f"build-temp-{root_template.name}", bucket=temp_bucket
        )
        output = deploy_cloud(params=root_params)
        if (
            cf.describe_stacks(StackName=root_stack_name)["Stacks"][0].get(
                "StackStatus"
            )
            != "CREATE_COMPLETE"
        ):
            raise ValueError("Stack creation failed.")
        empty_and_delete_bucket(bucket_name=temp_bucket)
        bucket = get_v_for_k(kv_list=output, kval="JobAgentS3Name", prefix="Output")
    push_and_update(bucket=bucket, suffix=suffix, root_params=root_params)
    root_key = f"templates/cf-root-{suffix}.yaml"
    if stack := cf.describe_stacks(StackName=root_stack_name)["Stacks"][0]:
        if (
            get_v_for_k(kv_list=stack["Outputs"], kval="ChildExists", prefix="Output")
            == "true"
        ):
            update_stacks(params=root_params, s3key=root_key)
            if update_lambdas:
                update_lambda_functions(stack=stack, bucket=bucket)
            with contextlib.suppress(ClientError):
                response = boto3.client("sns").subscribe(
                    TopicArn=get_v_for_k(
                        kv_list=stack.get("Outputs"),
                        kval="TopicArn",
                        prefix="Output",
                    ),
                    Protocol="email",
                    Endpoint=parse_cli().get("youremail"),
                    ReturnSubscriptionArn=True,
                )
                print(f"subscription arn: {response.get("SubscriptionArn")}")
        else:
            cleanup()
    else:
        update_stacks(params=root_params, s3key=root_key)
    cleanup()


if __name__ == "__main__":
    main()
