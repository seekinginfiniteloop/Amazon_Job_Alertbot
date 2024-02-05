# Copyright © 2024 Adam Poulemanos
# MIT license; see LICENSE.MD for details.

# This simple script pushes your config.toml to AWS Systems Manager Parameter Store --
# your role must have ssm:PutParameter permissions. Saves you the effort of manually
# entering them on their form.

# Use the config.toml to personalize your settings and then use this to push them to
# #AWS.

import boto3
import tomllib

app_name = "my-app"  # Replace with your app name

ssm = boto3.client("ssm")

with open("config.toml", "rb") as f:
    config = tomllib.load(f)

for key, value in config.items():
    ssm.put_parameter(
        Name=f"/{app_name}/{key}", Value=str(value), Type="String", Overwrite=True
    )
