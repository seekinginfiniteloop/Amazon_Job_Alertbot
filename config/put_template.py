# Copyright © 2024 Adam Poulemanos
# MIT license; see LICENSE.MD for details.

# This simple script pushes your config.toml to AWS Systems Manager Parameter Store --
# your role must have ssm:PutParameter permissions. Saves you the effort of manually
# entering them on their form.

# Use the config.toml to personalize your settings and then use this to push them to
# #AWS.


import boto3

ses_client = boto3.client("sesv2", region_name=boto3.session.Session().region_name)

template_data = {}
template_data = {
    "Subject": "Your Amazon Jobs Update for {{current_date}}",
    "Html": "<html><head></head><body><h1>Your Amazon Jobs Update for {{current_date}}</h1><br>new jobs: {{jobs_count}}<br>{{#each jobs_data}}<p><h2><a href='https://amazon.jobs{{this.job_path}}'>{{this.title}}, {{this.city}}</a></h2></p>{{this.description_short}}</p><br><p><a href='https://amazon.jobs{{this.url_next_step}}'>apply</a></p><br>{{/each}}</body></html>",
}
ses_client.create_email_template(
    TemplateName="AmznJobsNotice", TemplateContent=template_data
)
