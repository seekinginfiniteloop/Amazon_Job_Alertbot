<!-- PROJECT SHIELDS -->
<!--
*** README template based on Othneil Drew's excellent [Best-README-Template][best-readme-url]
MIT License

Copyright (c) 2021 Othneil Drew

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
-->
# Amazon Job Alertbot

![Amazon Job Alertbot](amazon_alertbot_sm.png "Amazon Job Alertbot")


Amazon Jobs Alertbot is a simple AWS-powered job alert agent that scrapes amazon.jobs with your search criteria and emails you updates... because Amazon apparently doesn't want you to be able to get updates the easy way.


<!-- ABOUT THE PROJECT -->
## About Amazon Job Alertbot

Amazon Job Alertbot was born over a weekend (probably more like 6 hours collectively...) out of a simple frustration -- unlike *every other jobs site on earth* -- Amazon jobs doesn't allow you to set up an update agent. It's clearly a deliberate choice, for reasons I can't discern.

Because I enjoy irony, I designed Amazon Jobs Alertbot for easy deployment as an AWS Lambda function using AWS services: DynamoDB to store jobs, Systems Manager's ParamStore to store configurations, CloudWatch Events to trigger the function at a time of your choosing, and Simple Email Service to send you emails with new jobs matching your criteria, and of course CloudWatch for logging. Just in case someone at Amazon thought adding jobs alerts would be too hard; I wanted to make it easy for them if they changed their mind.

Of course, you can also easily modify the script to run locally, or on an instance of your choice.

Until I find some spare time to provide more detailed instructions, this guide assumes you have at least a rudimentary familiarization with these AWS services and IAM for identity/access management.


<!-- GETTING STARTED -->
## Getting Started

I'll be adding a SAM template for automated deployment soon; in the meantime:

The script is ready to deploy as a Lambda function. To get it running, you need to:

### On Your Computer:

- Clone the repo
- Complete the [config/config.toml](/config/config.toml) to match your desired search criteria and settings. **NOTE: All fields are required even if left blank**
- Configure AWS CLI for access (`aws configure`) if you haven't already.
- run `python config/toml_to_ssm_paramstore.py` to automagically add your config parameters to your paramstore. **Your AWS role must have ssm:PutParameter permissions in IAM**

### On Your AWS Account:

- Set up Simple Email Service if you don't already have it configured
- Create a DynamoDB table for your job alert agent
- [Add the IAM policy](/config/sample_policy.json) and create a new role for your IAM function. You need to replace the placeholders with your function's name, DynamoDB table name, your AWS id, and your region.
- Create the Lambda function using [amazon_job_alertbot/main.py](/amazon_job_alertbot/main.py) and assign your lambda policy/role to the function. I suggest code signing it, which means you need to have or set up a profile in AWS Signer, zip main.py (I provide a zip package for you), and sign the zip with a signing job.
- Add a CloudWatch Events trigger to schedule the Lambda function when you want it to run. You don't need to pass anything in the JSON payload.
- Profit

**The IAM policy ready to copy:**

``` JSON
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:ConditionCheckItem",
                "dynamodb:PutItem",
                "dynamodb:DescribeTable",
                "dynamodb:GetItem",
                "dynamodb:Query",
                "dynamodb:UpdateItem",
                "ses:SendEmail",
                "ssm:GetParametersByPath",
                "ssm:GetParameters",
                "ssm:DescribeParameters",
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:dynamodb:us-east-1:{your_id}:table/{yourtable}",
                "arn:aws:ses:*:{your_id}:configuration-set/*",
                "arn:aws:ses:*:{your_id}:template/*",
                "arn:aws:ses:*:{your_id}:identity/*",
                "arn:aws:ssm:us-east-1:{your_id}:parameter/*",
                "arn:aws:logs:us-east-1:{your_id}:log-group:/aws/lambda/{yourlambdafuncname}:*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:SourceArn": "arn:aws:lambda:us-east-1:{your_id}:function:{yourlambdafuncname}"
                }
            }
        }
    ]
}
```

<!-- CONTRIBUTING -->
## Contributing

See [CONTRIBUTING.md][CONTRIBUTING-url] to contribute... though there's admittedly not much to contribute.


<!-- LICENSE -->
## License

Distributed under the MIT License. See [LICENSE.md][license-url] for more information.


Project Link: [https://github.com/psuedomagi/amazon_job_alertbot](https://github.com/psuedomagi/amazon_job_alertbot)

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[CONTRIBUTING-url]: CONTRIBUTING.md
[best-readme-url]: https://github.com/othneildrew/Best-README-Template/tree/master
[issues-url]: https://github.com/psuedomagi/amazon_job_alertbot/.github/issues
[license-url]: https://github.com/psuedomagi/amazon_job_alertbot/LICENSE.md
