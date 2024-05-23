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


Amazon Jobs Alertbot is an AWS-powered job alert agent that scrapes amazon.jobs with your search criteria and emails you updates... because Amazon apparently doesn't want you to be able to get updates the easy way.


## This project isn't dead; it's mostly functional, I just need to finish up the email templating. I'm taking a break to work on other things for awhile.

<!-- ABOUT THE PROJECT -->
## About Amazon Job Alertbot

Amazon Job Alertbot was born over a weekend (probably more like 6 hours collectively...) out of a simple frustration -- unlike *every other jobs site on earth* -- Amazon jobs doesn't allow you to set up an update agent. It's clearly a deliberate choice, for reasons I can't discern. 

Because I enjoy irony, I designed Amazon Jobs Alertbot for easy deployment as a CloudFormation stack of AWS lambda function/step function microservices: DynamoDB to store jobs, Systems Manager's ParamStore to store configurations, CloudWatch Events to trigger the function at a time of your choosing, and Simple Email Service to send you emails with new jobs matching your criteria, and of course CloudWatch for logging. All wrapped up with an automatic deployment script. Just in case someone at Amazon thought adding jobs alerts would be too hard; I wanted to make it easy for them if they changed their mind.

It started as a simple lambda function but then I got sucked into an exploratory rabbit hole of orchestrating a rube goldbergian microservice deployment to handle the task. Completely impractical, but I learned a lot along the way (biggest lesson: just use the cdk if you can program... debugging CloudFormation templates is very challenging, but does teach you a lot about how the cdk works under the hood). I didn't really design it to scale, but some simple changes would fix that (changing how the database stores data, using more distributed processes in the state machine -- I experimented with them but didn't want the costs associated since I'm just experimenting).

Of course, you can also easily modify the scripts to run locally, or on an instance of your choice.



<!-- GETTING STARTED -->
## Getting Started

I'll be adding a SAM template for automated deployment soon; in the meantime:

The script is ready to deploy as a Lambda function. To get it running, you need to:

### On Your Computer:

- Clone the repo
- Complete the [config/config.toml](/config/config.toml) to match your desired search criteria and settings. **NOTE: All fields are required even if left blank**
- Configure AWS CLI for access (`aws configure`) if you haven't already.
- run `python config/toml_to_ssm_paramstore.py` to automagically add your config parameters to your paramstore. **Your AWS role must have ssm:PutParameter permissions in IAM**

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
