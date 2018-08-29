# upload-preprocessing-service

This repo contains a docker container that will process VCF and BAM files to send to DBGap. The code relies on messages from SQS to know which files to process. Use the `ups-aws-infrastructure` repo to stand up the necessary AWS services (EC2 & ECS) to run this container. For more information, please read: https://hms-dbmi.atlassian.net/wiki/spaces/UDN/pages/74383364/UPS.

## Uploading docker images to Amazon ECR
`$(aws ecr get-login --no-include-email --region us-east-1)`
`docker build -t ups .`
`docker tag ups:latest 646975045128.dkr.ecr.us-east-1.amazonaws.com/ups:latest`
`docker push 646975045128.dkr.ecr.us-east-1.amazonaws.com/ups:latest`