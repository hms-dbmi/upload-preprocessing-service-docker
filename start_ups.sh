#!/usr/bin/env bash

AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)

docker stop ups
docker rm ups

docker run -t -d -v $(pwd)/scratch/:/scratch --name ups \
			-e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
			-e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
			-e QUEUE_NAME="upload-preprocessing" dbmi/upload-preprocessing-service-docker

#docker exec -i -t ups /bin/bash
