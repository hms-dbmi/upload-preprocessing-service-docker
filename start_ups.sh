#!/usr/bin/env bash

ONETIME_TOKEN=$(vault token-create -policy="ups-app-secrets" -use-limit=4 -ttl="1m" -format="json" | jq -r .auth.client_token)

AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)

docker stop ups
docker rm ups

docker run -t -v $(pwd):/output --name ups \
			-e VAULT_SKIP_VERIFY=1 \
			-e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
			-e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
			-e ONETIME_TOKEN=$ONETIME_TOKEN \
			-e QUEUE_NAME="upload-preprocessing-dev" \
			-e VAULT_ADDRESS="https://vault.aws.dbmi.hms.harvard.edu:443" dbmi/upload-preprocessing-service-docker

#docker exec -i -t ups /bin/bash
