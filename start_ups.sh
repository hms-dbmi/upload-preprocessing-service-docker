ONETIME_TOKEN=$(vault token-create -policy="ups-app-secrets" -use-limit=2 -ttl="1m" -format="json" | jq -r .auth.client_token)

AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)

docker stop samtools
docker rm samtools

docker run -t -d -v $(pwd):/output --name samtools -e VAULT_SKIP_VERIFY=1 -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e ONETIME_TOKEN=$ONETIME_TOKEN -e VAULT_ADDRESS="https://vault.aws.dbmi.hms.harvard.edu:443" dbmi/upload-preprocessing-service-docker

#docker exec -i -t samtools /bin/bash
#docker run -i -t -v $(pwd):/output quagbrain/tophat-bowtie2-samtools /bin/bash
#samtools view -H /output/somebam.bam | sed -e 's/LN:249250621/LN:UDN1234/' | samtools reheader - /output/somebam.bam > /output/somebam.reheader.bam
#samtools view -H /output/somebam.reheader.bam
#docker run -t --name samtools dbmi/upload-preprocessing-service-docker