docker stop samtools
docker rm samtools

docker run -d -t -v $(pwd):/output --name samtools dbmi/upload-preprocessing-service-docker

#docker exec -i -t samtools /bin/bash
#docker run -i -t -v $(pwd):/output quagbrain/tophat-bowtie2-samtools /bin/bash
#samtools view -H /output/somebam.bam | sed -e 's/LN:249250621/LN:UDN1234/' | samtools reheader - /output/somebam.bam > /output/somebam.reheader.bam
#samtools view -H /output/somebam.reheader.bam