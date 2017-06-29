FROM ubuntu

RUN apt-get update && apt-get install -y gcc make bzip2 zlib1g-dev ncurses-dev
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pip
RUN apt-get update && apt-get install -y wget

RUN mkdir /aspera/
RUN wget http://download.asperasoft.com/download/sw/connect/3.6.2/aspera-connect-3.6.2.117442-linux-64.tar.gz -P /aspera/
RUN tar -xvzf /aspera/aspera-connect-3.6.2.117442-linux-64.tar.gz -C /aspera/
RUN useradd -m aspera
RUN usermod -L aspera
RUN runuser -l aspera -c '/aspera/aspera-connect-3.6.2.117442-linux-64.sh'

RUN python3 -m pip install boto3
RUN python3 -m pip install requests
RUN python3 -m pip install hvac
RUN python3 -m pip install hvac
ADD samtools-1.3.1.tar.bz2 samtools.tar.bz2
RUN cd samtools.tar.bz2 && cd samtools-1.3.1 && make
ENV PATH /samtools.tar.bz2/samtools-1.3.1/:$PATH
ENV AWS_CONFIG_FILE /.aws/config

RUN mkdir /output/

RUN mkdir /.aws/
COPY config /.aws/config

COPY bam_rehead.sh /output/bam_rehead.sh
RUN chmod 700 /output/bam_rehead.sh

COPY bam_extract_header.sh /output/bam_extract_header.sh
RUN chmod 700 /output/bam_extract_header.sh

COPY poll_process.py /output/poll_process.py

CMD ["python3","/output/poll_process.py"]