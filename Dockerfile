FROM ubuntu

RUN apt-get update && apt-get install -y gcc make bzip2 zlib1g-dev ncurses-dev
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pip

RUN python3 -m pip install boto3
RUN python3 -m pip install requests
ADD samtools-1.3.1.tar.bz2 samtools.tar.bz2
RUN cd samtools.tar.bz2 && cd samtools-1.3.1 && make
ENV PATH /samtools.tar.bz2/samtools-1.3.1/:$PATH

RUN mkdir /output/

COPY poll_process.py /output/poll_process.py

RUN mkdir ~/.aws/
COPY config ~/.aws/config

CMD ["python3","/output/poll_process.py","upload-preprocessing"]