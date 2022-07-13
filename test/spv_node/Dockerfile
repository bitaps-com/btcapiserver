FROM ubuntu:18.04
MAINTAINER Aleksey Karpov <admin@bitaps.com>
RUN apt-get update

RUN apt-get -y install python3.8 python3.8-dev
RUN apt install -y python3-pip
RUN unlink  /usr/bin/python3;ln -s /usr/bin/python3.8 /usr/bin/python3

RUN apt-get -y install build-essential libtool autotools-dev automake pkg-config libssl-dev libevent-dev
RUN apt-get -y install libssl1.0-dev
RUN apt-get -y install git
COPY requirements.txt .
RUN pip3 install --requirement requirements.txt
RUN mkdir /config
RUN mkdir /app
COPY ./ ./app
WORKDIR /app
ENTRYPOINT ["python3", "main.py"]
