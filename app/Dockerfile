FROM ubuntu:18.04
MAINTAINER Aleksey Karpov <admin@bitaps.com>
RUN apt-get update
RUN apt install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt -y install python3.8
RUN apt -y install python3.8-dev
RUN update-alternatives --install /usr/bin/python python3 /usr/bin/python3.8 1
RUN update-alternatives  --set python3 /usr/bin/python3.8
RUN update-alternatives --config python3
RUN apt-get -y install python3.8-distutils
RUN apt-get -y install wget
RUN python3 --version
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3.8 get-pip.py
RUN apt-get -y install build-essential libtool autotools-dev automake pkg-config libssl-dev libevent-dev
RUN apt-get -y install libssl1.0-dev
RUN apt-get -y install libzbar0
RUN apt-get -y install git
COPY requirements.txt .
RUN pip3 install --requirement requirements.txt
RUN mkdir /config
RUN mkdir /app
COPY ./ ./app
WORKDIR /app
ENTRYPOINT ["python3.8", "main.py"]