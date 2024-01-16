FROM ubuntu:20.04
MAINTAINER Aleksey Karpov <admin@bitaps.com>
ENV TZ=Europe
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN echo "nameserver 8.8.8.8" > /etc/resolv.conf
RUN apt-get update
RUN apt-get -y install git
RUN apt-get -y install python3
RUN apt-get -y install python3-pip
RUN apt-get -y install build-essential libtool autotools-dev automake pkg-config libssl-dev libevent-dev
RUN pip3 install --upgrade cython
COPY requirements.txt .
RUN pip3 install gunicorn
RUN pip3 install --requirement requirements.txt
RUN mkdir /api
RUN mkdir /config
COPY ./ ./api
WORKDIR /api
ENTRYPOINT ["/usr/local/bin/gunicorn","-c", "/config/gunicorn.conf", "main:app"]