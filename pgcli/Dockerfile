FROM ubuntu:20.04
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update
RUN apt-get -y install python3
RUN apt-get -y install python3-pip
RUN apt-get -y install python-dev libpq-dev libevent-dev
RUN pip3 install pgcli

RUN mkdir /var/run/postgresql
ENTRYPOINT ["pgcli"]
