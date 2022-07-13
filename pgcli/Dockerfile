FROM ubuntu:18.04
MAINTAINER Aleksey Karpov <admin@bitaps.com>
RUN apt-get -y update && apt-get -y install libpq-dev build-essential
RUN apt-get -y install locales
RUN apt-get -y install python3
RUN apt-get -y install python3-pip

RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-
ENV LANGUAGE en_US:e
ENV LC_ALL en_US.UTF-8
RUN pip3 install pgcli
RUN pip3 install psycopg2-binary
RUN mkdir /var/run/postgresql
ENTRYPOINT ["pgcli"]