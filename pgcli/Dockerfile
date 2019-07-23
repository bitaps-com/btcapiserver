FROM ubuntu:18.04
MAINTAINER Aleksey Karpov <admin@bitaps.com>
RUN apt-get -y update && apt-get -y install libpq-dev build-essential
RUN apt-get -y install python-pip
RUN pip install pgcli==2.0.2
RUN pip install psycopg2-binary
RUN mkdir /var/run/postgresql
ENTRYPOINT ["pgcli"]
