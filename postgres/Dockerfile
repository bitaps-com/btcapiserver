FROM	postgres:12
RUN     apt-get -y update
RUN     apt-get -y install postgresql-plpython3-12
ENV     POSTGRES_HOST_AUTH_METHOD=trust
COPY    *.sql /docker-entrypoint-initdb.d/