FROM	postgres:11
RUN     apt-get -y update && apt-get -y install postgresql-plpython3-11
COPY    *.sql /docker-entrypoint-initdb.d/