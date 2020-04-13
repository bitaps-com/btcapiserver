FROM	postgres:12
RUN     apt-get -y update && apt-get -y install postgresql-plpython3-12
COPY    *.sql /docker-entrypoint-initdb.d/