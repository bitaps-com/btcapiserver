FROM	postgres:14.4-alpine
RUN     apk update
RUN     apk add postgresql14-plpython3 postgresql14-jit
ENV     POSTGRES_HOST_AUTH_METHOD=trust
COPY    *.sql /docker-entrypoint-initdb.d/
