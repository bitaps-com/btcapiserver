docker container stop btcapi-test
docker container rm btcapi-test
docker build -t btcapi-test .
docker run --rm \
           --name btcapi-test \
           -v /home/ubuntu/btcapiserver/config:/config/ \
           -v /home/ubuntu/btcapiserver/test:/test \
           --net=host \
           -it btcapi-test -v -s