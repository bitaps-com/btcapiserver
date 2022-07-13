#!/bin/sh
parentdir="$(dirname $(dirname $(dirname "$(pwd)")))"
echo $parentdir
docker container stop btcapi-filters-positive-test
docker container rm btcapi-filters-positive-test
docker run --name btcapi-filters-positive-test \
           -v $parentdir/config:/config/ \
           -v $parentdir/data/socket/:/var/run/postgresql/ \
           -v $(pwd)/:/app/ \
           --net=host \
           -it btcapi-filters-positive-test