import requests
from pybtc import *
import base64


def test_get_block_data_pointer(conf):
    r = requests.get(conf["base_url"] + "/rest/block/data/500075")
    assert r.status_code == 200
    d = r.json()["data"]
    assert 'height' in d
    assert 'hash' in d
    assert 'header' in d
    assert 'adjustedTimestamp' in d

    assert d['height'] ==500075
    assert d['hash'] == "0000000000000000005d4fc2cc9cf5fe603afb5384ef427eed5b82be0fc52b5a"
    assert d['header'] == "AAAAIAedDe23r7+iuE2B/4sDbqeHnvmLCDogAAAAAAAAAAAATK+0e7+VtQpzjTj" \
                          "H9cTAdzrcK49+b7s1jnVyss0ji6eupjhaRZYAGLgirBs="
    assert d['adjustedTimestamp'] == 1513662126
    assert d["previousBlockHash"] == "000000000000000000203a088bf99e87a76e038bff814db8a2bfafb7ed0d9d07"
    assert d["nextBlockHash"] == "00000000000000000091584f0e9f7cd78b7100ab664032235510712e2d7f3773"
    assert d["merkleRoot"] == "a78b23cdb272758e35bb6f7e8f2bdc3a77c0c4f5c7388d730ab595bf7bb4af4c"
    assert d["medianBlockTime"] == 1513658418
    assert d["blockTime"] == 1513662126
    assert d["size"] == 1127960
    assert d["strippedSize"] == 955028
    assert d["strippedSize"] == 955028
    assert d["weight"] == 3993044
    assert d["bits"] == 402691653
    assert d["bitsHex"] == "18009645"
    assert d["nonce"] == 464265912
    assert d["nonceHex"] == "1bac22b8"
    assert d["version"] == 536870912
    assert d["versionHex"] == "20000000"
    assert d["difficulty"] == 1873105475221.611
    assert d["blockDifficulty"] == 3016460802811.54
    assert d["blockReward"] == 1250000000
    assert d["blockFeeReward"] == 370375213
    assert d["coinbase"] == "036ba10704aea6385a622f4254432e434f4d2ffabe6d6d04680379d4fc7cbd8e4848c78db2c" \
                                    "69c3916938031af141583b687417e74c8e50100000000000000653307954c89000000000000"
    assert d["transactionsCount"] == 3169
    assert d["miner"]["name"] == "BTC.COM"
    assert d["miner"]["link"] == "http://btc.com/"

