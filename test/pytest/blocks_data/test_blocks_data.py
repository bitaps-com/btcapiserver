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
                          "H9cTAdzrcK49+b7s1jnVyss0ji6eupjhaRZYAGLgirBv9YQw="
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

def test_get_data_last_n_blocks(conf):
    r = requests.get(conf["base_url"] + "/rest/blocks/data/last/16")
    assert r.status_code == 200
    rd = r.json()["data"]
    for d in rd:
        assert 'height' in d
        assert 'hash' in d
        assert 'header' in d
        assert 'adjustedTimestamp' in d
        assert d['hash'] == rh2s(double_sha256(base64.b64decode(d["header"])[:80], hex=False))
    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/blocks/last/16")
    rd_1 = r.json()["data"]
    for i in range(16):
        assert rd_1[i]["height"] == rd[i]["height"]
        assert rd_1[i]["hash"] == rd[i]["hash"]
        # assert rd_1[i]["header"] == rd[i]["header"]
        # assert rd_1[i]["adjustedTimestamp"] == rd[i]["adjustedTimestamp"]
        assert rd_1[i]["previousBlockHash"] == rd[i]["previousBlockHash"]
        assert rd_1[i]["nextBlockHash"] == rd[i]["nextBlockHash"]
        assert rd_1[i]["merkleRoot"] == rd[i]["merkleRoot"]
        # assert rd_1[i]["medianBlockTime"] == rd[i]["medianBlockTime"]

        assert rd_1[i]["blockTime"] == rd[i]["blockTime"]
        assert rd_1[i]["size"] == rd[i]["size"]
        assert rd_1[i]["strippedSize"] == rd[i]["strippedSize"]
        assert rd_1[i]["weight"] == rd[i]["weight"]
        assert rd_1[i]["bits"] == rd[i]["bits"]
        assert rd_1[i]["bitsHex"] == rd[i]["bitsHex"]
        assert rd_1[i]["nonce"] == rd[i]["nonce"]
        assert rd_1[i]["nonceHex"] == rd[i]["nonceHex"]
        assert rd_1[i]["version"] == rd[i]["version"]
        assert rd_1[i]["versionHex"] == rd[i]["versionHex"]
        assert rd_1[i]["difficulty"] == rd[i]["difficulty"]
        assert rd_1[i]["blockDifficulty"] == rd[i]["blockDifficulty"]
        assert rd_1[i]["blockReward"] == rd[i]["blockReward"]
        assert rd_1[i]["blockFeeReward"] == rd[i]["blockFeeReward"]
        # assert rd_1[i]["coinbase"] == rd[i]["coinbase"]
        assert rd_1[i]["transactionsCount"] == rd[i]["transactionsCount"]
        # assert rd_1[i]["miner"] == rd[i]["miner"]


def test_get_data_today_blocks(conf):
    r = requests.get(conf["base_url"] + "/rest/blocks/data/today")
    assert r.status_code == 200
    rd = r.json()["data"]
    for d in rd:
        assert 'height' in d
        assert 'hash' in d
        assert 'header' in d
        assert 'adjustedTimestamp' in d
        assert d['hash'] == rh2s(double_sha256(base64.b64decode(d["header"])[:80], hex=False))
    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/blocks/today")
    rd_1 = r.json()["data"]
    for i in range(len(rd_1)):
        assert rd_1[i]["height"] == rd[i]["height"]
        assert rd_1[i]["hash"] == rd[i]["hash"]
        # assert rd_1[i]["header"] == rd[i]["header"]
        # assert rd_1[i]["adjustedTimestamp"] == rd[i]["adjustedTimestamp"]
        assert rd_1[i]["previousBlockHash"] == rd[i]["previousBlockHash"]
        assert rd_1[i]["nextBlockHash"] == rd[i]["nextBlockHash"]
        assert rd_1[i]["merkleRoot"] == rd[i]["merkleRoot"]
        # assert rd_1[i]["medianBlockTime"] == rd[i]["medianBlockTime"]

        assert rd_1[i]["blockTime"] == rd[i]["blockTime"]
        assert rd_1[i]["size"] == rd[i]["size"]
        assert rd_1[i]["strippedSize"] == rd[i]["strippedSize"]
        assert rd_1[i]["weight"] == rd[i]["weight"]
        assert rd_1[i]["bits"] == rd[i]["bits"]
        assert rd_1[i]["bitsHex"] == rd[i]["bitsHex"]
        assert rd_1[i]["nonce"] == rd[i]["nonce"]
        assert rd_1[i]["nonceHex"] == rd[i]["nonceHex"]
        assert rd_1[i]["version"] == rd[i]["version"]
        assert rd_1[i]["versionHex"] == rd[i]["versionHex"]
        assert rd_1[i]["difficulty"] == rd[i]["difficulty"]
        assert rd_1[i]["blockDifficulty"] == rd[i]["blockDifficulty"]
        assert rd_1[i]["blockReward"] == rd[i]["blockReward"]
        assert rd_1[i]["blockFeeReward"] == rd[i]["blockFeeReward"]
        # assert rd_1[i]["coinbase"] == rd[i]["coinbase"]
        assert rd_1[i]["transactionsCount"] == rd[i]["transactionsCount"]
        # assert rd_1[i]["miner"] == rd[i]["miner"]


def test_get_data_date_blocks(conf):
    r = requests.get(conf["base_url"] + "/rest/blocks/data/date/20190101")
    assert r.status_code == 200
    rd = r.json()["data"]
    for d in rd:
        assert 'height' in d
        assert 'hash' in d
        assert 'header' in d
        assert 'adjustedTimestamp' in d
        assert d['hash'] == rh2s(double_sha256(base64.b64decode(d["header"])[:80], hex=False))
    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/blocks/date/20190101")
    rd_1 = r.json()["data"]
    assert len(rd_1) == 149
    for i in range(len(rd_1)):
        assert rd_1[i]["height"] == rd[i]["height"]
        assert rd_1[i]["hash"] == rd[i]["hash"]
        # assert rd_1[i]["header"] == rd[i]["header"]
        # assert rd_1[i]["adjustedTimestamp"] == rd[i]["adjustedTimestamp"]
        assert rd_1[i]["previousBlockHash"] == rd[i]["previousBlockHash"]
        assert rd_1[i]["nextBlockHash"] == rd[i]["nextBlockHash"]
        assert rd_1[i]["merkleRoot"] == rd[i]["merkleRoot"]
        # assert rd_1[i]["medianBlockTime"] == rd[i]["medianBlockTime"]

        assert rd_1[i]["blockTime"] == rd[i]["blockTime"]
        assert rd_1[i]["size"] == rd[i]["size"]
        assert rd_1[i]["strippedSize"] == rd[i]["strippedSize"]
        assert rd_1[i]["weight"] == rd[i]["weight"]
        assert rd_1[i]["bits"] == rd[i]["bits"]
        assert rd_1[i]["bitsHex"] == rd[i]["bitsHex"]
        assert rd_1[i]["nonce"] == rd[i]["nonce"]
        assert rd_1[i]["nonceHex"] == rd[i]["nonceHex"]
        assert rd_1[i]["version"] == rd[i]["version"]
        assert rd_1[i]["versionHex"] == rd[i]["versionHex"]
        assert round(rd_1[i]["difficulty"]) == round(rd[i]["difficulty"])
        assert rd_1[i]["blockDifficulty"] == rd[i]["blockDifficulty"]
        assert rd_1[i]["blockReward"] == rd[i]["blockReward"]
        assert rd_1[i]["blockFeeReward"] == rd[i]["blockFeeReward"]
        # assert rd_1[i]["coinbase"] == rd[i]["coinbase"]
        assert rd_1[i]["transactionsCount"] == rd[i]["transactionsCount"]
        # assert rd_1[i]["miner"] == rd[i]["miner"]

def test_get_data_last_n_hours_blocks(conf):
    r = requests.get(conf["base_url"] + "/rest/blocks/data/last/10/hours")
    assert r.status_code == 200
    rd = r.json()["data"]
    for d in rd:
        assert 'height' in d
        assert 'hash' in d
        assert 'header' in d
        assert 'adjustedTimestamp' in d
        assert d['hash'] == rh2s(double_sha256(base64.b64decode(d["header"])[:80], hex=False))
    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/blocks/last/16/hours")
    rd_1 = r.json()["data"]
    for i in range(16):
        assert rd_1[i]["height"] == rd[i]["height"]
        assert rd_1[i]["hash"] == rd[i]["hash"]
        # assert rd_1[i]["header"] == rd[i]["header"]
        # assert rd_1[i]["adjustedTimestamp"] == rd[i]["adjustedTimestamp"]
        assert rd_1[i]["previousBlockHash"] == rd[i]["previousBlockHash"]
        assert rd_1[i]["nextBlockHash"] == rd[i]["nextBlockHash"]
        assert rd_1[i]["merkleRoot"] == rd[i]["merkleRoot"]
        # assert rd_1[i]["medianBlockTime"] == rd[i]["medianBlockTime"]

        assert rd_1[i]["blockTime"] == rd[i]["blockTime"]
        assert rd_1[i]["size"] == rd[i]["size"]
        assert rd_1[i]["strippedSize"] == rd[i]["strippedSize"]
        assert rd_1[i]["weight"] == rd[i]["weight"]
        assert rd_1[i]["bits"] == rd[i]["bits"]
        assert rd_1[i]["bitsHex"] == rd[i]["bitsHex"]
        assert rd_1[i]["nonce"] == rd[i]["nonce"]
        assert rd_1[i]["nonceHex"] == rd[i]["nonceHex"]
        assert rd_1[i]["version"] == rd[i]["version"]
        assert rd_1[i]["versionHex"] == rd[i]["versionHex"]
        assert rd_1[i]["difficulty"] == rd[i]["difficulty"]
        assert rd_1[i]["blockDifficulty"] == rd[i]["blockDifficulty"]
        assert rd_1[i]["blockReward"] == rd[i]["blockReward"]
        assert rd_1[i]["blockFeeReward"] == rd[i]["blockFeeReward"]
        # assert rd_1[i]["coinbase"] == rd[i]["coinbase"]
        assert rd_1[i]["transactionsCount"] == rd[i]["transactionsCount"]
        # assert rd_1[i]["miner"] == rd[i]["miner"]



