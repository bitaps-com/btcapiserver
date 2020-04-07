import requests
from pybtc import *


def test_get_block_transaction_id_list(conf):
    if not conf["option_transaction"]:
        return

    r = requests.get("https://gist.githubusercontent.com/4tochka/ec827a60214fc46eaa3aae71c6ba28bd/raw/"
                     "93e875692d2a1d21cc561824461f1cda92e25bf3/test%2520block")
    b = Block(r.text)
    r = requests.get(conf["base_url"] + "/rest/block/transaction/id/list/520667")
    assert r.status_code == 200
    d = r.json()["data"]
    for t in b["tx"]:
        assert b["tx"][t]["txId"] in d[int(t)]


def test_get_block_transactions(conf):
    if not conf["option_transaction"]:
        return

    r = requests.get("https://gist.githubusercontent.com/4tochka/ec827a60214fc46eaa3aae71c6ba28bd/raw/"
                     "93e875692d2a1d21cc561824461f1cda92e25bf3/test%2520block")
    b = Block(r.text, testnet=conf["testnet"])
    r = requests.get(conf["base_url"] + "/rest/block/transactions/520667?mode=verbose")
    assert r.status_code == 200
    d = r.json()["data"]

    for t in b["tx"]:
        if t not in d:
            continue
        assert b["tx"][t]["txId"] == d[int(t)]["txId"]
        assert b["tx"][t]["hash"] == d[int(t)]["hash"]
        assert b["tx"][t]["version"] == d[int(t)]["version"]
        assert b["tx"][t]["size"] == d[int(t)]["size"]
        assert b["tx"][t]["vSize"] == d[int(t)]["vSize"]
        assert b["tx"][t]["bSize"] == d[int(t)]["bSize"]
        assert b["tx"][t]["lockTime"] == d[int(t)]["lockTime"]
        assert b["tx"][t]["weight"] == d[int(t)]["weight"]
        assert b["tx"][t]["data"] == d[int(t)]["data"]
        assert b["tx"][t]["coinbase"] == d[int(t)]["coinbase"]
        assert b["tx"][t]["segwit"] == d[int(t)]["segwit"]
        assert b["tx"][t]["amount"] == d[int(t)]["amount"]
        for i in b["tx"][t]["vIn"]:
            assert b["tx"][t]["vIn"][i]["txId"] == d[int(t)]["vIn"][str(i)]["txId"]
            assert b["tx"][t]["vIn"][i]["vOut"] == d[int(t)]["vIn"][str(i)]["vOut"]
            assert b["tx"][t]["vIn"][i]["scriptSig"] == d[int(t)]["vIn"][str(i)]["scriptSig"]
            assert b["tx"][t]["vIn"][i]["sequence"] == d[int(t)]["vIn"][str(i)]["sequence"]
            if "txInWitness" in b["tx"][t]["vIn"][i]:
                assert b["tx"][t]["vIn"][i]["txInWitness"] == d[int(t)]["vIn"][str(i)]["txInWitness"]
            if not b["tx"][t]["coinbase"]:
                assert b["tx"][t]["vIn"][i]["scriptSigOpcodes"] == d[int(t)]["vIn"][str(i)]["scriptSigOpcodes"]
                assert b["tx"][t]["vIn"][i]["scriptSigAsm"] == d[int(t)]["vIn"][str(i)]["scriptSigAsm"]
        for i in b["tx"][t]["vOut"]:
            assert b["tx"][t]["vOut"][i]["value"] == d[int(t)]["vOut"][str(i)]["value"]
            assert b["tx"][t]["vOut"][i]["scriptPubKey"] == d[int(t)]["vOut"][str(i)]["scriptPubKey"]
            assert b["tx"][t]["vOut"][i]["nType"] == d[int(t)]["vOut"][str(i)]["nType"]
            assert b["tx"][t]["vOut"][i]["type"] == d[int(t)]["vOut"][str(i)]["type"]
            if "addressHash" in b["tx"][t]["vOut"][i]:
                assert b["tx"][t]["vOut"][i]["addressHash"] == d[int(t)]["vOut"][str(i)]["addressHash"]
                assert b["tx"][t]["vOut"][i]["address"] == d[int(t)]["vOut"][str(i)]["address"]
            assert b["tx"][t]["vOut"][i]["scriptPubKey"] == d[int(t)]["vOut"][str(i)]["scriptPubKey"]
            assert b["tx"][t]["vOut"][i]["scriptPubKeyAsm"] == d[int(t)]["vOut"][str(i)]["scriptPubKeyAsm"]
            assert b["tx"][t]["vOut"][i]["scriptPubKeyOpcodes"] == d[int(t)]["vOut"][str(i)]["scriptPubKeyOpcodes"]




