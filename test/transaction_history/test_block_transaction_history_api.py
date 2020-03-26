import requests
from pybtc import *


def test_get_block_transaction_id_list(conf):
    if not conf["option_transaction_history"]:
        return
    return

    r = requests.get("https://gist.githubusercontent.com/4tochka/ec827a60214fc46eaa3aae71c6ba28bd/raw/"
                     "93e875692d2a1d21cc561824461f1cda92e25bf3/test%2520block")
    b = Block(r.text)
    r = requests.get(conf["base_url"] + "/rest/block/transaction/id/list/520667")
    assert r.status_code == 200
    d = r.json()["data"]
    for t in br:
        assert br[t]["txId"] in d[int(t)]


def test_get_block_transactions_extended(conf):
    if not conf["option_transaction_history"]:
        return
    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/block/500075?"
                     "block_statistic=1&transactions=1&mode=verbose")
    assert r.status_code == 200
    br = r.json()["data"]["transactions"]["list"]

    r = requests.get(conf["base_url"] + "/rest/block/transactions/500075")
    assert r.status_code == 200
    tms = time.time()
    d = r.json()["data"]

    for t in range(len(br)):
        assert br[t]["txId"] == d[int(t)]["txId"]
        if t:
            assert br[t]["hash"] == d[int(t)]["hash"]
            assert br[t]["size"] == d[int(t)]["size"]
            assert br[t]["vSize"] == d[int(t)]["vSize"]
            assert br[t]["bSize"] == d[int(t)]["bSize"]
            assert br[t]["weight"] == d[int(t)]["weight"]
            assert br[t]["segwit"] == d[int(t)]["segwit"]
        assert br[t]["version"] == d[int(t)]["version"]
        assert br[t]["lockTime"] == d[int(t)]["lockTime"]
        assert br[t]["data"] == d[int(t)]["data"]
        assert br[t]["coinbase"] == d[int(t)]["coinbase"]
        assert br[t]["amount"] == d[int(t)]["amount"]
        for i in br[t]["vIn"]:
            if not t:
                continue
            assert br[t]["vIn"][i]["txId"] == d[int(t)]["vIn"][str(i)]["txId"]
            assert br[t]["vIn"][i]["vOut"] == d[int(t)]["vIn"][str(i)]["vOut"]

            assert br[t]["vIn"][i]["type"] == d[int(t)]["vIn"][str(i)]["type"]
            assert br[t]["vIn"][i]["amount"] == d[int(t)]["vIn"][str(i)]["amount"]
            assert br[t]["vIn"][i]["scriptPubKey"] == d[int(t)]["vIn"][str(i)]["scriptPubKey"]
            assert br[t]["vIn"][i]["scriptPubKeyOpcodes"] == d[int(t)]["vIn"][str(i)]["scriptPubKeyOpcodes"]
            assert br[t]["vIn"][i]["scriptPubKeyAsm"] == d[int(t)]["vIn"][str(i)]["scriptPubKeyAsm"]
            if "address" in br[int(t)]["vIn"][i]:
                assert br[t]["vIn"][i]["address"] == d[int(t)]["vIn"][str(i)]["address"]



            assert br[t]["vIn"][i]["scriptSig"] == d[int(t)]["vIn"][str(i)]["scriptSig"]
            assert br[t]["vIn"][i]["sequence"] == d[int(t)]["vIn"][str(i)]["sequence"]
            if "txInWitness" in br[t]["vIn"][i]:
                assert br[t]["vIn"][i]["txInWitness"] == d[int(t)]["vIn"][str(i)]["txInWitness"]
            if not br[t]["coinbase"]:
                assert br[t]["vIn"][i]["scriptSigOpcodes"] == d[int(t)]["vIn"][str(i)]["scriptSigOpcodes"]
                assert br[t]["vIn"][i]["scriptSigAsm"] == d[int(t)]["vIn"][str(i)]["scriptSigAsm"]
        for i in br[t]["vOut"]:
            assert br[t]["vOut"][i]["value"] == d[int(t)]["vOut"][str(i)]["value"]
            assert br[t]["vOut"][i]["scriptPubKey"] == d[int(t)]["vOut"][str(i)]["scriptPubKey"]
            assert br[t]["vOut"][i]["nType"] == d[int(t)]["vOut"][str(i)]["nType"]
            assert br[t]["vOut"][i]["type"] == d[int(t)]["vOut"][str(i)]["type"]
            if "addressHash" in br[t]["vOut"][i]:
                assert br[t]["vOut"][i]["addressHash"] == d[int(t)]["vOut"][str(i)]["addressHash"]
                assert br[t]["vOut"][i]["address"] == d[int(t)]["vOut"][str(i)]["address"]
            assert br[t]["vOut"][i]["scriptPubKey"] == d[int(t)]["vOut"][str(i)]["scriptPubKey"]
            assert br[t]["vOut"][i]["scriptPubKeyAsm"] == d[int(t)]["vOut"][str(i)]["scriptPubKeyAsm"]
            assert br[t]["vOut"][i]["scriptPubKeyOpcodes"] == d[int(t)]["vOut"][str(i)]["scriptPubKeyOpcodes"]
            if br[t]["vOut"][i]["spent"] != d[int(t)]["vOut"][str(i)]["spent"]:
                time.sleep(1)
                if t["vOut"][i]["spent"]:
                    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/" +
                                     t["vOut"][i]["spent"][0]["txId"])
                    assert r.status_code == 200
                    st = r.json()["data"]
                    if not ((st["time"] - tms >= 0) or (st["time"] - br[t]["time"])):
                        assert t[int(t)]["vOut"][i]["spent"] == d[int(t)]["vOut"][str(i)]["spent"]
            # else:
            #     if br[t]["vOut"][i]["spent"]:
            #         print(br[t]["vOut"][i]["spent"])



