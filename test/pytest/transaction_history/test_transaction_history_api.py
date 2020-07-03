import requests
from pybtc import *



"""
address transactions records

stxo

"""

import requests
from pybtc import *


def test_get_transaction_extended(conf):
    if not conf["option_transaction_history"]:
        return

    r = requests.get(conf["base_url"] +
                     "/rest/transaction/f44e31829bd6424c932bee7cc0aa0e4730403b8be3e956ed7d5a67e001cfb804")
    assert r.status_code == 200
    d_1 = r.json()["data"]


    r = requests.get(conf["base_url"] + "/rest/transaction/621860:1373")
    assert r.status_code == 200
    d_2 = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/"
                     "f44e31829bd6424c932bee7cc0aa0e4730403b8be3e956ed7d5a67e001cfb804")
    assert r.status_code == 200
    t = r.json()["data"]


    for d in [d_1, d_2]:
        assert t["txId"] == d["txId"]
        assert t["hash"] == d["hash"]
        assert t["version"] == d["version"]
        assert t["size"] == d["size"]
        assert t["vSize"] == d["vSize"]
        assert t["bSize"] == d["bSize"]
        assert t["lockTime"] == d["lockTime"]
        assert t["weight"] == d["weight"]
        assert t["data"] == d["data"]
        assert t["coinbase"] == d["coinbase"]
        assert t["segwit"] == d["segwit"]
        assert t["amount"] == d["amount"]
        for i in t["vIn"]:
            assert t["vIn"][i]["txId"] == d["vIn"][str(i)]["txId"]
            assert t["vIn"][i]["vOut"] == d["vIn"][str(i)]["vOut"]
            assert t["vIn"][i]["type"] == d["vIn"][str(i)]["type"]
            assert t["vIn"][i]["amount"] == d["vIn"][str(i)]["amount"]
            assert t["vIn"][i]["scriptPubKey"] == d["vIn"][str(i)]["scriptPubKey"]
            assert t["vIn"][i]["scriptPubKeyOpcodes"] == d["vIn"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vIn"][i]["scriptPubKeyAsm"] == d["vIn"][str(i)]["scriptPubKeyAsm"]
            assert t["vIn"][i]["confirmations"] == d["vIn"][str(i)]["confirmations"]
            assert t["vIn"][i]["blockHeight"] == d["vIn"][str(i)]["blockHeight"]
            if "address" in t["vIn"][i]:
                assert t["vIn"][i]["address"] == d["vIn"][str(i)]["address"]
            assert t["vIn"][i]["scriptSig"] == d["vIn"][str(i)]["scriptSig"]
            assert t["vIn"][i]["sequence"] == d["vIn"][str(i)]["sequence"]
            if "txInWitness" in t["vIn"][i]:
                assert t["vIn"][i]["txInWitness"] == d["vIn"][str(i)]["txInWitness"]
            if not t["coinbase"]:
                assert t["vIn"][i]["scriptSigOpcodes"] == d["vIn"][str(i)]["scriptSigOpcodes"]
                assert t["vIn"][i]["scriptSigAsm"] == d["vIn"][str(i)]["scriptSigAsm"]
        for i in t["vOut"]:
            assert t["vOut"][i]["value"] == d["vOut"][str(i)]["value"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["nType"] == d["vOut"][str(i)]["nType"]
            assert t["vOut"][i]["type"] == d["vOut"][str(i)]["type"]
            if "addressHash" in t["vOut"][i]:
                assert t["vOut"][i]["addressHash"] == d["vOut"][str(i)]["addressHash"]
                assert t["vOut"][i]["address"] == d["vOut"][str(i)]["address"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["scriptPubKeyAsm"] == d["vOut"][str(i)]["scriptPubKeyAsm"]
            assert t["vOut"][i]["scriptPubKeyOpcodes"] == d["vOut"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vOut"][i]["spent"] == d["vOut"][str(i)]["spent"]
    r = requests.get(conf["base_url"] + "/rest/transaction/621860:55555")
    assert r.status_code == 404
    r = requests.get(conf["base_url"] + "/rest/transaction/6218605555")
    assert r.status_code == 400


def test_get_coinbase_transaction_extended(conf):
    if not conf["option_transaction_history"]:
        return

    r = requests.get(conf["base_url"] +
                     "/rest/transaction/7eec7bd452c07a8dbdb4bdb6f900f827a2e9814852b2546878b93970bbcb3dd8")
    assert r.status_code == 200
    d_1 = r.json()["data"]


    r = requests.get(conf["base_url"] + "/rest/transaction/622797:0")
    assert r.status_code == 200
    d_2 = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/"
                     "7eec7bd452c07a8dbdb4bdb6f900f827a2e9814852b2546878b93970bbcb3dd8")
    assert r.status_code == 200
    t = r.json()["data"]


    for d in [d_1, d_2]:
        assert t["txId"] == d["txId"]
        # assert t["hash"] == d["hash"]
        assert t["version"] == d["version"]
        # assert t["size"] == d["size"]
        # assert t["vSize"] == d["vSize"]
        # assert t["bSize"] == d["bSize"]
        assert t["lockTime"] == d["lockTime"]
        # assert t["weight"] == d["weight"]
        assert t["data"] == d["data"]
        assert t["coinbase"] == d["coinbase"]
        # assert t["segwit"] == d["segwit"]
        assert t["amount"] == d["amount"]
        for i in t["vIn"]:
            assert t["vIn"][i]["txId"] == d["vIn"][str(i)]["txId"]
            assert t["vIn"][i]["vOut"] == d["vIn"][str(i)]["vOut"]
            assert t["vIn"][i]["scriptSig"] == d["vIn"][str(i)]["scriptSig"]
            assert t["vIn"][i]["sequence"] == d["vIn"][str(i)]["sequence"]
            if "txInWitness" in t["vIn"][i]:
                assert t["vIn"][i]["txInWitness"] == d["vIn"][str(i)]["txInWitness"]
            if not t["coinbase"]:
                assert t["vIn"][i]["scriptSigOpcodes"] == d["vIn"][str(i)]["scriptSigOpcodes"]
                assert t["vIn"][i]["scriptSigAsm"] == d["vIn"][str(i)]["scriptSigAsm"]
        for i in t["vOut"]:
            assert t["vOut"][i]["value"] == d["vOut"][str(i)]["value"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["nType"] == d["vOut"][str(i)]["nType"]
            assert t["vOut"][i]["type"] == d["vOut"][str(i)]["type"]
            if "addressHash" in t["vOut"][i]:
                assert t["vOut"][i]["addressHash"] == d["vOut"][str(i)]["addressHash"]
                assert t["vOut"][i]["address"] == d["vOut"][str(i)]["address"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["scriptPubKeyAsm"] == d["vOut"][str(i)]["scriptPubKeyAsm"]
            assert t["vOut"][i]["scriptPubKeyOpcodes"] == d["vOut"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vOut"][i]["spent"] == d["vOut"][str(i)]["spent"]
    r = requests.get(conf["base_url"] + "/rest/transaction/621860:55555")
    assert r.status_code == 404
    r = requests.get(conf["base_url"] + "/rest/transaction/6218605555")
    assert r.status_code == 400



def test_get_transaction_extended_list(conf):
    if not conf["option_transaction_history"]:
        return
    l = ["f8dc0b9ec82b62173b0e1689d5158b06ccb4494dd2380116ad526819e57ec86c",
         "543ef9762afd24805e350b31dec91ee861dc35b4a480496a830d3594ecdee02a",
         "8041b77af027eb296364f96772612d94ce71b3dcbff3314e4f947121e52d6661",
         "7eec7bd452c07a8dbdb4bdb6f900f827a2e9814852b2546878b93970bbcb3dd8"]
    for h in l:
        r = requests.get(conf["base_url"] + "/rest/transaction/" + h)
        assert r.status_code == 200
        d = r.json()["data"]


        r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/" + h )
        assert r.status_code == 200
        t = r.json()["data"]

        assert t["txId"] == d["txId"]
        if not t["coinbase"]:
            assert t["hash"] == d["hash"]
            assert t["size"] == d["size"]
            assert t["vSize"] == d["vSize"]
            assert t["bSize"] == d["bSize"]
            assert t["weight"] == d["weight"]
            assert t["segwit"] == d["segwit"]
        assert t["version"] == d["version"]

        assert t["lockTime"] == d["lockTime"]

        assert t["data"] == d["data"]
        assert t["coinbase"] == d["coinbase"]

        assert t["amount"] == d["amount"]
        for i in t["vIn"]:
            if t["coinbase"]:
                continue
            assert t["vIn"][i]["txId"] == d["vIn"][str(i)]["txId"]
            assert t["vIn"][i]["vOut"] == d["vIn"][str(i)]["vOut"]
            assert t["vIn"][i]["type"] == d["vIn"][str(i)]["type"]
            assert t["vIn"][i]["amount"] == d["vIn"][str(i)]["amount"]
            assert t["vIn"][i]["scriptPubKey"] == d["vIn"][str(i)]["scriptPubKey"]
            assert t["vIn"][i]["scriptPubKeyOpcodes"] == d["vIn"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vIn"][i]["scriptPubKeyAsm"] == d["vIn"][str(i)]["scriptPubKeyAsm"]
            if t["vIn"][i]["blockHeight"] != -1:
                assert t["vIn"][i]["confirmations"] == d["vIn"][str(i)]["confirmations"]
                assert t["vIn"][i]["blockHeight"] == d["vIn"][str(i)]["blockHeight"]
            if "address" in t["vIn"][i]:
                assert t["vIn"][i]["address"] == d["vIn"][str(i)]["address"]
            assert t["vIn"][i]["scriptSig"] == d["vIn"][str(i)]["scriptSig"]
            assert t["vIn"][i]["sequence"] == d["vIn"][str(i)]["sequence"]
            if "txInWitness" in t["vIn"][i]:
                assert t["vIn"][i]["txInWitness"] == d["vIn"][str(i)]["txInWitness"]
            if not t["coinbase"]:
                assert t["vIn"][i]["scriptSigOpcodes"] == d["vIn"][str(i)]["scriptSigOpcodes"]
                assert t["vIn"][i]["scriptSigAsm"] == d["vIn"][str(i)]["scriptSigAsm"]
        for i in t["vOut"]:
            assert t["vOut"][i]["value"] == d["vOut"][str(i)]["value"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["nType"] == d["vOut"][str(i)]["nType"]
            assert t["vOut"][i]["type"] == d["vOut"][str(i)]["type"]
            if "addressHash" in t["vOut"][i]:
                assert t["vOut"][i]["addressHash"] == d["vOut"][str(i)]["addressHash"]
                assert t["vOut"][i]["address"] == d["vOut"][str(i)]["address"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["scriptPubKeyAsm"] == d["vOut"][str(i)]["scriptPubKeyAsm"]
            assert t["vOut"][i]["scriptPubKeyOpcodes"] == d["vOut"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vOut"][i]["spent"] == d["vOut"][str(i)]["spent"]





def test_get_unconfirmed_transaction_extended(conf):
    if not conf["option_transaction_history"]:
        return

    r = requests.get("https://api.bitaps.com/btc/v1/mempool/transactions")
    assert r.status_code == 200
    t = r.json()["data"]["transactions"]
    tx_list = [i["txId"] for i in t]
    counter = 0
    for tx in tx_list:
        time.sleep(0.4)

        r = requests.get(conf["base_url"] +
                         "/rest/transaction/" + tx)
        if r.status_code == 404:
            continue
        counter += 1
        assert r.status_code == 200
        d = r.json()["data"]

        r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/" + tx)
        assert r.status_code == 200
        t = r.json()["data"]

        assert t["txId"] == d["txId"]
        assert t["hash"] == d["hash"]
        assert t["version"] == d["version"]
        assert t["size"] == d["size"]
        assert t["vSize"] == d["vSize"]
        assert t["bSize"] == d["bSize"]
        assert t["lockTime"] == d["lockTime"]
        assert t["weight"] == d["weight"]
        assert t["data"] == d["data"]
        assert t["coinbase"] == d["coinbase"]
        assert t["segwit"] == d["segwit"]
        assert t["amount"] == d["amount"]
        for i in t["vIn"]:
            assert t["vIn"][i]["txId"] == d["vIn"][str(i)]["txId"]
            assert t["vIn"][i]["vOut"] == d["vIn"][str(i)]["vOut"]
            assert t["vIn"][i]["type"] == d["vIn"][str(i)]["type"]
            assert t["vIn"][i]["amount"] == d["vIn"][str(i)]["amount"]
            assert t["vIn"][i]["scriptPubKey"] == d["vIn"][str(i)]["scriptPubKey"]
            assert t["vIn"][i]["scriptPubKeyOpcodes"] == d["vIn"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vIn"][i]["scriptPubKeyAsm"] == d["vIn"][str(i)]["scriptPubKeyAsm"]
            if t["vIn"][i]["blockHeight"] != -1:
                assert t["vIn"][i]["blockHeight"] == d["vIn"][str(i)]["blockHeight"]
                assert t["vIn"][i]["confirmations"] == d["vIn"][str(i)]["confirmations"]
            if "address" in t["vIn"][i]:
                assert t["vIn"][i]["address"] == d["vIn"][str(i)]["address"]
            assert t["vIn"][i]["scriptSig"] == d["vIn"][str(i)]["scriptSig"]
            assert t["vIn"][i]["sequence"] == d["vIn"][str(i)]["sequence"]
            if "txInWitness" in t["vIn"][i]:
                assert t["vIn"][i]["txInWitness"] == d["vIn"][str(i)]["txInWitness"]
            if not t["coinbase"]:
                assert t["vIn"][i]["scriptSigOpcodes"] == d["vIn"][str(i)]["scriptSigOpcodes"]
                assert t["vIn"][i]["scriptSigAsm"] == d["vIn"][str(i)]["scriptSigAsm"]
        for i in t["vOut"]:
            assert t["vOut"][i]["value"] == d["vOut"][str(i)]["value"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["nType"] == d["vOut"][str(i)]["nType"]
            assert t["vOut"][i]["type"] == d["vOut"][str(i)]["type"]
            if "addressHash" in t["vOut"][i]:
                assert t["vOut"][i]["addressHash"] == d["vOut"][str(i)]["addressHash"]
                assert t["vOut"][i]["address"] == d["vOut"][str(i)]["address"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["scriptPubKeyAsm"] == d["vOut"][str(i)]["scriptPubKeyAsm"]
            assert t["vOut"][i]["scriptPubKeyOpcodes"] == d["vOut"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vOut"][i]["spent"] == d["vOut"][str(i)]["spent"]
    assert counter > 0

def test_get_transaction_by_pointer_list_extended(conf):
    if not conf["option_transaction_history"]:
        return

    tl = ["9414ad2bb1a038076bc859ef4cb048c9e859ca155307008bcad94d9e78abba24",
          "b6056e9210beb7a3f019e7758fa87a9d07121507305efea3b6f638dd5390de3e",
          "f44e31829bd6424c932bee7cc0aa0e4730403b8be3e956ed7d5a67e001cfb804"]

    r = requests.post(conf["base_url"] + "/rest/transactions/by/pointer/list",
                      data= json.dumps(tl).encode())
    assert r.status_code == 200
    dl = r.json()["data"]

    for tx in tl:
        r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/"+tx)
        assert r.status_code == 200
        t = r.json()["data"]

        for d in [dl[t["txId"]]]:
            assert t["txId"] == d["txId"]
            assert t["hash"] == d["hash"]
            assert t["version"] == d["version"]
            assert t["size"] == d["size"]
            assert t["vSize"] == d["vSize"]
            assert t["bSize"] == d["bSize"]
            assert t["lockTime"] == d["lockTime"]
            assert t["weight"] == d["weight"]
            assert t["data"] == d["data"]
            assert t["coinbase"] == d["coinbase"]
            assert t["segwit"] == d["segwit"]
            assert t["amount"] == d["amount"]
            for i in t["vIn"]:
                assert t["vIn"][i]["txId"] == d["vIn"][str(i)]["txId"]
                assert t["vIn"][i]["vOut"] == d["vIn"][str(i)]["vOut"]
                assert t["vIn"][i]["type"] == d["vIn"][str(i)]["type"]
                assert t["vIn"][i]["amount"] == d["vIn"][str(i)]["amount"]
                assert t["vIn"][i]["scriptPubKey"] == d["vIn"][str(i)]["scriptPubKey"]
                assert t["vIn"][i]["scriptPubKeyOpcodes"] == d["vIn"][str(i)]["scriptPubKeyOpcodes"]
                assert t["vIn"][i]["scriptPubKeyAsm"] == d["vIn"][str(i)]["scriptPubKeyAsm"]
                assert t["vIn"][i]["confirmations"] == d["vIn"][str(i)]["confirmations"]
                assert t["vIn"][i]["blockHeight"] == d["vIn"][str(i)]["blockHeight"]
                if "address" in t["vIn"][i]:
                    assert t["vIn"][i]["address"] == d["vIn"][str(i)]["address"]
                assert t["vIn"][i]["scriptSig"] == d["vIn"][str(i)]["scriptSig"]
                assert t["vIn"][i]["sequence"] == d["vIn"][str(i)]["sequence"]
                if "txInWitness" in t["vIn"][i]:
                    assert t["vIn"][i]["txInWitness"] == d["vIn"][str(i)]["txInWitness"]
                if not t["coinbase"]:
                    assert t["vIn"][i]["scriptSigOpcodes"] == d["vIn"][str(i)]["scriptSigOpcodes"]
                    assert t["vIn"][i]["scriptSigAsm"] == d["vIn"][str(i)]["scriptSigAsm"]
            for i in t["vOut"]:
                assert t["vOut"][i]["value"] == d["vOut"][str(i)]["value"]
                assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
                assert t["vOut"][i]["nType"] == d["vOut"][str(i)]["nType"]
                assert t["vOut"][i]["type"] == d["vOut"][str(i)]["type"]
                if "addressHash" in t["vOut"][i]:
                    assert t["vOut"][i]["addressHash"] == d["vOut"][str(i)]["addressHash"]
                    assert t["vOut"][i]["address"] == d["vOut"][str(i)]["address"]
                assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
                assert t["vOut"][i]["scriptPubKeyAsm"] == d["vOut"][str(i)]["scriptPubKeyAsm"]
                assert t["vOut"][i]["spent"] == d["vOut"][str(i)]["spent"]

def test_get_unconfirmed_transaction_by_pointer_list_extended(conf):
    if not conf["option_transaction_history"]:
        return
    tms = int(time.time())
    r = requests.get("https://api.bitaps.com/btc/v1/mempool/transactions")
    assert r.status_code == 200
    t = r.json()["data"]["transactions"]
    tx_list = [i["txId"] for i in t]


    q = time.time()
    r = requests.post(conf["base_url"] + "/rest/transactions/by/pointer/list",
                      data= json.dumps(tx_list).encode())
    assert r.status_code == 200
    dl = r.json()["data"]
    assert time.time() - q < 1


    counter = 0
    for tx in tx_list:
        time.sleep(0.35)
        d = dl[tx]

        r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/" + tx)
        assert r.status_code == 200
        t = r.json()["data"]
        if d is None:
            continue
        counter += 1
        assert t["txId"] == d["txId"]
        assert t["hash"] == d["hash"]
        assert t["version"] == d["version"]
        assert t["size"] == d["size"]
        assert t["vSize"] == d["vSize"]
        assert t["bSize"] == d["bSize"]
        assert t["lockTime"] == d["lockTime"]
        assert t["weight"] == d["weight"]
        assert t["data"] == d["data"]
        assert t["coinbase"] == d["coinbase"]
        assert t["segwit"] == d["segwit"]
        assert t["amount"] == d["amount"]
        for i in t["vIn"]:
            assert t["vIn"][i]["txId"] == d["vIn"][str(i)]["txId"]
            assert t["vIn"][i]["vOut"] == d["vIn"][str(i)]["vOut"]
            assert t["vIn"][i]["type"] == d["vIn"][str(i)]["type"]
            assert t["vIn"][i]["amount"] == d["vIn"][str(i)]["amount"]
            assert t["vIn"][i]["scriptPubKey"] == d["vIn"][str(i)]["scriptPubKey"]
            assert t["vIn"][i]["scriptPubKeyOpcodes"] == d["vIn"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vIn"][i]["scriptPubKeyAsm"] == d["vIn"][str(i)]["scriptPubKeyAsm"]
            if t["vIn"][i]["blockHeight"] != -1:
                assert t["vIn"][i]["blockHeight"] == d["vIn"][str(i)]["blockHeight"]
                assert t["vIn"][i]["confirmations"] == d["vIn"][str(i)]["confirmations"]
            if "address" in t["vIn"][i]:
                assert t["vIn"][i]["address"] == d["vIn"][str(i)]["address"]
            assert t["vIn"][i]["scriptSig"] == d["vIn"][str(i)]["scriptSig"]
            assert t["vIn"][i]["sequence"] == d["vIn"][str(i)]["sequence"]
            if "txInWitness" in t["vIn"][i]:
                assert t["vIn"][i]["txInWitness"] == d["vIn"][str(i)]["txInWitness"]
            if not t["coinbase"]:
                assert t["vIn"][i]["scriptSigOpcodes"] == d["vIn"][str(i)]["scriptSigOpcodes"]
                assert t["vIn"][i]["scriptSigAsm"] == d["vIn"][str(i)]["scriptSigAsm"]
        for i in t["vOut"]:
            assert t["vOut"][i]["value"] == d["vOut"][str(i)]["value"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["nType"] == d["vOut"][str(i)]["nType"]
            assert t["vOut"][i]["type"] == d["vOut"][str(i)]["type"]
            if "addressHash" in t["vOut"][i]:
                assert t["vOut"][i]["addressHash"] == d["vOut"][str(i)]["addressHash"]
                assert t["vOut"][i]["address"] == d["vOut"][str(i)]["address"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["scriptPubKeyAsm"] == d["vOut"][str(i)]["scriptPubKeyAsm"]
            assert t["vOut"][i]["scriptPubKeyOpcodes"] == d["vOut"][str(i)]["scriptPubKeyOpcodes"]

            if t["vOut"][i]["spent"] != d["vOut"][str(i)]["spent"]:
                r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/" +
                                 t["vOut"][i]["spent"][0]["txId"])
                assert r.status_code == 200
                st = r.json()["data"]
                if not ((st["time"] - tms >= 0) or (st["time"] - t["time"])):
                    assert t["vOut"][i]["spent"] == d["vOut"][str(i)]["spent"]

    assert counter > 0

def test_get_confirmed_transaction_by_pointer_list_extended(conf):
    if not conf["option_transaction_history"]:
        return

    r = requests.get("https://api.bitaps.com/btc/v1/mempool/transactions")
    assert r.status_code == 200
    t = r.json()["data"]["transactions"]
    tx_list = [i["txId"] for i in t][:30]


    q = time.time()
    r = requests.post(conf["base_url"] + "/rest/transactions/by/pointer/list",
                      data= json.dumps(tx_list).encode())
    assert r.status_code == 200
    dl = r.json()["data"]
    assert time.time() - q < 2
    tx_conf = []

    counter = 0
    for tx in tx_list:
        d = dl[tx]
        if d is None:
            continue
        for i in d["vIn"]:
            if d["vIn"][i]["blockHeight"] is not None:
                tx_conf.append(d["vIn"][i]["txId"])

    q = time.time()
    tx_conf = tx_conf[:80]
    r = requests.post(conf["base_url"] + "/rest/transactions/by/pointer/list",
                      data= json.dumps(tx_conf).encode())
    tms = int(time.time())
    assert r.status_code == 200
    dl = r.json()["data"]
    assert time.time() - q < 2
    counter = 0


    for tx in tx_conf:

        time.sleep(0.4)

        d = dl[tx]

        r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/" + tx)
        assert r.status_code == 200
        t = r.json()["data"]
        if d is None:
            continue
        counter += 1
        assert t["txId"] == d["txId"]

        assert t["version"] == d["version"]
        if not t["coinbase"]:
            assert t["hash"] == d["hash"]
            assert t["size"] == d["size"]
            assert t["vSize"] == d["vSize"]
            assert t["bSize"] == d["bSize"]
            assert t["weight"] == d["weight"]
            assert t["segwit"] == d["segwit"]
        assert t["lockTime"] == d["lockTime"]
        assert t["data"] == d["data"]
        assert t["coinbase"] == d["coinbase"]

        assert t["amount"] == d["amount"]
        for i in t["vIn"]:
            if t["coinbase"]:
                continue
            assert t["vIn"][i]["txId"] == d["vIn"][str(i)]["txId"]
            assert t["vIn"][i]["vOut"] == d["vIn"][str(i)]["vOut"]
            assert t["vIn"][i]["type"] == d["vIn"][str(i)]["type"]
            assert t["vIn"][i]["amount"] == d["vIn"][str(i)]["amount"]
            assert t["vIn"][i]["scriptPubKey"] == d["vIn"][str(i)]["scriptPubKey"]
            assert t["vIn"][i]["scriptPubKeyOpcodes"] == d["vIn"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vIn"][i]["scriptPubKeyAsm"] == d["vIn"][str(i)]["scriptPubKeyAsm"]
            if t["vIn"][i]["blockHeight"] != -1:
                assert t["vIn"][i]["blockHeight"] == d["vIn"][str(i)]["blockHeight"]
                assert t["vIn"][i]["confirmations"] == d["vIn"][str(i)]["confirmations"]
            if "address" in t["vIn"][i]:
                assert t["vIn"][i]["address"] == d["vIn"][str(i)]["address"]
            assert t["vIn"][i]["scriptSig"] == d["vIn"][str(i)]["scriptSig"]
            assert t["vIn"][i]["sequence"] == d["vIn"][str(i)]["sequence"]
            if "txInWitness" in t["vIn"][i]:
                assert t["vIn"][i]["txInWitness"] == d["vIn"][str(i)]["txInWitness"]
            if not t["coinbase"]:
                assert t["vIn"][i]["scriptSigOpcodes"] == d["vIn"][str(i)]["scriptSigOpcodes"]
                assert t["vIn"][i]["scriptSigAsm"] == d["vIn"][str(i)]["scriptSigAsm"]
        for i in t["vOut"]:
            assert t["vOut"][i]["value"] == d["vOut"][str(i)]["value"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["nType"] == d["vOut"][str(i)]["nType"]
            assert t["vOut"][i]["type"] == d["vOut"][str(i)]["type"]
            if "addressHash" in t["vOut"][i]:
                assert t["vOut"][i]["addressHash"] == d["vOut"][str(i)]["addressHash"]
                assert t["vOut"][i]["address"] == d["vOut"][str(i)]["address"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["scriptPubKeyAsm"] == d["vOut"][str(i)]["scriptPubKeyAsm"]
            assert t["vOut"][i]["scriptPubKeyOpcodes"] == d["vOut"][str(i)]["scriptPubKeyOpcodes"]

            if t["vOut"][i]["spent"] != d["vOut"][str(i)]["spent"]:
                time.sleep(1)
                if t["vOut"][i]["spent"]:
                    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/" +
                                     t["vOut"][i]["spent"][0]["txId"])
                    assert r.status_code == 200
                    st = r.json()["data"]
                    if not ((st["time"] - tms >= 0) or (st["time"] - t["time"])):
                        assert t["vOut"][i]["spent"] == d["vOut"][str(i)]["spent"]


    assert counter > 0



    assert tx_conf




def test_get_mixed_transaction_by_pointer_list_extended(conf):
    if not conf["option_transaction_history"]:
        return
    tms = int(time.time())
    r = requests.get("https://api.bitaps.com/btc/v1/mempool/transactions")
    assert r.status_code == 200
    t = r.json()["data"]["transactions"]
    tx_list = [i["txId"] for i in t][:5]
    tx_list.append("f8dc0b9ec82b62173b0e1689d5158b06ccb4494dd2380116ad526819e57ec86c")
    tx_list.append("7eec7bd452c07a8dbdb4bdb6f900f827a2e9814852b2546878b93970bbcb3dd8") # coinbase
    tx_list.append("da46a2d6f3cf7e6a54fa01ed4f1c9195bee62638e4ce78b16a48e207e148dc47")

    q = time.time()
    r = requests.post(conf["base_url"] + "/rest/transactions/by/pointer/list",
                      data= json.dumps(tx_list).encode())
    assert r.status_code == 200
    dl = r.json()["data"]
    assert time.time() - q < 1


    counter = 0
    for tx in tx_list:
        time.sleep(0.4)
        d = dl[tx]

        r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/" + tx)
        assert r.status_code == 200
        t = r.json()["data"]
        if d is None:
            continue
        counter += 1
        assert t["txId"] == d["txId"]

        assert t["version"] == d["version"]
        if not t["coinbase"]:
            assert t["hash"] == d["hash"]
            assert t["size"] == d["size"]
            assert t["vSize"] == d["vSize"]
            assert t["bSize"] == d["bSize"]
            assert t["weight"] == d["weight"]
            assert t["segwit"] == d["segwit"]
        assert t["lockTime"] == d["lockTime"]
        assert t["data"] == d["data"]
        assert t["coinbase"] == d["coinbase"]

        assert t["amount"] == d["amount"]
        for i in t["vIn"]:
            if t["coinbase"]:
                continue
            assert t["vIn"][i]["txId"] == d["vIn"][str(i)]["txId"]
            assert t["vIn"][i]["vOut"] == d["vIn"][str(i)]["vOut"]
            assert t["vIn"][i]["type"] == d["vIn"][str(i)]["type"]
            assert t["vIn"][i]["amount"] == d["vIn"][str(i)]["amount"]
            assert t["vIn"][i]["scriptPubKey"] == d["vIn"][str(i)]["scriptPubKey"]
            assert t["vIn"][i]["scriptPubKeyOpcodes"] == d["vIn"][str(i)]["scriptPubKeyOpcodes"]
            assert t["vIn"][i]["scriptPubKeyAsm"] == d["vIn"][str(i)]["scriptPubKeyAsm"]
            if t["vIn"][i]["blockHeight"] != -1:
                assert t["vIn"][i]["blockHeight"] == d["vIn"][str(i)]["blockHeight"]
                assert t["vIn"][i]["confirmations"] == d["vIn"][str(i)]["confirmations"]
            if "address" in t["vIn"][i]:
                assert t["vIn"][i]["address"] == d["vIn"][str(i)]["address"]
            assert t["vIn"][i]["scriptSig"] == d["vIn"][str(i)]["scriptSig"]
            assert t["vIn"][i]["sequence"] == d["vIn"][str(i)]["sequence"]
            if "txInWitness" in t["vIn"][i]:
                assert t["vIn"][i]["txInWitness"] == d["vIn"][str(i)]["txInWitness"]
            if not t["coinbase"]:
                assert t["vIn"][i]["scriptSigOpcodes"] == d["vIn"][str(i)]["scriptSigOpcodes"]
                assert t["vIn"][i]["scriptSigAsm"] == d["vIn"][str(i)]["scriptSigAsm"]
        for i in t["vOut"]:
            assert t["vOut"][i]["value"] == d["vOut"][str(i)]["value"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["nType"] == d["vOut"][str(i)]["nType"]
            assert t["vOut"][i]["type"] == d["vOut"][str(i)]["type"]
            if "addressHash" in t["vOut"][i]:
                assert t["vOut"][i]["addressHash"] == d["vOut"][str(i)]["addressHash"]
                assert t["vOut"][i]["address"] == d["vOut"][str(i)]["address"]
            assert t["vOut"][i]["scriptPubKey"] == d["vOut"][str(i)]["scriptPubKey"]
            assert t["vOut"][i]["scriptPubKeyAsm"] == d["vOut"][str(i)]["scriptPubKeyAsm"]
            assert t["vOut"][i]["scriptPubKeyOpcodes"] == d["vOut"][str(i)]["scriptPubKeyOpcodes"]

            if t["vOut"][i]["spent"] != d["vOut"][str(i)]["spent"]:
                time.sleep(1)
                r = requests.get("https://api.bitaps.com/btc/v1/blockchain/transaction/" +
                                 t["vOut"][i]["spent"][0]["txId"])
                assert r.status_code == 200
                st = r.json()["data"]
                if not ((st["time"] - tms >= 0) or (st["time"] - t["time"])):
                    assert t["vOut"][i]["spent"] == d["vOut"][str(i)]["spent"]

    assert counter > 0
