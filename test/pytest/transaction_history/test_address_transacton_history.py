import requests
from pybtc import *


def test_address_state_extended(conf):
    r = requests.get(conf["base_url"] + "/rest/address/state/12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX")

    assert r.status_code == 200
    d_1 = r.json()["data"]
    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/address/state/12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX")

    d = r.json()["data"]
    assert d_1["receivedAmount"] == d["receivedAmount"]
    assert d_1["balance"] == d["balance"]
    assert d_1["sentAmount"] == d["sentAmount"]
    assert d_1["receivedTxCount"] == d["receivedTxCount"]
    assert d_1["sentTxCount"] == d["sentTxCount"]
    assert d_1["receivedOutsCount"] == d["outputsReceivedCount"]
    assert d_1["spentOutsCount"] == d["outputsSpentCount"]
    assert d_1["largestReceivedTxAmount"] == 5000000000
    assert d_1["largestReceivedTxPointer"] == "1:0"
    assert d_1["type"] == d["type"]
    assert d_1["pendingReceivedAmount"] == d["pendingReceivedAmount"]
    assert d_1["pendingSentAmount"] == d["pendingSentAmount"]
    assert d_1["pendingReceivedTxCount"] == d["pendingReceivedTxCount"]
    assert d_1["pendingSentTxCount"] == d["pendingSentTxCount"]
    assert d_1["pendingReceivedOutsCount"] == d["unconfirmedOutputsReceivedCount"]
    assert d_1["pendingSpentOutsCount"] == d["unconfirmedOutputsSpentCount"]


    r = requests.get(conf["base_url"] + "/rest/address/state/12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?type=splitted")
    assert r.status_code == 200
    d_1 = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/address/state/12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX")

    d = r.json()["data"]

    assert d_1["P2PKH"]["balance"] + d_1["PUBKEY"]["balance"]  == d["balance"]
    assert d_1["P2PKH"]["receivedAmount"] + d_1["PUBKEY"]["receivedAmount"]  == d["receivedAmount"]

    r = requests.get(conf["base_url"] + "/rest/address/state/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    import pprint
    pprint.pprint(d_1)


def test_address_transactions(conf):
    r = requests.get(conf["base_url"] + "/rest/address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61")
    assert r.status_code == 200
    l = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/"
                     "address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61")
    assert r.status_code == 200
    r = r.json()["data"]

    assert l["page"] == r["page"]
    assert l["pages"] == r["pages"]
    assert l["limit"] == r["limit"]


    for i in range(len(r["list"])):
        assert l["list"][i]["txId"] == r["list"][i]["txId"]
        assert l["list"][i]["blockHeight"] == r["list"][i]["blockHeight"]
        assert l["list"][i]["confirmations"] == r["list"][i]["confirmations"]
        assert l["list"][i]["blockIndex"] == r["list"][i]["blockIndex"]
        assert l["list"][i]["outAddressCount"] == r["list"][i]["outAddressCount"]
        assert l["list"][i]["inputAddressCount"] == r["list"][i]["inputAddressCount"]
        assert l["list"][i]["outsCount"] == r["list"][i]["outsCount"]
        assert l["list"][i]["inputsCount"] == r["list"][i]["inputsCount"]
        assert l["list"][i]["bSize"] == r["list"][i]["bSize"]
        assert l["list"][i]["size"] == r["list"][i]["size"]
        assert abs(l["list"][i]["amount"]) == r["list"][i]["amount"]
        if l["list"][i]["data"] is not None:
            assert l["list"][i]["data"] == r["list"][i]["data"]
        else:
            assert "" == r["list"][i]["data"]
        # assert l["list"][i]["segwit"] == r["list"][i]["segwit"]
        # assert l["list"][i]["rbf"] == r["list"][i]["rbf"]

    for page in range(10):
        r = requests.get(conf["base_url"] + "/rest/address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61?page=%s" % page)
        assert r.status_code == 200
        l = r.json()["data"]

        r = requests.get("https://api.bitaps.com/btc/v1/blockchain/"
                         "address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61?page=%s" % page)
        assert r.status_code == 200
        r = r.json()["data"]

        assert l["page"] == r["page"]
        assert l["pages"] == r["pages"]
        assert l["limit"] == r["limit"]


        for i in range(len(r["list"])):
            assert l["list"][i]["txId"] == r["list"][i]["txId"]
            assert l["list"][i]["blockHeight"] == r["list"][i]["blockHeight"]
            assert l["list"][i]["confirmations"] == r["list"][i]["confirmations"]
            assert l["list"][i]["blockIndex"] == r["list"][i]["blockIndex"]
            assert l["list"][i]["outAddressCount"] == r["list"][i]["outAddressCount"]
            assert l["list"][i]["inputAddressCount"] == r["list"][i]["inputAddressCount"]
            assert l["list"][i]["outsCount"] == r["list"][i]["outsCount"]
            assert l["list"][i]["inputsCount"] == r["list"][i]["inputsCount"]
            assert l["list"][i]["bSize"] == r["list"][i]["bSize"]
            assert l["list"][i]["size"] == r["list"][i]["size"]
            assert abs(l["list"][i]["amount"]) == r["list"][i]["amount"]
            if l["list"][i]["data"] is not None:
                assert l["list"][i]["data"] == r["list"][i]["data"]
            else:
                assert "" == r["list"][i]["data"]


    r = requests.get(conf["base_url"] + "/rest/address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61?order=asc")
    assert r.status_code == 200
    l = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/"
                     "address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61?order=asc")
    assert r.status_code == 200
    r = r.json()["data"]

    assert l["page"] == r["page"]
    assert l["pages"] == r["pages"]
    assert l["limit"] == r["limit"]

    for i in range(len(r["list"])):
        assert l["list"][i]["txId"] == r["list"][i]["txId"]
        assert l["list"][i]["blockHeight"] == r["list"][i]["blockHeight"]
        assert l["list"][i]["confirmations"] == r["list"][i]["confirmations"]
        assert l["list"][i]["blockIndex"] == r["list"][i]["blockIndex"]
        assert l["list"][i]["outAddressCount"] == r["list"][i]["outAddressCount"]
        assert l["list"][i]["inputAddressCount"] == r["list"][i]["inputAddressCount"]
        assert l["list"][i]["outsCount"] == r["list"][i]["outsCount"]
        assert l["list"][i]["inputsCount"] == r["list"][i]["inputsCount"]
        assert l["list"][i]["bSize"] == r["list"][i]["bSize"]
        assert l["list"][i]["size"] == r["list"][i]["size"]
        assert abs(l["list"][i]["amount"]) == r["list"][i]["amount"]
        if l["list"][i]["data"] is not None:
            assert l["list"][i]["data"] == r["list"][i]["data"]
        else:
            assert "" == r["list"][i]["data"]

    r = requests.get(conf["base_url"] + "/rest/address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61?mode=verbose")
    assert r.status_code == 200
    l = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/"
                     "address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61?mode=verbose")
    assert r.status_code == 200
    r = r.json()["data"]

    assert l["page"] == r["page"]
    assert l["pages"] == r["pages"]
    assert l["limit"] == r["limit"]

    for i in range(len(r["list"])):
        assert l["list"][i]["txId"] == r["list"][i]["txId"]
        assert l["list"][i]["blockHeight"] == r["list"][i]["blockHeight"]
        assert l["list"][i]["confirmations"] == r["list"][i]["confirmations"]
        for k in r["list"][i]["vIn"]:
            assert l["list"][i]["vIn"][k]["vOut"] == r["list"][i]["vIn"][k]["vOut"]
            assert l["list"][i]["vIn"][k]["sequence"] == r["list"][i]["vIn"][k]["sequence"]
            assert l["list"][i]["vIn"][k]["scriptSig"] == r["list"][i]["vIn"][k]["scriptSig"]
            assert l["list"][i]["vIn"][k]["scriptPubKey"] == r["list"][i]["vIn"][k]["scriptPubKey"]
            assert l["list"][i]["vIn"][k]["address"] == r["list"][i]["vIn"][k]["address"]
            assert l["list"][i]["vIn"][k]["amount"] == r["list"][i]["vIn"][k]["amount"]
            assert l["list"][i]["vIn"][k]["type"] == r["list"][i]["vIn"][k]["type"]
            assert l["list"][i]["vIn"][k]["scriptPubKeyOpcodes"] == r["list"][i]["vIn"][k]["scriptPubKeyOpcodes"]
            assert l["list"][i]["vIn"][k]["scriptPubKeyAsm"] == r["list"][i]["vIn"][k]["scriptPubKeyAsm"]
            assert l["list"][i]["vIn"][k]["scriptSigOpcodes"] == r["list"][i]["vIn"][k]["scriptSigOpcodes"]
            assert l["list"][i]["vIn"][k]["scriptSigAsm"] == r["list"][i]["vIn"][k]["scriptSigAsm"]
        for k in r["list"][i]["vOut"]:
            assert l["list"][i]["vOut"][k]["value"] == r["list"][i]["vOut"][k]["value"]
            assert l["list"][i]["vOut"][k]["nType"] == r["list"][i]["vOut"][k]["nType"]
            assert l["list"][i]["vOut"][k]["type"] == r["list"][i]["vOut"][k]["type"]
            assert l["list"][i]["vOut"][k]["scriptPubKey"] == r["list"][i]["vOut"][k]["scriptPubKey"]
            if "addressHash" in r["list"][i]["vOut"][k]:
                assert l["list"][i]["vOut"][k]["addressHash"] == r["list"][i]["vOut"][k]["addressHash"]
                assert l["list"][i]["vOut"][k]["address"] == r["list"][i]["vOut"][k]["address"]
            assert l["list"][i]["vOut"][k]["reqSigs"] == r["list"][i]["vOut"][k]["reqSigs"]
            assert l["list"][i]["vOut"][k]["scriptPubKeyOpcodes"] == r["list"][i]["vOut"][k]["scriptPubKeyOpcodes"]
            assert l["list"][i]["vOut"][k]["scriptPubKeyAsm"] == r["list"][i]["vOut"][k]["scriptPubKeyAsm"]

        assert l["list"][i]["bSize"] == r["list"][i]["bSize"]
        assert l["list"][i]["size"] == r["list"][i]["size"]



    r = requests.get(conf["base_url"] + "/rest/address/transactions/12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?mode=verbose")
    assert r.status_code == 200
    l = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/"
                     "address/transactions/12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?mode=verbose")
    assert r.status_code == 200
    r = r.json()["data"]

    assert l["page"] == r["page"]
    assert l["pages"] == r["pages"]
    assert l["limit"] == r["limit"]

    for i in range(len(r["list"])):
        assert l["list"][i]["txId"] == r["list"][i]["txId"]
        assert l["list"][i]["blockHeight"] == r["list"][i]["blockHeight"]
        assert l["list"][i]["confirmations"] == r["list"][i]["confirmations"]
        for k in r["list"][i]["vIn"]:
            assert l["list"][i]["vIn"][k]["vOut"] == r["list"][i]["vIn"][k]["vOut"]
            assert l["list"][i]["vIn"][k]["sequence"] == r["list"][i]["vIn"][k]["sequence"]
            assert l["list"][i]["vIn"][k]["scriptSig"] == r["list"][i]["vIn"][k]["scriptSig"]
            assert l["list"][i]["vIn"][k]["scriptPubKey"] == r["list"][i]["vIn"][k]["scriptPubKey"]
            assert l["list"][i]["vIn"][k]["address"] == r["list"][i]["vIn"][k]["address"]
            assert l["list"][i]["vIn"][k]["amount"] == r["list"][i]["vIn"][k]["amount"]
            assert l["list"][i]["vIn"][k]["type"] == r["list"][i]["vIn"][k]["type"]
            assert l["list"][i]["vIn"][k]["scriptPubKeyOpcodes"] == r["list"][i]["vIn"][k]["scriptPubKeyOpcodes"]
            assert l["list"][i]["vIn"][k]["scriptPubKeyAsm"] == r["list"][i]["vIn"][k]["scriptPubKeyAsm"]
            assert l["list"][i]["vIn"][k]["scriptSigOpcodes"] == r["list"][i]["vIn"][k]["scriptSigOpcodes"]
            assert l["list"][i]["vIn"][k]["scriptSigAsm"] == r["list"][i]["vIn"][k]["scriptSigAsm"]
        for k in r["list"][i]["vOut"]:
            assert l["list"][i]["vOut"][k]["value"] == r["list"][i]["vOut"][k]["value"]
            assert l["list"][i]["vOut"][k]["nType"] == r["list"][i]["vOut"][k]["nType"]
            assert l["list"][i]["vOut"][k]["type"] == r["list"][i]["vOut"][k]["type"]
            assert l["list"][i]["vOut"][k]["scriptPubKey"] == r["list"][i]["vOut"][k]["scriptPubKey"]
            if "addressHash" in r["list"][i]["vOut"][k]:
                assert l["list"][i]["vOut"][k]["addressHash"] == r["list"][i]["vOut"][k]["addressHash"]
                assert l["list"][i]["vOut"][k]["address"] == r["list"][i]["vOut"][k]["address"]
            assert l["list"][i]["vOut"][k]["scriptPubKeyOpcodes"] == r["list"][i]["vOut"][k]["scriptPubKeyOpcodes"]
            assert l["list"][i]["vOut"][k]["scriptPubKeyAsm"] == r["list"][i]["vOut"][k]["scriptPubKeyAsm"]

        assert l["list"][i]["bSize"] == r["list"][i]["bSize"]
        assert l["list"][i]["size"] == r["list"][i]["size"]


    r = requests.get(conf["base_url"] + "/rest/address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61?from_block=611292")
    assert r.status_code == 200
    l = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/"
                     "address/transactions/194zMcwXswZtkMdpoX9BYDc4T4iGqW1G61")
    assert r.status_code == 200
    r = r.json()["data"]

    assert l["page"] == 1
    assert l["pages"] == 2
    assert l["limit"] == r["limit"]


    for i in range(len(r["list"])):
        assert l["list"][i]["txId"] == r["list"][i]["txId"]
        assert l["list"][i]["blockHeight"] == r["list"][i]["blockHeight"]
        assert l["list"][i]["confirmations"] == r["list"][i]["confirmations"]
        assert l["list"][i]["blockIndex"] == r["list"][i]["blockIndex"]
        assert l["list"][i]["outAddressCount"] == r["list"][i]["outAddressCount"]
        assert l["list"][i]["inputAddressCount"] == r["list"][i]["inputAddressCount"]
        assert l["list"][i]["outsCount"] == r["list"][i]["outsCount"]
        assert l["list"][i]["inputsCount"] == r["list"][i]["inputsCount"]
        assert l["list"][i]["bSize"] == r["list"][i]["bSize"]
        assert l["list"][i]["size"] == r["list"][i]["size"]
        assert abs(l["list"][i]["amount"]) == r["list"][i]["amount"]
        if l["list"][i]["data"] is not None:
            assert l["list"][i]["data"] == r["list"][i]["data"]
        else:
            assert "" == r["list"][i]["data"]
        # assert l["list"][i]["segwit"] == r["list"][i]["segwit"]
        # assert l["list"][i]["rbf"] == r["list"][i]["rbf"]


def test_address_unconfirmed_transactions(conf):
    r = requests.get(conf["base_url"] + "/rest/address/unconfirmed/transactions/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
    assert r.status_code == 200
    l = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/"
                     "address/unconfirmed/transactions/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
    assert r.status_code == 200
    r = r.json()["data"]
    for i in range(len(r["list"])):
        assert l["list"][i]["txId"] == r["list"][i]["txId"]
        assert l["list"][i]["outAddressCount"] == r["list"][i]["outAddressCount"]
        assert l["list"][i]["inputAddressCount"] == r["list"][i]["inputAddressCount"]
        assert l["list"][i]["outsCount"] == r["list"][i]["outsCount"]
        assert l["list"][i]["bSize"] == r["list"][i]["bSize"]
        assert l["list"][i]["size"] == r["list"][i]["size"]
        assert abs(l["list"][i]["amount"]) == r["list"][i]["amount"]
        if l["list"][i]["data"] is not None:
            assert l["list"][i]["data"] == r["list"][i]["data"]
        else:
            assert "" == r["list"][i]["data"]


    r = requests.get(conf["base_url"] + "/rest/address/unconfirmed/transactions/"
                                        "37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7?mode=verbose")
    assert r.status_code == 200
    l = r.json()["data"]

    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/"
                     "address/unconfirmed/transactions/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7?mode=verbose")
    assert r.status_code == 200
    r = r.json()["data"]
    for i in range(len(r["list"])):
        assert l["list"][i]["txId"] == r["list"][i]["txId"]
        assert l["list"][i]["bSize"] == r["list"][i]["bSize"]
        assert l["list"][i]["size"] == r["list"][i]["size"]

        for k in r["list"][i]["vIn"]:
            assert l["list"][i]["vIn"][k]["vOut"] == r["list"][i]["vIn"][k]["vOut"]
            assert l["list"][i]["vIn"][k]["sequence"] == r["list"][i]["vIn"][k]["sequence"]
            assert l["list"][i]["vIn"][k]["scriptSig"] == r["list"][i]["vIn"][k]["scriptSig"]
            assert l["list"][i]["vIn"][k]["scriptPubKey"] == r["list"][i]["vIn"][k]["scriptPubKey"]
            assert l["list"][i]["vIn"][k]["address"] == r["list"][i]["vIn"][k]["address"]
            assert l["list"][i]["vIn"][k]["amount"] == r["list"][i]["vIn"][k]["amount"]
            assert l["list"][i]["vIn"][k]["type"] == r["list"][i]["vIn"][k]["type"]
            assert l["list"][i]["vIn"][k]["scriptPubKeyOpcodes"] == r["list"][i]["vIn"][k]["scriptPubKeyOpcodes"]
            assert l["list"][i]["vIn"][k]["scriptPubKeyAsm"] == r["list"][i]["vIn"][k]["scriptPubKeyAsm"]
            assert l["list"][i]["vIn"][k]["scriptSigOpcodes"] == r["list"][i]["vIn"][k]["scriptSigOpcodes"]
            assert l["list"][i]["vIn"][k]["scriptSigAsm"] == r["list"][i]["vIn"][k]["scriptSigAsm"]
        for k in r["list"][i]["vOut"]:
            assert l["list"][i]["vOut"][k]["value"] == r["list"][i]["vOut"][k]["value"]
            assert l["list"][i]["vOut"][k]["nType"] == r["list"][i]["vOut"][k]["nType"]
            assert l["list"][i]["vOut"][k]["type"] == r["list"][i]["vOut"][k]["type"]
            assert l["list"][i]["vOut"][k]["scriptPubKey"] == r["list"][i]["vOut"][k]["scriptPubKey"]
            if "addressHash" in r["list"][i]["vOut"][k]:
                assert l["list"][i]["vOut"][k]["addressHash"] == r["list"][i]["vOut"][k]["addressHash"]
                assert l["list"][i]["vOut"][k]["address"] == r["list"][i]["vOut"][k]["address"]
            assert l["list"][i]["vOut"][k]["reqSigs"] == r["list"][i]["vOut"][k]["reqSigs"]
            assert l["list"][i]["vOut"][k]["scriptPubKeyOpcodes"] == r["list"][i]["vOut"][k]["scriptPubKeyOpcodes"]
            assert l["list"][i]["vOut"][k]["scriptPubKeyAsm"] == r["list"][i]["vOut"][k]["scriptPubKeyAsm"]
