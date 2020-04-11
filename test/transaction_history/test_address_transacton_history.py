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
