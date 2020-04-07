import requests
from pybtc import *


def test_address_state_extended(conf):
    r = requests.get(conf["base_url"] + "/rest/address/state/12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX")
    print(r.json())

    assert r.status_code == 200
    d_1 = r.json()["data"]
    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/address/state/12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX")
    print(r.json())

    d = r.json()["data"]
    print(d_1)
    assert d_1["receivedAmount"] == d["receivedAmount"]
    assert d_1["balance"] == d["balance"]
    assert d_1["sentAmount"] == d["sentAmount"]
    assert d_1["receivedTxCount"] == d["receivedTxCount"]
    assert d_1["sentTxCount"] == d["sentTxCount"]
    assert d_1["outputsReceivedCount"] == d["outputsReceivedCount"]
    assert d_1["outputsSpentCount"] == d["outputsSpentCount"]
    # assert d_1["largestTxAmount"] == d["largestTxAmount"]
    # assert d_1["largestTxBlock"] == d["largestTxBlock"]
    # assert d_1["largestTxIndex"] == d["largestTxIndex"]
    assert d_1["type"] == d["type"]
    # assert d_1["pendingReceivedAmount"] == d["pendingReceivedAmount"]
    # assert d_1["pendingSentAmount"] == d["pendingSentAmount"]
    # assert d_1["pendingReceivedTxCount"] == d["pendingReceivedTxCount"]
    # assert d_1["pendingSentTxCount"] == d["pendingSentTxCount"]
    # assert d_1["unconfirmedOutputsReceivedCount"] == d["unconfirmedOutputsReceivedCount"]
