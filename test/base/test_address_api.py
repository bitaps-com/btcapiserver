import requests
from pybtc import *


def test_address_state_basic(conf):
    r = requests.get(conf["base_url"] + "/rest/address/state/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/address/state/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
    d = r.json()["data"]
    if  conf["option_transaction_history"]:
        assert d_1["receivedAmount"] == d["receivedAmount"]
        assert d_1["sentAmount"] == d["sentAmount"]
    else:
        assert d_1["confirmed"] == d["receivedAmount"] - d["sentAmount"]

    r = requests.get(conf["base_url"] + "/rest/address/state/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?type=P2PKH")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    r = requests.get(conf["base_url"] + "/rest/address/state/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?type=PUBKEY")
    assert r.status_code == 200
    d_2 = r.json()["data"]
    if  conf["option_transaction_history"]:
        assert d_1["receivedAmount"] +  d_2["receivedAmount"] == d["receivedAmount"]
        assert d_1["sentAmount"] + d_2["sentAmount"]== d["sentAmount"]
    else:
        assert d_1["confirmed"] + d_2["confirmed"]== d["receivedAmount"] - d["sentAmount"]

def test_get_address_by_list_basic(conf):
    r = requests.post(conf["base_url"] + "/rest/addresses/state/by/address/list",
                      json=["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"])
    assert r.status_code == 200
    dl = r.json()["data"]

    r = requests.get(conf["base_url"] + "/rest/address/state/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    if  conf["option_transaction_history"]:
        assert dl["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"]["confirmed"] == d_1["receivedAmount"] - d_1["sentAmount"]
    else:
        assert dl["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"]["confirmed"] == d_1["confirmed"]

    r = requests.post(conf["base_url"] + "/rest/addresses/state/by/address/list?type=P2PKH",
                      json=["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"])
    assert r.status_code == 200
    dl = r.json()["data"]

    r = requests.get(conf["base_url"] + "/rest/address/state/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?type=P2PKH")
    assert r.status_code == 200
    d_1 = r.json()["data"]

    if  conf["option_transaction_history"]:
        assert dl["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"]["confirmed"] == d_1["receivedAmount"] - d_1["sentAmount"]
    else:
        assert dl["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"]["confirmed"] == d_1["confirmed"]


    r = requests.post(conf["base_url"] + "/rest/addresses/state/by/address/list?type=PUBKEY",
                      json=["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"])
    assert r.status_code == 200
    dl = r.json()["data"]

    r = requests.get(conf["base_url"] + "/rest/address/state/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?type=PUBKEY")
    assert r.status_code == 200
    d_1 = r.json()["data"]

    if  conf["option_transaction_history"]:
        assert dl["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"]["confirmed"] == d_1["receivedAmount"] - d_1["sentAmount"]
    else:
        assert dl["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"]["confirmed"] == d_1["confirmed"]


def test_get_address_utxo(conf):
    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    a = 0
    for k in d_1:
        a += k["amount"]
    assert a  > 6830321573
    assert d_1[0]["type"]  == "PUBKEY"
    assert d_1[1]["type"]  == "P2PKH"

    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?limit=2")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert len(d_1) == 2
    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?limit=0")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert len(d_1) > 10

    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?limit=1&order=desc")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert d_1[0]["block"] >= 624897

    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?limit=1&order=asc")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert d_1[0]["block"] == 0


    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?from_block=1")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert d_1[0]["type"]  == "P2PKH"

    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?from_block=624897")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert d_1[0]["block"]  == 624897

    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?order_by_amount=1")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert d_1[0]["amount"] == 1

    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?"
                                        "order_by_amount=1&order=dsc")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert d_1[0]["amount"] == 5000000000

    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?"
                                        "type=PUBKEY&order_by_amount=1&order=dsc")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert d_1[0]["amount"] == 5000000000

    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?"
                                        "type=P2PKH&order_by_amount=1&order=dsc")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert d_1[0]["amount"] == 400000000



    r = requests.get(conf["base_url"] + "/rest/address/uutxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
    assert r.status_code == 200

    r = requests.get(conf["base_url"] + "/rest/address/uutxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?limit=2")
    assert r.status_code == 200

    r = requests.get(conf["base_url"] + "/rest/address/uutxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?limit=0")
    assert r.status_code == 200


    r = requests.get(conf["base_url"] + "/rest/address/uutxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?limit=1&order=dsc")
    assert r.status_code == 200

    r = requests.get(conf["base_url"] + "/rest/address/uutxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?limit=1&order=asc")
    assert r.status_code == 200

    r = requests.get(conf["base_url"] + "/rest/address/uutxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?"
                                        "order_by_amount=1&order=dsc")
    assert r.status_code == 200

    r = requests.get(conf["base_url"] + "/rest/address/uutxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?"
                                        "type=PUBKEY&order_by_amount=1&order=asc")
    assert r.status_code == 200

    r = requests.get(conf["base_url"] + "/rest/address/uutxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?"
                                        "type=P2PKH&order_by_amount=1&order=asc")
    assert r.status_code == 200
