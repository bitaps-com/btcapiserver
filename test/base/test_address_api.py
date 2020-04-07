import requests
from pybtc import *


def test_address_state(conf):
    r = requests.get(conf["base_url"] + "/rest/address/state/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    r = requests.get("https://api.bitaps.com/btc/v1/blockchain/address/state/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
    d = r.json()["data"]
    # assert d_1["balance"]["confirmed"] == d["receivedAmount"] - d["sentAmount"]

def test_get_address_by_list(conf):
    r = requests.post(conf["base_url"] + "/rest/addresses/state/by/address/list",
                      json=["37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7",
                            "17ami8DrFbU625nzButiQrFcqEtcCMtUqH",
                            "1G2iR6zmqqBJkcZateaoRAZtgEfETbjzDE"])
    assert r.status_code == 200


def test_get_address_utxo(conf):
    r = requests.get(conf["base_url"] + "/rest/address/utxo/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    a = 0
    for k in d_1:
        a += k["amount"]
    print(a)
    r = requests.get(conf["base_url"] + "/rest/address/state/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
    print(r.json())
    assert r.status_code == 200
    d_1 = r.json()["data"]
    # if conf["option_transaction_history"]:
    #     assert a == d_1["balance"]
    # else:
    #     assert a == d_1["balance"]["confirmed"]