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
    r = requests.get(conf["base_url"] + "/rest/address/utxo/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    a = 0
    for k in d_1:
        a += k["amount"]

    r = requests.get(conf["base_url"] + "/rest/address/state/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
    assert r.status_code == 200
    d_1 = r.json()["data"]
    assert a == d_1["balance"]["confirmed"]