import unittest
import configparser
from pybtc import *
import requests
import zlib
from pprint import pprint
import psycopg2
config_file =   "../config/btcapi-server.conf"
config = configparser.ConfigParser()
config.read(config_file)

postgres_dsn = config["POSTGRESQL"]["dsn"]
option_transaction = True if config["OPTIONS"]["transaction"] == "on" else False
option_merkle_proof = True if config["OPTIONS"]["merkle_proof"] == "on" else False
option_address_state = True if config["OPTIONS"]["address_state"] == "on" else False
option_address_timeline = True if config["OPTIONS"]["address_timeline"] == "on" else False
option_blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
option_transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False
base_url = config["SERVER"]["api_endpoint_test_base_url"]

class AddressAPIEndpointsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("\nTesting Address API endpoints:\n")

    def test_address_state(self):
        print("/rest/address/state/{address}:\n")

        r = requests.get(base_url + "/rest/address/state/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
        self.assertEqual(r.status_code, 200)
        d_1 = r.json()["data"]
        pprint(d_1)
        r = requests.get("https://api.bitaps.com/btc/v1/blockchain/address/state/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
        d = r.json()["data"]
        self.assertEqual(d_1["balance"]["confirmed"], d["receivedAmount"] - d["sentAmount"])
        print("OK\n")


    def test_get_address_by_list(self):
        print("/rest/addresses/state/by/address/list:\n")
        r = requests.post(base_url + "/rest/addresses/state/by/address/list",
                          json=["37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7",
                                "17ami8DrFbU625nzButiQrFcqEtcCMtUqH",
                                "1G2iR6zmqqBJkcZateaoRAZtgEfETbjzDE"])
        self.assertEqual(r.status_code, 200)
        pprint(r.json())
        print("OK\n")


    def test_get_address_utxo(self):
        print("/rest/address/utxo/{address}:\n")

        r = requests.get(base_url + "/rest/address/utxo/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
        self.assertEqual(r.status_code, 200)
        d_1 = r.json()["data"]
        # pprint(d_1)
        a = 0
        for k in d_1:
            a += k["amount"]

        r = requests.get(base_url + "/rest/address/state/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
        self.assertEqual(r.status_code, 200)
        d_1 = r.json()["data"]
        self.assertEqual(a, d_1["balance"]["confirmed"])

        print("OK\n")
