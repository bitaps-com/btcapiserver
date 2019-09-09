import unittest
import configparser
from pybtc import *
import requests
from pprint import pprint
import base64

config_file =   "/config/btcapi-server.conf"
config = configparser.ConfigParser()
config.read(config_file)


option_transaction = True if config["OPTIONS"]["transaction"] == "on" else False
option_merkle_proof = True if config["OPTIONS"]["merkle_proof"] == "on" else False
option_address_state = True if config["OPTIONS"]["address_state"] == "on" else False
option_address_timeline = True if config["OPTIONS"]["address_timeline"] == "on" else False
option_blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
option_transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False
base_url = config["SERVER"]["api_endpoint_test_base_url"]

class TransactionsAPIEndpointsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("\nTesting blocks API endpoints:\n")

    def test_get_transaction(self):
        print("/rest/transaction/{tx_pointer}:\n")

        # r = requests.get(base_url + "/rest/transaction/ee1afca2d1130676503a6db5d6a77075b2bf71382cfdf99231f89717b5257b5b")
        # self.assertEqual(r.status_code, 200)
        # d = r.json()["data"]
        # pprint(d)
        # r = requests.get(base_url + "/rest/transaction/20:0")
        # self.assertEqual(r.status_code, 200)
        # d2 = r.json()["data"]
        # self.assertEqual(d["txId"], d2["txId"])

        r = requests.get(base_url + "/rest/transaction/187347da95da1e5029a33fbcbb0137c80818cb6d9a0b41a4c43d3db1c8ffa58c")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        pprint(d)


        if option_merkle_proof:
            r = requests.get(base_url + "/rest/block/" + d["blockHash"])
            self.assertEqual(r.status_code, 200)
            m_root = rh2s(bytes_from_hex(r.json()["data"]["header"][8 + 64:8 + 64 + 64]))
            self.assertEqual(merkle_root_from_proof(base64.b64decode(d["merkleProof"]), d['txId'], d["blockIndex"]), m_root)
            print("merkle proof verified ->", m_root)

        print("OK\n")

    # def test_get_transaction_hash_by_pointer(self):
    #     print("/rest/transaction/hash/by/blockchain/pointer/{tx_blockchain_pointer}:\n")
    #     r = requests.get(base_url + "/rest/transaction/hash/by/blockchain/pointer/7:0")
    #     self.assertEqual(r.status_code, 200)
    #     pprint(r.json())
    #     print("OK\n")


    # def test_get_transactions_hash_by_list(self):
    #     print("/rest/transactions/hash/by/blockchain/pointer/list:\n")
    #     r = requests.post(base_url + "/rest/transactions/hash/by/blockchain/pointer/list",
    #                       json=["4:0", "0:5"])
    #     self.assertEqual(r.status_code, 200)
    #     pprint(r.json())
    #     print("OK\n")
    #
    # def test_get_transactions_by_list(self):
    #     print("/rest/transactions/by/pointer/list:\n")
    #     r = requests.post(base_url + "/rest/transactions/by/pointer/list",
    #                       json=["9:0",
    #                             "8:1", "300000:2", "c09e98f862e9f62f6f560a48d4ab1fed0ffceaa64eaa2fdf93196ccec6842a86"])
    #     self.assertEqual(r.status_code, 200)
    #     pprint(r.json())
    #     print("OK\n")
    #
    #
    # def test_get_transactions_merkle_proof(self):
    #     if not option_merkle_proof: return
    #
    #     print("/rest/transaction/merkle_proof/{tx_pointer}:\n")
    #     r = requests.get(base_url + "/rest/transaction/merkle_proof/"
    #                                 "c09e98f862e9f62f6f560a48d4ab1fed0ffceaa64eaa2fdf93196ccec6842a86")
    #     self.assertEqual(r.status_code, 200)
    #     pprint(r.json())
    #     print("OK\n")
