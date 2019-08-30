import unittest
import configparser
import os, sys
from pybtc import *
import json
import requests
from pprint import pprint

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

class BlockAPIEndpointsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("\nTesting blocks API endpoints:\n")

    def test_get_block_last(self):
        print("/rest/block/last:\n")
        r = requests.get(base_url + "/rest/block/last")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        pprint(d)
        self.assertEqual('height' in d, True)
        self.assertEqual('hash' in d, True)
        self.assertEqual('header' in d, True)
        self.assertEqual('adjustedTimestamp' in d, True)
        print("OK\n")

    def test_get_block_pointer(self):
        print("/rest/block/{block_pointer}:\n")
        r = requests.get(base_url + "/rest/block/0")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        pprint(d)
        self.assertEqual('height' in d, True)
        self.assertEqual('hash' in d, True)
        self.assertEqual('header' in d, True)
        self.assertEqual('adjustedTimestamp' in d, True)
        r = requests.get(base_url + "/rest/block/" + d["hash"])
        self.assertEqual(r.status_code, 200)
        d2 = r.json()["data"]
        self.assertEqual(d['height'], d2['height'])
        self.assertEqual(d['hash'], d2['hash'])
        self.assertEqual(d['header'], d2['header'])
        self.assertEqual(d['adjustedTimestamp'], d2['adjustedTimestamp'])

        r = requests.get(base_url + "/rest/block/ffff00000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
        self.assertEqual(r.status_code, 404)
        print("OK\n")


    def test_get_block_headers(self):
        print("/rest/block/headers/{block_pointer}:\n")
        r = requests.get(base_url + "/rest/block/headers/10")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        print("received ", len(d), "headers")
        print("OK\n")
        print("/rest/block/headers/{block_pointer}/{count}:\n")
        r = requests.get(base_url + "/rest/block/headers/10/2")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        self.assertEqual(len(d), 2)
        pprint(d)
        r = requests.get(base_url + "/rest/block/headers/10000000")
        self.assertEqual(r.status_code, 404)

        r = requests.get(base_url + "/rest/block/0")
        h = r.json()["data"]["hash"]

        r = requests.get(base_url + "/rest/block/headers/0/20")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        self.assertEqual(h, rh2s(bytes_from_hex(d[0][8:64+8])))
        print("OK\n")


    def test_get_block_utxo(self):
        print("/rest/block/utxo/{block_pointer}:\n")
        r = requests.get(base_url + "/rest/block/utxo/0")
        self.assertEqual(r.status_code, 200)
        pprint(r.json())
        print("OK\n")


