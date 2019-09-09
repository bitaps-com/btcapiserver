import unittest
import configparser
import os, sys
from pybtc import *
import base64
import zlib
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
option_block_filters = True if config["OPTIONS"]["block_filters"] == "on" else False
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



    def test_get_block_range_filter_headers(self):
        if option_block_filters:
            print("/rest/block/range/filter/headers:\n")
            r = requests.get(base_url + "/rest/block/range/filter/headers")
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            for i in d:
                print(i)
            r = requests.get(base_url + "/rest/block/range/filter/headers/"
                                        "f6a7f0f3df0c14aeaea3e11fd0c51acac724fd092e1bd1bf123e05444cd77eef")
            self.assertEqual(r.status_code, 200)

            print("OK\n")

    def test_get_block_range_filter(self):
        if option_block_filters:
            print("/rest/block/range/filter/{filter_header}:\n")
            r = requests.get(base_url + "/rest/block/range/filter/"
                                        "63c46a50b418913b7dadae14d0dad0147ed3fbb09c3ff1427076f54d04528303")
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            dd = base64.b64decode(d.encode())
            f = zlib.decompress(dd)
            print("filter length base64 compressed ",len(d)/1024/1024 , "Mb; cpmressed ",
                  len(dd)/1024/1024, "Mb; decompressed ", len(f)/1024/1024, " Mb;")

            self.assertEqual(sha256(f, hex=1), "63c46a50b418913b7dadae14d0dad0147ed3fbb09c3ff1427076f54d04528303")
            print("OK\n")

    def test_get_block_filter_headers(self):
        if option_block_filters:
            print("/rest/block/filter/headers/{block_pointer}/{count}:\n")
            r = requests.get(base_url + "/rest/block/filter/headers/"
                                        "00000000770ebe897270ca5f6d539d8afb4ea4f4e757761a34ca82e17207d886/10" )
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            print(d)
            self.assertEqual(len(d), 10)
            r = requests.get(base_url + "/rest/block/filter/headers/"
                                        "00000000770ebe897270ca5f6d539d8afb4ea4f4e757761a34ca82e17207d886" )
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            self.assertEqual(len(d), 2000)
            print("OK\n")

    def test_get_block_filters(self):
        if option_block_filters:
            print("/rest/block/filters/{block_pointer}/{count}:\n")
            r = requests.get(base_url + "/rest/block/filters/"
                                        "00000000770ebe897270ca5f6d539d8afb4ea4f4e757761a34ca82e17207d886/10" )
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]

            self.assertEqual(len(d), 10)

            r = requests.get(base_url + "/rest/block/filters/200000" )
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            self.assertEqual(len(d), 144)
            print("OK\n")

    def test_get_block_filter(self):
        if option_block_filters:
            print("/rest/block/filter/{filter_header}:\n")
            r = requests.get(base_url + "/rest/block/filter/"
                                        "721496856afe6bd1dd9b07a44b1a8cf55e3b36a5cbdf592d067ccbc571f9f186")
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            print("OK\n")

    def test_get_block_transactions(self):
        print("/rest/block/transactions/{block_pointer}:\n")
        r = requests.get(base_url + "/rest/block/transactions/100000")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        for t in d["transactions"]:
            print(">>>", t)
        print("OK\n")

