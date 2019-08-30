import unittest
import configparser
from pybtc import *
import requests
import zlib
from pprint import pprint
import psycopg2
config_file =   "/config/btcapi-server.conf"
config = configparser.ConfigParser()
config.read(config_file)

postgres_dsn = config["POSTGRESQL"]["dsn"]
option_transaction = True if config["OPTIONS"]["transaction"] == "on" else False
option_merkle_proof = True if config["OPTIONS"]["merkle_proof"] == "on" else False
option_address_state = True if config["OPTIONS"]["address_state"] == "on" else False
option_address_timeline = True if config["OPTIONS"]["address_timeline"] == "on" else False
option_blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
block_filter_bits = int(config["OPTIONS"]["block_filter_bits"])
block_filter_fps = int(config["OPTIONS"]["block_filter_fps"])
block_filter_capacity = int(config["OPTIONS"]["block_filter_capacity"])
option_blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
option_transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False
base_url = config["SERVER"]["api_endpoint_test_base_url"]

class AddressAPIEndpointsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("\nTesting Address API endpoints:\n")

    # def test_address_state(self):
    #     print("/rest/address/state/{address}:\n")
    #
    #     r = requests.get(base_url + "/rest/address/state/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
    #     self.assertEqual(r.status_code, 200)
    #     d = r.json()["data"]
    #     pprint(d)
    #
    #     print("OK\n")
    #

    # def test_get_address_by_list(self):
    #     print("/rest/addresses/state/by/address/list:\n")
    #     r = requests.post(base_url + "/rest/addresses/state/by/address/list",
    #                       json=["37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7",
    #                             "17ami8DrFbU625nzButiQrFcqEtcCMtUqH",
    #                             "1G2iR6zmqqBJkcZateaoRAZtgEfETbjzDE"])
    #     self.assertEqual(r.status_code, 200)
    #     pprint(r.json())
    #     print("OK\n")

    def test_bloom(sels):
        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()

        cursor.execute("SELECT height, filter  "
                                      "FROM block_filters_checkpoints "
                                   "order by height asc limit 1;" )
        rows = cursor.fetchall()
        row  = rows[0]
        b = bytes(row[1])
        N = c_int_to_int(b)
        i = c_int_len(N)
        f = zlib.decompress(b[i:])
        h = row[0]
        f = set(decode_gcs(f, N, P=block_filter_bits))
        print(">>>",len(f), h)

        cursor.execute("SELECT distinct  address  "
                                    "FROM transaction_map "
                                    "where  pointer < %s;" ,  ((130599+1) << 39,) )
        rows = cursor.fetchall()


        print("affected addresses ", len(rows))
        N = block_filter_fps
        M = block_filter_capacity
        elements = [map_into_range(siphash(bytes(e[0])), N * M) for e in rows]

        for a in elements:
            if a not in f:
                print("false negative!")


        print("OK\n")
