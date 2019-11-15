import unittest
import configparser
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
option_block_batch_filters = True if config["OPTIONS"]["block_batch_filters"] == "on" else False

block_filter_capacity = int(config["OPTIONS"]["block_filter_capacity"])

base_url = config["SERVER"]["api_endpoint_test_base_url"]




def create_gcs(elements, N=None, M=784931, P=19, v_0=0, v_1=0, hashed=False, hex=False):
    # M=784931
    # P=19
    # BIP 158  constant values
    # v_0, v_1 - randomization vectors for siphash
    ttb = 0
    ttc = 0
    ttb_max = 0
    ttb_min = 99999999
    t = bytes()
    if N is None:
        N = len(elements)

    if N >= 4294967296 or M >= 4294967296:
        raise TypeError("elements count MUST be <2^32 and M MUST be <2^32")

    gcs_filter = bitarray(endian='big')
    gcs_filter_append = gcs_filter.append
    last = 0
    if not hashed:
        elements = [map_into_range(siphash(e, v_0=v_0, v_1=v_1), N * M) for e in elements]
    gcs_filter = bytearray()
    for value in  sorted(elements):
        delta = value - last
        bl = delta.bit_length()
        print(ttc, bl)
        # if bl < 0: bl = 1
        # if bl < 12:
        #     bl = 12
        #     ttb += 12
        # else:
        #     ttb += bl
        ttb += bl
        if ttb_max < delta.bit_length(): ttb_max = bl
        if ttb_min > delta.bit_length(): ttb_min = bl
        ttc+=1
        # t += int_to_bytes(bl)
        last = value

        # gcs_filter += int_to_c_int(delta)

    f = bytes(gcs_filter)
    print("ttb", ttb)
    print("ttb_max", ttb_max)
    print("ttb_min", ttb_min)
    print("ttc", ttc)
    print("len ", len(t), len(zlib.compress(t)))
    return f.hex() if hex else f

class BlockAPIEndpointsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("\nTesting blocks API endpoints:\n")

    def test_get_block_last(self):
        print("/rest/block/last:\n")
        r = requests.get(base_url + "/rest/block/last")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        self.assertEqual('height' in d, True)
        self.assertEqual('hash' in d, True)
        self.assertEqual('header' in d, True)
        self.assertEqual('adjustedTimestamp' in d, True)
        self.assertEqual(d['hash'], rh2s(double_sha256(base64.b64decode(d["header"]), hex=0)))
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
        self.assertEqual(len(d), 2000)
        print("OK\n")

        print("/rest/block/headers/{block_pointer}/{count}:\n")
        r = requests.get(base_url + "/rest/block/headers/10/2")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        self.assertEqual(len(d), 2)
        r = requests.get(base_url + "/rest/block/headers/10000000")
        self.assertEqual(r.status_code, 404)
        r = requests.get(base_url + "/rest/block/0")
        h = r.json()["data"]["hash"]
        r = requests.get(base_url + "/rest/block/headers/0/20")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        self.assertEqual(h, rh2s(base64.b64decode(d[0])[4:32+4]))
        print("OK\n")


    def test_get_block_utxo(self):
        print("/rest/block/utxo/{block_pointer}:\n")
        r = requests.get(base_url + "/rest/block/utxo/0")
        self.assertEqual(r.status_code, 200)
        pprint(r.json())
        r = requests.get(base_url + "/rest/block/utxo/100000")
        self.assertEqual(r.status_code, 200)
        pprint(r.json())
        print("OK\n")

    def test_get_block_transactions(self):
        print("/rest/block/transactions/{block_pointer}:\n")
        r = requests.get(base_url + "/rest/block/transactions/100000")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        print("Transactions for block 100000:")
        for t in d["transactions"]:
            print("    ", t)
            if not t["coinbase"]:
                for i in t["vIn"]:
                    self.assertEqual("scriptPubKey" in  t["vIn"][i], True)
            self.assertEqual(t["fee"] >= 0, True)

        r = requests.get(base_url + "/rest/block/last")
        self.assertEqual(r.status_code, 200)
        d = r.json()["data"]
        h = d["height"]

        for k in range(h-1000, h):
            q = time.time()
            r = requests.get(base_url + "/rest/block/transactions/" + str(k))
            s = time.time() - q
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            for t in d["transactions"]:
                if not t["coinbase"]:
                    for i in t["vIn"]:
                        if "scriptPubKey" not in  t["vIn"][i]:
                            print("error:::", t, k)
                            print(t)
                        self.assertEqual("scriptPubKey" in  t["vIn"][i], True)
                self.assertEqual(t["fee"] >= 0, True)
            print(k, " block transactions %s [%s] OK" % (len(d["transactions"]), round(s,4)), r.json()["time"])


        print("OK\n")




    def test_get_block_range_filter_headers(self):
        if option_block_filters:
            print("/rest/block/range/filter/headers:\n")
            r = requests.get(base_url + "/rest/block/range/filter/headers")
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            print("Block range filter headers:")
            # todo add precomputed header hashes check
            for i in d:
                print(i)
            r = requests.get(base_url + "/rest/block/range/filter/headers/"
                                        "42f8590d88ad626e882a6267ce80c0457f10125b1476154160ecb8e4e2aa7220")
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
            print("filter length base64 compressed ",len(d)/1024/1024 , "Mb; compressed ",
                  len(dd)/1024/1024, "Mb; decompressed ", len(f)/1024/1024, " Mb;")

            self.assertEqual(sha256(f, hex=1), "63c46a50b418913b7dadae14d0dad0147ed3fbb09c3ff1427076f54d04528303")


            r = requests.get(base_url + "/rest/block/range/filter/headers/"
                                        "42f8590d88ad626e882a6267ce80c0457f10125b1476154160ecb8e4e2aa7220")
            self.assertEqual(r.status_code, 200)
            hl = r.json()["data"]
            tf = list()
            tt = 0
            c =0
            k = bytearray()
            try:
                for h in hl:
                    h= h["header"]
                    r = requests.get(base_url + "/rest/block/range/filter/"+h)
                    d = r.json()["data"]
                    dd = base64.b64decode(d.encode())
                    f = zlib.decompress(dd)
                    tt += len(dd)
                    N = c_int_to_int(f)
                    i = c_int_len(N)
                    print("N", N)
                    fl1 = decode_gcs(f[i:], N, P=block_filter_bits)

                    print("filter ", c + 1, len(fl1), len(dd))

                    for i in fl1:
                        k += i.to_bytes(5, byteorder="big")
                    print("len raw", len(k), len(zlib.compress(k, level=9)))

                    tf += fl1
                    c+= 1
                    if c > 1:
                        break

                print("tt ", tt)
                k = set(tf)
                print("k", len(k))

                kf = create_gcs(list(k), hashed=True, M=block_filter_fps, P=block_filter_bits)
            except:
                import traceback
                print(traceback.format_exc())
            print("len filter 16450659 14829728")
            print("len filter", len(kf), len(zlib.compress(kf)))

            print("OK\n")

    def test_get_block_filter_headers(self):
        if option_block_filters:
            print("/rest/block/filter/headers/{block_pointer}/{count}:\n")
            r = requests.get(base_url + "/rest/block/filter/headers/"
                                        "00000000770ebe897270ca5f6d539d8afb4ea4f4e757761a34ca82e17207d886/10")
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            print(d)
            self.assertEqual(len(d), 10)
            r = requests.get(base_url + "/rest/block/filter/headers/"
                                        "000000002c05cc2e78923c34df87fd108b22221ac6076c18f3ade378a4d915e9")
            self.assertEqual(r.status_code, 200)
            d = r.json()["data"]
            self.assertEqual(len(d), 2000)
            r = requests.get(base_url + "/rest/block/filter/headers/10")
            self.assertEqual(r.status_code, 200)
            d2 = r.json()["data"]
            for i in range(2000):
                self.assertEqual(d[i], d2[i])
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
            print(d)
            print("OK\n")

