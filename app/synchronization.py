import asyncio
import os
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
from setproctitle import setproctitle
import logging
import signal
import sys
import asyncpg
import traceback
import colorlog
from collections import deque
import time
from  math import *
import _pickle as pickle
from pybtc import MRU, rh2s, merkle_tree, merkle_proof
from pybtc import parse_script, s2rh
from utils import *
import datetime
import json


class SynchronizationWorker:

    def __init__(self, in_reader, in_writer, out_reader, out_writer, psql_dsn, start_blocks):
        setproctitle('btcapi: sync worker')
        in_writer.close()
        out_reader.close()
        self.out_writer = out_writer
        self.in_reader = in_reader
        self.psql_dsn = psql_dsn
        self.shutdown = False

        self.start_blocks = start_blocks
        self.raw_blocks = deque()
        self.raw_blocks_size = 0
        self.tx_batch_limit = 5000
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.loop = asyncio.get_event_loop()
        self.log = logging.getLogger("sync")
        ch = logging.StreamHandler()
        formatter = colorlog.ColoredFormatter('%(log_color)s%(asctime)s %(module)s: %(message)s')
        formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s %(levelname)s: %(message)s (%(module)s:%(lineno)d)')

        ch.setFormatter(formatter)
        self.log.addHandler(ch)
        self.log.info("Synchronization worker started")
        self.log.setLevel(logging.DEBUG)
        self.loop.set_default_executor(ThreadPoolExecutor(5))

        self.tx_map = deque()
        self.transactions = deque()
        self.tx_map_batches = MRU()
        self.transactions_batches = MRU()

        self.headers = deque()
        self.headers_batches = MRU()

        self.tx_map = deque()
        self.tx_map_batches = MRU()
        self.stxo_batches = MRU()
        self.stxo = deque()
        self.blocks_stat = deque()
        self.blocks_stat_batches = MRU()

        self.block_map_timestamp = dict()
        self.block_best_timestamp = 0
        self.checkpoint = 0
        self.tx_map_table_checkpoint = 0
        self.tx_and_headers_tables_checkpoint = 0
        self.shutdown = False

        self.blockchain_stat = {
            "outputs": {"count": {"total": 0},  # total outputs count
                        # What is the total quantity of
                        # coins in bitcoin blockchain?

                        "amount": {"min": {"pointer": 0,  # output with minimal amount
                                           "value": 0},  # What is the minimum amount of a coins?

                                   "max": {"pointer": 0,  # output with maximal amount
                                           "value": 0},  # What is the maximal amount of a coins?

                                   "total": 0,  # total amount of all outputs

                                   "map": {"count": dict(),  # quantity distribution by amount
                                           # How many coins exceed 1 BTC?

                                           "amount": dict()}  # amounts distribution by amount
                                   # What is the total amount of all coins
                                   # exceeding 10 BTC?
                                   },

                        "type": {"map": {"count": dict(),  # quantity distribution by type
                                         # How many P2SH coins?

                                         "amount": dict(),  # amounts distribution by type
                                         # What is the total amount of
                                         # all P2PKH coins?

                                         "size": dict()}},  # sizes distribution by type
                        # What is the total size
                        # of all P2PKH coins?

                        "age": {"map": {"count": dict(),  # distribution of counts by age
                                        # How many coins older then 1 year?

                                        "amount": dict(),  # distribution of amount by age
                                        # What amount of coins older then 1 month?

                                        "type": dict()  # distribution of counts by type
                                        # How many P2SH coins older then 1 year?
                                        }}

                        },

            "inputs": {"count": {"total": 0},  # total inputs count
                       # What is the total quantity of
                       # spent coins in bitcoin blockchain?

                       "amount": {"min": {"pointer": 0,  # input with minimal amount
                                          "value": 0},  # What is the smallest coin spent?

                                  "max": {"pointer": 0,  # input with maximal amount
                                          "value": 0},  # what is the greatest coin spent?

                                  "total": 0,  # total amount of all inputs
                                  # What is the total amount of
                                  # all spent coins?

                                  "map": {"count": dict(),  # quantity distribution by amount
                                          # How many spent coins exceed 1 BTC?

                                          "amount": dict()}  # amounts distribution by amount
                                  # What is the total amount of
                                  #  all spent coins exceeding 10 BTC?
                                  },
                       "type": {
                           "map": {"count": dict(),  # quantity distribution by type
                                   # How many P2SH  spent coins?

                                   "amount": dict(),  # amounts distribution by type
                                   # What is the total amount
                                   # of all P2PKH spent?

                                   "size": dict()  # sizes distribution by type
                                   # What is the total
                                   # size of all P2PKH spent?

                                   }},

                       # P2SH redeem script statistics
                       "P2SH": {
                           "type": {"map": {"count": dict(),
                                            "amount": dict(),
                                            "size": dict()}
                                    }
                       },

                       # P2WSH redeem script statistics
                       "P2WSH": {
                           "type": {"map": {"count": dict(),
                                            "amount": dict(),
                                            "size": dict()}
                                    }
                       }
                       },

            "transactions": {"count": {"total": 0},

                             "amount": {"min": {"pointer": 0,
                                                "value": 0},

                                        "max": {"pointer": 0,
                                                "value": 0},

                                        "map": {"count": dict(),
                                                "amount": dict(),
                                                "size": dict()},
                                        "total": 0},
                             "size": {"min": {"pointer": 0, "value": 0},
                                      "max": {"pointer": 0, "value": 0},
                                      "total": {"size": 0, "bSize": 0, "vSize": 0},
                                      "map": {"count": dict(), "amount": dict()}},

                             "type": {"map": {"count": dict(), "size": dict(),
                                              "amount": dict()}},

                             "fee": {"min": {"pointer": 0, "value": 0},
                                     "max": {"pointer": 0, "value": 0},
                                     "total": 0}
                             }
        }

        signal.signal(signal.SIGTERM, self.terminate)

        self.processes = []
        self.tasks = [self.loop.create_task(self.save_to_db()),
                      self.loop.create_task(self.message_loop()),
                      self.loop.create_task(self.watchdog())]

        self.loop.run_forever()


    async def watchdog(self):
        last_checkpoint = self.checkpoint
        while True:
            self.checkpoint = min(self.tx_map_table_checkpoint,
                                  self.tx_and_headers_tables_checkpoint)
            if last_checkpoint != self.checkpoint:
                # report checkpoints to main process
                last_checkpoint = self.checkpoint
                pipe_sent_msg(b'checkpoint', pickle.dumps(self.checkpoint), self.writer)
            if self.raw_blocks:
                await self.process_block(self.raw_blocks.popleft())
                continue
            await asyncio.sleep(2)


    def process_block(self, block):
        try:
            block = pickle.loads(block)

            bip69 = True
            rbf = False
            hp = None
            op = None
            oa = None
            pks = None
            inputs_amount = 0

            block_stat = {
                "outputs": {"count": {"total": 0},
                            "amount": {"min": {"pointer": 0, "value": 0},
                                       "max": {"pointer": 0, "value": 0},
                                       "total": 0,
                                       "map": {"count": dict(), "amount": dict()}
                                       },
                            "type": {"map": {"count": dict(), "amount": dict(), "size": dict()}},
                            },

                "inputs": {"count": {"total": 0},
                           "amount": {"min": {"pointer": 0, "value": 0},
                                      "max": {"pointer": 0, "value": 0},
                                      "total": 0,
                                      "map": {"count": dict(), "amount": dict()}
                                      },
                           "type": {
                               "map": {"count": dict(), "amount": dict(), "size": dict()
                                       }},
                           # coin days destroyed
                           "destroyed": {
                               "count": {"total": 0},
                               "value": {"min": {"pointer": 0, "value": 0},
                                         "max": {"pointer": 0, "value": 0},
                                         "total": 0
                                         },
                               "map": {"count": dict(), "amount": dict(), "type": dict()}},
                           # P2SH redeem script statistics
                           "P2SH": {
                               "type": {"map": {"count": dict(), "amount": dict(), "size": dict()}
                                        }
                           },
                           # P2WSH redeem script statistics
                           "P2WSH": {
                               "type": {"map": {"count": dict(), "amount": dict(), "size": dict()}
                                        }
                           }
                           },

                "transactions": {"count": {"total": 0},
                                 "amount": {"min": {"pointer": 0, "value": 0},
                                            "max": {"pointer": 0, "value": 0},
                                            "map": {"count": dict(),
                                                    "amount": dict(),
                                                    "size": dict()},
                                            "total": 0},
                                 "size": {"min": {"pointer": 0, "value": 0},
                                          "max": {"pointer": 0, "value": 0},
                                          "total": {"size": 0, "bSize": 0, "vSize": 0},
                                          "map": {"count": dict(), "amount": dict()}},

                                 "type": {"map": {"count": dict(), "size": dict(),
                                                  "amount": dict()}},

                                 "fee": {"min": {"pointer": 0, "value": 0},
                                         "max": {"pointer": 0, "value": 0},
                                         "total": 0},
                                 "feeRate": {"min": {"pointer": 0, "value": 0},
                                             "max": {"pointer": 0, "value": 0},
                                             "total": 0,
                                             "map": {"count": dict(),
                                                     "amount": dict(),
                                                     "size": dict()}},
                                 "vFeeRate": {"min": {"pointer": 0, "value": 0},
                                             "max": {"pointer": 0, "value": 0},
                                             "total": 0,
                                             "map": {"count": dict(),
                                                     "amount": dict(),
                                                     "size": dict()}}
                                 }
            }

            if self.block_best_timestamp < block["time"]:
                self.block_best_timestamp = block["time"]

            self.block_map_timestamp[block["height"]] = self.block_best_timestamp
            date = datetime.datetime.fromtimestamp(self.block_best_timestamp, datetime.timezone.utc)

            for t in block["rawTx"]:
                tx = block["rawTx"][t]

                if not tx["coinbase"]:
                    for i in tx["vIn"]:
                        inp = tx["vIn"][i]
                        coin = inp["coin"]
                        pointer = (block["height"] << 39) + (t << 20) + (0 << 19) + i

                        if not rbf and inp["sequence"] < 0xfffffffe:
                            rbf = True

                        if bip69:
                            h = rh2s(inp["txId"])
                            if hp is not None:
                                if hp > h:
                                    bip69 = False
                                elif hp == h and op > inp["vOut"]:
                                    bip69 = False
                            hp, op = h, inp["vOut"]

                        idate = datetime.datetime.fromtimestamp(self.block_map_timestamp[coin[0] >> 39],
                                                                datetime.timezone.utc)

                        amount = inp["coin"][1]
                        inputs_amount += amount

                        # count
                        self.blockchain_stat["inputs"]["count"]["total"] += 1
                        block_stat["inputs"]["count"]["total"] += 1

                        # amount
                        self.blockchain_stat["inputs"]["amount"]["total"] += 1
                        block_stat["inputs"]["amount"]["total"] += amount

                        # amount minimum
                        if block_stat["inputs"]["amount"]["min"]["pointer"] == 0 or \
                                block_stat["inputs"]["amount"]["min"]["value"] > amount:
                            block_stat["inputs"]["amount"]["min"]["pointer"] = pointer
                            block_stat["inputs"]["amount"]["min"]["value"] = amount
                        if self.blockchain_stat["inputs"]["amount"]["min"]["pointer"] == 0 or \
                                self.blockchain_stat["inputs"]["amount"]["min"]["value"] > amount:
                            self.blockchain_stat["inputs"]["amount"]["min"]["pointer"] = pointer
                            self.blockchain_stat["inputs"]["amount"]["min"]["value"] = amount

                        # amount maximum
                        if block_stat["inputs"]["amount"]["max"]["value"] < amount:
                            block_stat["inputs"]["amount"]["max"]["pointer"] = pointer
                            block_stat["inputs"]["amount"]["max"]["value"] = amount
                        if self.blockchain_stat["inputs"]["amount"]["max"]["value"] < amount:
                            self.blockchain_stat["inputs"]["amount"]["max"]["pointer"] = pointer
                            self.blockchain_stat["inputs"]["amount"]["max"]["value"] = amount

                        amount_key = str(floor(log10(amount))) if amount else "null"

                        # amount map count
                        try:
                            block_stat["inputs"]["amount"]["map"]["count"][amount_key] += 1
                        except:
                            block_stat["inputs"]["amount"]["map"]["count"][amount_key] = 1
                        try:
                            self.blockchain_stat["inputs"]["amount"]["map"]["count"][amount_key] += 1
                        except:
                            self.blockchain_stat["inputs"]["amount"]["map"]["count"][amount_key] = 1

                        # amount map amount
                        try:
                            block_stat["inputs"]["amount"]["map"]["amount"][amount_key] += amount
                        except:
                            block_stat["inputs"]["amount"]["map"]["amount"][amount_key] = amount
                        try:
                            self.blockchain_stat["inputs"]["amount"]["map"]["amount"][amount_key] += amount
                        except:
                            self.blockchain_stat["inputs"]["amount"]["map"]["amount"][amount_key] = amount



                        age = (self.block_best_timestamp - self.block_map_timestamp[inp["coin"][0]>>39]) // 86400
                        type = inp["coin"][2][0]

                        coin_days = age * amount
                        coin_days_key = str(floor(log10(coin_days))) if coin_days else "null"

                        # type map count

                        try:
                            block_stat["inputs"]["type"]["map"]["count"][type] += 1
                        except:
                            block_stat["inputs"]["type"]["map"]["count"][type] = 1
                        try:
                            self.blockchain_stat["inputs"]["type"]["map"]["count"][type] += 1
                        except:
                            self.blockchain_stat["inputs"]["type"]["map"]["count"][type] = 1


                        # type map amount
                        try:
                            block_stat["inputs"]["type"]["map"]["amount"][type] += amount
                        except:
                            block_stat["inputs"]["type"]["map"]["amount"][type] = amount
                        try:
                            self.blockchain_stat["inputs"]["type"]["map"]["amount"][type] += amount
                        except:
                            self.blockchain_stat["inputs"]["type"]["map"]["amount"][type] = amount

                        # type map size
                        try:
                            block_stat["inputs"]["type"]["map"]["size"][type] += tx["size"]
                        except:
                            block_stat["inputs"]["type"]["map"]["size"][type] = tx["size"]
                        try:
                            self.blockchain_stat["inputs"]["type"]["map"]["size"][type] += tx["size"]
                        except:
                            self.blockchain_stat["inputs"]["type"]["map"]["size"][type] = tx["size"]

                        # coindays destroyed (only block stat)

                        block_stat["inputs"]["destroyed"]["count"]["total"] += 1
                        block_stat["inputs"]["destroyed"]["value"]["total"] += coin_days

                        if block_stat["inputs"]["destroyed"]["value"]["min"]["pointer"] == 0 or \
                                block_stat["inputs"]["destroyed"]["value"]["min"]["value"] > coin_days:
                            block_stat["inputs"]["destroyed"]["value"]["min"]["pointer"] = pointer
                            block_stat["inputs"]["destroyed"]["value"]["min"]["pointer"] = coin_days
                        if block_stat["inputs"]["destroyed"]["value"]["max"]["value"] < coin_days:
                            block_stat["inputs"]["destroyed"]["value"]["max"]["pointer"] = pointer
                            block_stat["inputs"]["destroyed"]["value"]["max"]["value"] = coin_days


                        try:
                            block_stat["inputs"]["destroyed"]["map"]["count"][coin_days_key] += 1
                        except:
                            block_stat["inputs"]["destroyed"]["map"]["count"][coin_days_key] = 1

                        try:
                            block_stat["inputs"]["destroyed"]["map"]["amount"][coin_days_key] += amount
                        except:
                            block_stat["inputs"]["destroyed"]["map"]["amount"][coin_days_key] = amount

                        try:
                            block_stat["inputs"]["destroyed"]["map"]["amount"][coin_days_key] += type
                        except:
                            block_stat["inputs"]["destroyed"]["map"]["amount"][coin_days_key] = type

                        # type

                        if type == 1 or type == 6:
                            # P2SH or P2WSH
                            tt = "P2SH" if type == 1 else "P2WSH"

                            s = parse_script(inp["coin"][2][1:])
                            st = s["type"]
                            if st == "MULTISIG":
                                st += "_%s/%s"  % (s["reqSigs"], s["pubKeys"])

                            try:
                                block_stat["inputs"][tt]["type"]["map"]["count"][st] += 1
                            except:
                                block_stat["inputs"][tt]["type"]["map"]["count"][st] = 1
                            try:
                                self.blockchain_stat["inputs"][tt]["type"]["map"]["count"][st] += 1
                            except:
                                self.blockchain_stat["inputs"][tt]["type"]["map"]["count"][st] = 1

                            try:
                                block_stat["inputs"][tt]["type"]["map"]["amount"][st] += amount
                            except:
                                block_stat["inputs"][tt]["type"]["map"]["amount"][st] = amount
                            try:
                                self.blockchain_stat["inputs"][tt]["type"]["map"]["amount"][st] += amount
                            except:
                                self.blockchain_stat["inputs"][tt]["type"]["map"]["amount"][st] = amount

                            try:
                                block_stat["inputs"][tt]["type"]["map"]["size"][st] += tx["size"]
                            except:
                                block_stat["inputs"][tt]["type"]["map"]["size"][st] = tx["size"]
                            try:
                                self.blockchain_stat["inputs"][tt]["type"]["map"]["size"][st] += tx["size"]
                            except:
                                self.blockchain_stat["inputs"][tt]["type"]["map"]["size"][st] = tx["size"]

                        self.blockchain_stat["outputs"]["amount"]["map"]["count"][idate.year] -= 1
                        self.blockchain_stat["outputs"]["amount"]["map"]["amount"][idate.year] -= amount
                        self.blockchain_stat["outputs"]["amount"]["map"][type][idate.year] -= 1


                # prepare outputs
                for i in tx["vOut"]:
                    out = tx["vOut"][i]
                    pointer = (block["height"] << 39) + (t << 20) + (1 << 19) + i

                    if bip69:
                        if oa is not None:
                            if oa > out["value"]:
                                bip69 = False
                            elif oa == out["value"]:
                                if pks > out["scriptPubKey"].hex():
                                    bip69 = False
                            oa = out["value"]
                            pks = out["scriptPubKey"].hex()

                    amount = out["value"]
                    block_stat["outputs"]["count"]["total"] += 1
                    self.blockchain_stat["outputs"]["count"]["total"] += 1

                    block_stat["outputs"]["amount"]["total"] += amount
                    self.blockchain_stat["outputs"]["amount"]["total"] += amount

                    if block_stat["outputs"]["amount"]["min"]["pointer"] == 0 or \
                            block_stat["outputs"]["amount"]["min"]["value"] > amount:
                        block_stat["outputs"]["amount"]["min"]["pointer"] = pointer
                        block_stat["outputs"]["amount"]["min"]["value"] = amount
                    if self.blockchain_stat["outputs"]["amount"]["min"]["pointer"] == 0 or \
                            self.blockchain_stat["outputs"]["amount"]["min"]["value"] > amount:
                        self.blockchain_stat["outputs"]["amount"]["min"]["pointer"] = pointer
                        self.blockchain_stat["outputs"]["amount"]["min"]["value"] = amount

                    if block_stat["outputs"]["amount"]["max"]["value"] < amount:
                        block_stat["outputs"]["amount"]["max"]["pointer"] = pointer
                        block_stat["outputs"]["amount"]["max"]["value"] = amount
                    if self.blockchain_stat["outputs"]["amount"]["max"]["value"] < amount:
                        self.blockchain_stat["outputs"]["amount"]["max"]["pointer"] = pointer
                        self.blockchain_stat["outputs"]["amount"]["max"]["value"] = amount

                    amount_key = str(floor(log10(amount))) if amount else "null"
                    try:
                        block_stat["outputs"]["amount"]["map"]["count"][amount_key] += 1
                    except:
                        block_stat["outputs"]["amount"]["map"]["count"][amount_key] = 1
                    try:
                        self.blockchain_stat["outputs"]["amount"]["map"]["count"][amount_key] += 1
                    except:
                        self.blockchain_stat["outputs"]["amount"]["map"]["count"][amount_key] = 1

                    try:
                        block_stat["outputs"]["amount"]["map"]["amount"][amount_key] += amount
                    except:
                        block_stat["outputs"]["amount"]["map"]["amount"][amount_key] = amount
                    try:
                        self.blockchain_stat["outputs"]["amount"]["map"]["amount"][amount_key] += amount
                    except:
                        self.blockchain_stat["outputs"]["amount"]["map"]["amount"][amount_key] = amount

                    # age

                    try:
                        self.blockchain_stat["outputs"]["amount"]["map"]["count"][date.year] += 1

                    except:
                        self.blockchain_stat["outputs"]["amount"]["map"]["count"][date.year] = 1
                    try:
                        self.blockchain_stat["outputs"]["amount"]["map"]["amount"][date.year] += amount
                    except:
                        self.blockchain_stat["outputs"]["amount"]["map"]["amount"][date.year] = amount
                    try:
                        self.blockchain_stat["outputs"]["amount"]["map"][out["nType"]]
                    except:
                        self.blockchain_stat["outputs"]["amount"]["map"][out["nType"]] = dict()
                    try:
                        self.blockchain_stat["outputs"]["amount"]["map"][out["nType"]][date.year] += 1
                    except:
                        self.blockchain_stat["outputs"]["amount"]["map"][out["nType"]][date.year] = 1



                # transaction


                pointer = (block["height"] << 19) + t
                amount = tx["amount"]
                amount_key = str(floor(log10(amount))) if amount else "null"

                block_stat["transactions"]["count"]["total"] += 1
                self.blockchain_stat["transactions"]["count"]["total"] += 1

                block_stat["transactions"]["amount"]["total"] += amount
                self.blockchain_stat["transactions"]["amount"]["total"] += amount

                if block_stat["transactions"]["amount"]["min"]["pointer"] == 0 or \
                        block_stat["transactions"]["amount"]["min"]["value"] > amount:
                    block_stat["transactions"]["amount"]["min"]["pointer"] = pointer
                    block_stat["transactions"]["amount"]["min"]["value"] = amount
                if self.blockchain_stat["transactions"]["amount"]["min"]["pointer"] == 0 or \
                        self.blockchain_stat["transactions"]["amount"]["min"]["value"] > amount:
                    self.blockchain_stat["transactions"]["amount"]["min"]["pointer"] = pointer
                    self.blockchain_stat["transactions"]["amount"]["min"]["value"] = amount

                if block_stat["transactions"]["amount"]["max"]["value"] < amount:
                    block_stat["transactions"]["amount"]["max"]["pointer"] = pointer
                    block_stat["transactions"]["amount"]["max"]["value"] = amount
                if self.blockchain_stat["transactions"]["amount"]["max"]["value"] < amount:
                    self.blockchain_stat["transactions"]["amount"]["max"]["pointer"] = pointer
                    self.blockchain_stat["transactions"]["amount"]["max"]["value"] = amount

                try:
                    block_stat["transactions"]["amount"]["map"]["amount"][amount_key] += amount
                except:
                    block_stat["transactions"]["amount"]["map"]["amount"][amount_key] = amount
                try:
                    self.blockchain_stat["transactions"]["amount"]["map"]["amount"][amount_key] += amount
                except:
                    self.blockchain_stat["transactions"]["amount"]["map"]["amount"][amount_key] = amount


                try:
                    block_stat["transactions"]["amount"]["map"]["count"][amount_key] += 1
                except:
                    block_stat["transactions"]["amount"]["map"]["count"][amount_key] = 1
                try:
                    self.blockchain_stat["transactions"]["amount"]["map"]["count"][amount_key] += 1
                except:
                    self.blockchain_stat["transactions"]["amount"]["map"]["count"][amount_key] = 1

                try:
                    block_stat["transactions"]["amount"]["map"]["size"][amount_key] += tx["size"]
                except:
                    block_stat["transactions"]["amount"]["map"]["size"][amount_key] = tx["size"]
                try:
                    self.blockchain_stat["transactions"]["amount"]["map"]["size"][amount_key] += tx["size"]
                except:
                    self.blockchain_stat["transactions"]["amount"]["map"]["size"][amount_key] = tx["size"]

                fee = inputs_amount - amount
                fee_rate = int((fee / tx["size"]) * 100)
                v_fee_rate = int((fee / tx["vSize"]) * 100)
                fee_rate_key = int(floor(fee_rate / 10))
                v_fee_rate_key = int(floor(v_fee_rate / 10))
                size = tx["size"]
                block_stat["transactions"]["size"]["total"]["size"] += size
                self.blockchain_stat["transactions"]["size"]["total"]["size"] += size
                block_stat["transactions"]["size"]["total"]["vSize"] += tx["vSize"]
                self.blockchain_stat["transactions"]["size"]["total"]["vSize"] += tx["vSize"]
                block_stat["transactions"]["size"]["total"]["bSize"] += tx["bSize"]
                self.blockchain_stat["transactions"]["size"]["total"]["bSize"] += tx["bSize"]

                if block_stat["transactions"]["size"]["min"]["pointer"] == 0 or \
                        block_stat["transactions"]["size"]["min"]["value"] > size:
                    block_stat["transactions"]["size"]["min"]["pointer"] = pointer
                    block_stat["transactions"]["size"]["min"]["value"] = size
                if self.blockchain_stat["transactions"]["size"]["min"]["pointer"] == 0 or \
                        self.blockchain_stat["transactions"]["size"]["min"]["value"] > size:
                    self.blockchain_stat["transactions"]["size"]["min"]["pointer"] = pointer
                    self.blockchain_stat["transactions"]["size"]["min"]["value"] = size

                if block_stat["transactions"]["size"]["max"]["value"] < size:
                    block_stat["transactions"]["size"]["max"]["pointer"] = pointer
                    block_stat["transactions"]["size"]["max"]["value"] = size
                if self.blockchain_stat["transactions"]["size"]["max"]["value"] < size:
                    self.blockchain_stat["transactions"]["size"]["max"]["pointer"] = pointer
                    self.blockchain_stat["transactions"]["size"]["max"]["value"] = size

                if size < 1000:
                    size_key = str(floor(size / 100))
                else:
                    size_key = "%sK" % floor(size / 1000)

                try:
                    block_stat["transactions"]["size"]["map"]["count"][size_key] += 1
                except:
                    block_stat["transactions"]["size"]["map"]["count"][size_key] = 1
                try:
                    self.blockchain_stat["transactions"]["size"]["map"]["count"][size_key] += 1
                except:
                    self.blockchain_stat["transactions"]["size"]["map"]["count"][size_key] = 1

                try:
                    block_stat["transactions"]["size"]["map"]["amount"][size_key] += 1
                except:
                    block_stat["transactions"]["size"]["map"]["amount"][size_key] = 1
                try:
                    self.blockchain_stat["transactions"]["size"]["map"]["amount"][size_key] += 1
                except:
                    self.blockchain_stat["transactions"]["size"]["map"]["amount"][size_key] = 1

                t_list = []
                if tx["segwit"]:  t_list.append("segwit")
                if bip69:  t_list.append("bip69")
                if rbf:  t_list.append("rbf")

                for ttp in t_list:
                    try:
                        block_stat["transactions"]["type"]["map"]["count"][ttp] += 1
                    except:
                        block_stat["transactions"]["type"]["map"]["count"][ttp] = 1
                    try:
                        self.blockchain_stat["transactions"]["type"]["map"]["count"][ttp] += 1
                    except:
                        self.blockchain_stat["transactions"]["type"]["map"]["count"][ttp] = 1

                    try:
                        block_stat["transactions"]["type"]["map"]["amount"][ttp] += amount
                    except:
                        block_stat["transactions"]["type"]["map"]["amount"][ttp] = amount
                    try:
                        self.blockchain_stat["transactions"]["type"]["map"]["amount"][ttp] += amount
                    except:
                        self.blockchain_stat["transactions"]["type"]["map"]["amount"][ttp] = amount

                    try:
                        block_stat["transactions"]["type"]["map"]["size"][ttp] += size
                    except:
                        block_stat["transactions"]["type"]["map"]["size"][ttp] = size
                    try:
                        self.blockchain_stat["transactions"]["type"]["map"]["size"][ttp] += size
                    except:
                        self.blockchain_stat["transactions"]["type"]["map"]["size"][ttp] = size

                # fee
                if block_stat["transactions"]["fee"]["min"]["pointer"] == 0 or \
                        block_stat["transactions"]["fee"]["min"]["value"] > fee:
                    block_stat["transactions"]["fee"]["min"]["pointer"] = pointer
                    block_stat["transactions"]["fee"]["min"]["value"] = fee
                if self.blockchain_stat["transactions"]["fee"]["min"]["pointer"] == 0 or \
                        self.blockchain_stat["transactions"]["fee"]["min"]["value"] > fee:
                    self.blockchain_stat["transactions"]["fee"]["min"]["pointer"] = pointer
                    self.blockchain_stat["transactions"]["fee"]["min"]["value"] = fee

                if block_stat["transactions"]["fee"]["max"]["value"] < fee:
                    block_stat["transactions"]["fee"]["max"]["pointer"] = pointer
                    block_stat["transactions"]["fee"]["max"]["value"] = fee
                if self.blockchain_stat["transactions"]["fee"]["max"]["value"] < fee:
                    self.blockchain_stat["transactions"]["fee"]["max"]["pointer"] = pointer
                    self.blockchain_stat["transactions"]["fee"]["max"]["value"] = fee

                block_stat["transactions"]["fee"]["total"] += fee
                self.blockchain_stat["transactions"]["fee"]["total"] += fee

                # fee_rate
                if block_stat["transactions"]["feeRate"]["min"]["pointer"] == 0 or \
                        block_stat["transactions"]["feeRate"]["min"]["value"] > fee_rate:
                    block_stat["transactions"]["feeRate"]["min"]["pointer"] = pointer
                    block_stat["transactions"]["feeRate"]["min"]["value"] = fee_rate
                if block_stat["transactions"]["feeRate"]["max"]["value"] < fee_rate:
                    block_stat["transactions"]["feeRate"]["max"]["pointer"] = pointer
                    block_stat["transactions"]["feeRate"]["max"]["value"] = fee_rate
                block_stat["transactions"]["feeRate"]["total"] += fee_rate

                try:
                    block_stat["transactions"]["feeRate"]["map"]["amount"][fee_rate_key] += amount
                except:
                    block_stat["transactions"]["feeRate"]["map"]["amount"][fee_rate_key] = amount

                try:
                    block_stat["transactions"]["feeRate"]["map"]["count"][fee_rate_key] += 1
                except:
                    block_stat["transactions"]["feeRate"]["map"]["count"][fee_rate_key] = 1

                try:
                    block_stat["transactions"]["feeRate"]["map"]["size"][fee_rate_key] += tx["size"]
                except:
                    block_stat["transactions"]["feeRate"]["map"]["size"][fee_rate_key] = tx["size"]

                # v_fee_rate_key

                if block_stat["transactions"]["vFeeRate"]["min"]["pointer"] == 0 or \
                        block_stat["transactions"]["vFeeRate"]["min"]["value"] > v_fee_rate_key:
                    block_stat["transactions"]["vFeeRate"]["min"]["pointer"] = pointer
                    block_stat["transactions"]["vFeeRate"]["min"]["value"] = v_fee_rate_key
                if block_stat["transactions"]["vFeeRate"]["max"]["value"] < v_fee_rate_key:
                    block_stat["transactions"]["vFeeRate"]["max"]["pointer"] = pointer
                    block_stat["transactions"]["vFeeRate"]["max"]["value"] = v_fee_rate_key
                block_stat["transactions"]["vFeeRate"]["total"] += v_fee_rate_key

                try:
                    block_stat["transactions"]["vFeeRate"]["map"]["amount"][v_fee_rate_key] += amount
                except:
                    block_stat["transactions"]["vFeeRate"]["map"]["amount"][v_fee_rate_key] = amount

                try:
                    block_stat["transactions"]["vFeeRate"]["map"]["count"][v_fee_rate_key] += 1
                except:
                    block_stat["transactions"]["vFeeRate"]["map"]["count"][v_fee_rate_key] = 1

                try:
                    block_stat["transactions"]["vFeeRate"]["map"]["size"][v_fee_rate_key] += tx["size"]
                except:
                    block_stat["transactions"]["vFeeRate"]["map"]["size"][v_fee_rate_key] = tx["size"]


            self.blocks_stat.append((block["height"], json.dumps(block_stat),
                                     json.dumps(self.blockchain_stat)))


            if len(self.blocks_stat) >= self.tx_batch_limit:

                self.blocks_stat_batches[block["height"]] = self.blocks_stat
                self.blocks_stat = deque()


        except:
            self.log.critical(str(traceback.format_exc()))
            self.log.critical("")


    async def save_to_db(self):
        try:
            self.log.debug("analytica save processor started")
            self.db_pool = await asyncpg.create_pool(self.psql_dsn, min_size=1, max_size=1)
            while True:
                q = time.time()
                async with self.db_pool.acquire() as conn:
                    async with conn.transaction():
                        if  self.blocks_stat_batches:
                            height, h_batch = self.blocks_stat_batches.pop()

                            await conn.copy_records_to_table('blocks_stat',
                                                             columns=["height", "block", "blockchain"],
                                                             records=h_batch)


                            self.tx_map_table_checkpoint = height
                            print("stat", height)
                        else:
                            await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.log.debug("save_to_db process canceled")
        except:
            self.log.critical("save_to_db process failed; terminate server ...")
            self.log.critical(str(traceback.format_exc()))
            self.loop.create_task(self.terminate_coroutine())



    async def message_loop(self):
        try:
            self.reader = await get_pipe_reader(self.in_reader, self.loop)
            self.writer = await get_pipe_writer(self.out_writer, self.loop)
            mf = self.loop.create_task(pipe_get_msg(self.reader))
            msg_type = None
            while True:
                if mf.done():
                    msg_type, msg = mf.result()
                    mf = self.loop.create_task(pipe_get_msg(self.reader))
                # msg_type, msg = await pipe_get_msg(self.reader)
                if msg_type == b'block':
                    # pipe_sent_msg(b"block", msg, self.tx_and_headers_subworker_writer)
                    self.process_block(msg)
                    # await self.tx_and_headers_subworker_writer.drain()
                    msg_type = None
                elif msg_type == b'shutdown':
                    await self.terminate_coroutine()
                    msg_type = None
                elif msg_type == b'flush':
                    self.shutdown = True
                    pipe_sent_msg(b"flush", msg, self.tx_and_headers_subworker_writer)
                    h = pickle.loads(msg)
                    await self.tx_and_headers_subworker_writer.drain()
                    if self.headers:
                        self.tx_map_batches[h] = deque(self.tx_map)
                        self.headers_batches[h] = deque(self.headers)
                    while self.tx_map_table_checkpoint != h:
                        self.log.debug("Transaction map table last checkpoint %s -> [%s]" % (self.tx_map_table_checkpoint,
                                                                                             h))
                        await asyncio.sleep(5)

                    while self.tx_and_headers_tables_checkpoint != self.tx_map_table_checkpoint:
                        self.log.debug(
                            "Transaction map table last checkpoint %s; "
                            "Transaction checkpoint %s " % (self.tx_map_table_checkpoint,
                                                            self.tx_and_headers_tables_checkpoint))
                        await asyncio.sleep(2)
                    pipe_sent_msg(b'checkpoint', pickle.dumps(self.checkpoint), self.writer)
                    msg_type = None
                else:
                    await mf


        except asyncio.CancelledError:
            self.log.debug("message loop  canceled")
        except:
            if not self.shutdown:
                self.log.debug("broken pipe; terminate server ...")
                print(traceback.format_exc())
                self.loop.create_task(self.terminate_coroutine())


    async def terminate_coroutine(self):
        sys.excepthook = self._exc
        self.shutdown = True
        self.log.warning('sync worker stop request received')
        [task.cancel() for task in self.tasks]
        [process.terminate() for process in self.processes]
        if self.tasks: await asyncio.wait(self.tasks)

        try: await self.db_pool.close()
        except: pass

        self.log.warning("sync worker stopped")
        self.loop.stop()

    def _exc(self, a, b, c):
        return

    def terminate(self, a, b):
        if not self.shutdown:
            self.shutdown = True
            self.loop.create_task(self.terminate_coroutine())
