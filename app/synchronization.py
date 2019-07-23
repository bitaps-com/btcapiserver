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
from math import floor, log10

import _pickle as pickle
from pybtc import MRU, rh2s, merkle_tree, merkle_proof, parse_script
from pybtc import int_to_c_int, s2rh
from utils import pipe_sent_msg, get_pipe_reader, get_pipe_writer, pipe_get_msg


class SynchronizationWorker:

    def __init__(self, in_reader, in_writer, out_reader,
                 out_writer, psql_dsn, start_blocks, option_merkle_proof,
                 option_timeline, option_blockchain_analytica):
        setproctitle('btcapi engine sync worker')
        in_writer.close()
        out_reader.close()
        self.out_writer = out_writer
        self.in_reader = in_reader
        self.psql_dsn = psql_dsn
        self.shutdown = False
        self.option_timeline = option_timeline
        self.option_merkle_proof = option_merkle_proof
        self.option_blockchain_analytica = option_blockchain_analytica
        self.start_blocks = start_blocks
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
        self.tx_map_batches = MRU()
        self.block_map_timestamp = dict()
        self.headers = deque()
        self.headers_batches = MRU()
        self.block_best_timestamp = 0
        self.checkpoint = 0
        self.tx_map_table_checkpoint = 0
        self.tx_and_headers_tables_checkpoint = 0
        self.shutdown = False

        self.tx_and_headers_subworker = None
        self.tx_and_headers_subworker_writer = None
        self.tx_and_headers_subworker_reader = None

        signal.signal(signal.SIGTERM, self.terminate)

        self.processes = []
        self.tasks = [self.loop.create_task(self.handle_tx_and_headers()),
                      self.loop.create_task(self.save_to_db()),
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
            await asyncio.sleep(2)

    async def process_block(self, block):
        try:
            pipe_sent_msg(b"block", block, self.tx_and_headers_subworker_writer)

            block = pickle.loads(block)
            if block["height"] > self.start_blocks["transaction_map_start_block"]:
                if self.block_best_timestamp < block["time"]:
                    self.block_best_timestamp = block["time"]

                for t in block["rawTx"]:
                    tx = block["rawTx"][t]
                    # get inputs
                    if not tx["coinbase"]:
                        for i in tx["vIn"]:
                            inp = tx["vIn"][i]
                            if inp["coin"][2][0] in (7, 8, 3, 4): continue
                            pointer = (block["height"] << 39) + (t << 20) + (0 << 19) + i
                            self.tx_map.append((pointer, inp["coin"][2], inp["coin"][1]))




                    # prepare outputs
                    for i in tx["vOut"]:
                        out = tx["vOut"][i]
                        if out["nType"] in (7, 8, 3, 4): continue
                        pointer = (block["height"] << 39) + (t << 20) + (1 << 19) + i

                        if "addressHash" not in out:
                            address = b"".join((bytes([out["nType"]]), out["scriptPubKey"]))
                        else:
                            address = b"".join((bytes([out["nType"]]), out["addressHash"]))

                        self.tx_map.append((pointer, address, out["value"]))


                if self.option_timeline:
                    self.headers.append((block["height"], s2rh(block["hash"]),
                                         block["header"], block["time"], self.block_best_timestamp))
                else:
                    self.headers.append((block["height"], s2rh(block["hash"]), block["header"], block["time"]))

                if len(self.tx_map) >= self.tx_batch_limit:
                    self.tx_map_batches[block["height"]] = deque(self.tx_map)
                    self.tx_map = deque()

                    self.headers_batches[block["height"]] = deque(self.headers)
                    self.headers = deque()

        except:
            self.log.critical(str(traceback.format_exc()))
            self.log.critical("")

    async def save_to_db(self):
        try:
            self.log.debug("transaction map save processor started")
            self.db_pool = await asyncpg.create_pool(self.psql_dsn, min_size=1, max_size=1)
            while True:
                q = time.time()
                if self.tx_map_batches:
                    height, batch = self.tx_map_batches.pop()
                    if not isinstance(height, int):
                        print(height, batch)
                    async with self.db_pool.acquire() as conn:
                        await conn.copy_records_to_table('transaction_map',
                                                         columns=["pointer", "address", "amount"],
                                                         records=batch)

                    height, h_batch = self.headers_batches.pop()
                    if not isinstance(height, int):
                        print(height, batch)
                    async with self.db_pool.acquire() as conn:
                        if self.option_timeline:
                            await conn.copy_records_to_table('blocks',
                                                             columns=["height", "hash",
                                                                      "header", "timestamp",
                                                                      "adjusted_timestamp"],
                                                             records=h_batch)
                        else:
                            await conn.copy_records_to_table('blocks',
                                                             columns=["height", "hash",
                                                                      "header", "timestamp"],
                                                             records=h_batch)

                    self.tx_map_table_checkpoint = height
                    if height % 1000 == 0:
                        self.log.debug("transaction map batch %s; height %s; time %s;" % (len(batch),
                                                                                          height,
                                                                                          round(time.time() - q, 4)))
                else:
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.log.debug("save_to_db process canceled")
        except:
            self.log.critical("save_to_db process failed; terminate server ...")
            self.log.critical(str(traceback.format_exc()))
            self.loop.create_task(self.terminate_coroutine())

    async def handle_tx_and_headers(self):
        self.log.info('start transaction and header tables subworker')
        # prepare pipes for communications
        in_reader, in_writer = os.pipe()
        out_reader, out_writer = os.pipe()
        in_reader, out_reader = os.fdopen(in_reader, 'rb'), os.fdopen(out_reader, 'rb')
        in_writer, out_writer = os.fdopen(in_writer, 'wb'), os.fdopen(out_writer, 'wb')

        # create new process
        self.tx_and_headers_subworker = Process(target=TxHeadersSubworker,
                                                args=(in_reader, in_writer,
                                                      out_reader, out_writer,
                                                      self.psql_dsn, self.start_blocks,
                                                      self.option_merkle_proof,
                                                      self.option_blockchain_analytica))
        self.tx_and_headers_subworker.start()
        in_reader.close()
        out_writer.close()
        # get stream reader
        self.tx_and_headers_subworker_reader = await get_pipe_reader(out_reader, self.loop)
        self.tx_and_headers_subworker_writer = await get_pipe_writer(in_writer, self.loop)

        # start message loop
        self.tasks.append(self.loop.create_task(self.tx_and_headers_message_loop()))
        # wait if process crash
        await self.loop.run_in_executor(None, self.tx_and_headers_subworker.join)
        self.log.warning('transaction and header tables subworker stopped')
        self.tx_and_headers_subworker = None
        self.sync_transaction_worker_writer = None
        self.tx_and_headers_subworker_reader = None

    async def tx_and_headers_message_loop(self):
        try:
            while True:
                msg_type, msg = await pipe_get_msg(self.tx_and_headers_subworker_reader)
                if msg_type == b'checkpoint':
                    self.tx_and_headers_tables_checkpoint = pickle.loads(msg)
        except asyncio.CancelledError:
            self.log.debug("tx and headers message loop canceled")
        except:
            if not self.shutdown:
                self.log.debug("broken pipe; terminate server ...")
                self.loop.create_task(self.terminate_coroutine())

    async def message_loop(self):
        try:
            self.reader = await get_pipe_reader(self.in_reader, self.loop)
            self.writer = await get_pipe_writer(self.out_writer, self.loop)

            while True:
                msg_type, msg = await pipe_get_msg(self.reader)
                if msg_type == b'block':
                    await self.process_block(msg)
                elif msg_type == b'shutdown':
                    await self.terminate_coroutine()
                elif msg_type == b'flush':
                    self.shutdown = True
                    pipe_sent_msg(b"flush", msg, self.tx_and_headers_subworker_writer)
                    h = pickle.loads(msg)
                    await self.tx_and_headers_subworker_writer.drain()
                    if self.headers:
                        self.tx_map_batches[h] = deque(self.tx_map)
                        self.headers_batches[h] = deque(self.headers)
                    while self.tx_map_table_checkpoint != h:
                        self.log.debug(
                            "Transaction map table last checkpoint %s -> [%s]" % (self.tx_map_table_checkpoint,
                                                                                  h))
                        await asyncio.sleep(5)

                    while self.tx_and_headers_tables_checkpoint != self.tx_map_table_checkpoint:
                        self.log.debug(
                            "Transaction map table last checkpoint %s; "
                            "Transaction checkpoint %s " % (self.tx_map_table_checkpoint,
                                                            self.tx_and_headers_tables_checkpoint))
                        await asyncio.sleep(2)
                    pipe_sent_msg(b'checkpoint', pickle.dumps(self.checkpoint), self.writer)



        except asyncio.CancelledError:
            self.log.debug("message loop  canceled")
        except:
            if not self.shutdown:
                self.log.debug("broken pipe; terminate server ...")
                self.loop.create_task(self.terminate_coroutine())

    async def terminate_coroutine(self):
        sys.excepthook = self._exc
        self.shutdown = True
        self.log.warning('sync worker stop request received')
        [task.cancel() for task in self.tasks]
        [process.terminate() for process in self.processes]
        if self.tasks: await asyncio.wait(self.tasks)

        try:
            await self.db_pool.close()
        except:
            pass

        self.log.warning("sync worker stopped")
        self.loop.stop()

    def _exc(self, a, b, c):
        return

    def terminate(self, a, b):
        if not self.shutdown:
            self.shutdown = True
            self.loop.create_task(self.terminate_coroutine())


class TxHeadersSubworker:

    def __init__(self, in_reader, in_writer, out_reader,
                 out_writer, psql_dsn, start_blocks, option_merkle_proof, option_blockchain_analytica):
        setproctitle('btcapi engine sync subworker')
        in_writer.close()
        out_reader.close()
        self.out_writer = out_writer
        self.in_reader = in_reader
        self.start_blocks = start_blocks
        self.option_merkle_proof = option_merkle_proof
        self.option_blockchain_analytica = option_blockchain_analytica
        self.block_map_timestamp = dict()
        self.psql_dsn = psql_dsn
        self.tx_batch_limit = 5000
        self.block_best_timestamp = 0

        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.loop = asyncio.get_event_loop()
        self.log = logging.getLogger("sync2")
        ch = logging.StreamHandler()
        formatter = colorlog.ColoredFormatter('%(log_color)s%(asctime)s %(module)s: %(message)s ')
        formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s %(levelname)s: %(message)s (%(module)s:%(lineno)d)')
        ch.setFormatter(formatter)
        self.log.addHandler(ch)
        self.log.warning("Synchronization subworker started")
        self.log.setLevel(logging.DEBUG)
        self.loop.set_default_executor(ThreadPoolExecutor(5))

        self.checkpoint = 0
        self.transactions = deque()
        self.transactions_batches = MRU()

        self.headers = deque()
        self.headers_batches = MRU()

        signal.signal(signal.SIGTERM, self.terminate)
        self.tasks = [self.loop.create_task(self.message_loop()),
                      self.loop.create_task(self.save_to_db()),
                      self.loop.create_task(self.watchdog())]
        self.loop.run_forever()

    async def watchdog(self):
        last_checkpoint = self.checkpoint
        while True:
            if self.checkpoint != last_checkpoint:
                last_checkpoint = self.checkpoint
                pipe_sent_msg(b'checkpoint', pickle.dumps(self.checkpoint), self.out_writer)
            await asyncio.sleep(2)

    async def process_block(self, block):
        try:
            block = pickle.loads(block)
            block_stat = None
            if self.option_blockchain_analytica:
                block_stat = {
                    "outputs": {"count": {"total": 0},
                                "amount": {"min": {"pointer": 0, "value": 0},
                                           "max": {"pointer": 0, "value": 0},
                                           "total": 0,
                                           "map": {"count": dict(), "amount": dict()}
                                           },
                                "type": {"map": {"count": dict(), "amount": dict(), "size": dict()}},
                                "age": {"map": {"count": dict(), "amount": dict(), "type": dict()}}},

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
                                     "io": {"outputs": {"count": {"map": {"outCount": dict()},
                                                                  "reduce": 0}},
                                            "inputs": {
                                                "count": {"map": {"inputCount": dict()}}}},

                                     "amount": {"min": {"pointer": 0, "value": 0},
                                                "max": {"pointer": 0, "value": 0},
                                                "map": {"count": dict(),
                                                        "amount": dict(),
                                                        "size": dict(),
                                                        "feeRate": dict(),
                                                        "outFeeRate": dict()},
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
                                                 "average": 0,
                                                 "map": {"count": dict(),
                                                         "amount": dict(),
                                                         "size": dict()}}
                                     }
                }
            if block["height"] > self.start_blocks["transaction_start_block"]:
                if self.block_best_timestamp < block["time"]:
                    self.block_best_timestamp = block["time"]

                if self.option_blockchain_analytica:
                    self.block_map_timestamp[block["height"]] = self.block_best_timestamp

                if self.option_merkle_proof:
                    m_tree = merkle_tree(block["rawTx"][i]["txId"] for i in block["rawTx"])
                try:
                    for t in block["rawTx"]:
                        tx = block["rawTx"][t]
                        raw_tx = tx.serialize(hex=False)
                        inputs = []
                        inputs_append = inputs.append
                        if not tx["coinbase"]:
                            for i in tx["vIn"]:
                                inp = tx["vIn"][i]
                                inputs_append(inp["coin"][2])
                                inputs_append(int_to_c_int(inp["coin"][1]))
                                pointer = (block["height"] << 39) + (t << 20) + (0 << 19) + i
                                if self.option_blockchain_analytica:
                                    amount = inp["coin"][1]
                                    block_stat["inputs"]["count"]["total"] += 1
                                    block_stat["inputs"]["amount"]["total"] += amount
                                    if block_stat["inputs"]["amount"]["min"]["pointer"] == 0 or \
                                            block_stat["inputs"]["amount"]["min"]["value"] > amount:
                                        block_stat["inputs"]["amount"]["min"]["pointer"] = pointer
                                        block_stat["inputs"]["amount"]["min"]["value"] = amount
                                    if block_stat["inputs"]["amount"]["max"]["value"] < amount:
                                        block_stat["inputs"]["amount"]["max"]["pointer"] = pointer
                                        block_stat["inputs"]["amount"]["max"]["value"] = amount
                                    amount_key = str(floor(log10(amount))) if amount else "null"
                                    try:
                                        block_stat["inputs"]["amount"]["map"]["count"][amount_key] += 1
                                    except:
                                        block_stat["inputs"]["amount"]["map"]["count"][amount_key] = 1
                                    try:
                                        block_stat["inputs"]["amount"]["map"]["amount"][amount_key] += amount
                                    except:
                                        block_stat["inputs"]["amount"]["map"]["amount"][amount_key] = amount
                                    age = (self.block_best_timestamp - self.block_map_timestamp[inp["coin"][0]>>39]) // 86400
                                    type = inp["coin"][2][0]
                                    coin_days = age * amount
                                    coin_days_key = str(floor(log10(coin_days))) if coin_days else "null"
                                    try:
                                        block_stat["inputs"]["type"]["map"]["count"][type] += 1
                                    except:
                                        block_stat["inputs"]["type"]["map"]["count"][type] = 1
                                    try:
                                        block_stat["inputs"]["type"]["map"]["amount"][type] += amount
                                    except:
                                        block_stat["inputs"]["type"]["map"]["amount"][type] = amount
                                    try:
                                        block_stat["inputs"]["type"]["map"]["size"][type] += tx["size"]
                                    except:
                                        block_stat["inputs"]["type"]["map"]["size"][type] = tx["size"]

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

                                    if type == 1 or type == 6:
                                        # P2SH or P2WSH
                                        tt = "P2SH" if type == 1 else "P2WSH"

                                        s = parse_script(inp["coin"][2][1:])
                                        try:
                                            block_stat["inputs"][tt]["type"]["map"]["count"][s["nType"]] += 1
                                        except:
                                            block_stat["inputs"][tt]["type"]["map"]["count"][s["nType"]] = 1

                                        try:
                                            block_stat["inputs"][tt]["type"]["map"]["amount"][s["nType"]] += amount
                                        except:
                                            block_stat["inputs"][tt]["type"]["map"]["amount"][s["nType"]] = amount

                                        try:
                                            block_stat["inputs"][tt]["type"]["map"]["size"][s["nType"]] += tx["size"]
                                        except:
                                            block_stat["inputs"][tt]["type"]["map"]["size"][s["nType"]] = tx["size"]

                        for i in tx["vOut"]:
                            out = tx["vOut"][i]
                            if out["nType"] in (7, 8, 3, 4): continue
                            pointer = (block["height"] << 39) + (t << 20) + (1 << 19) + i

                            if self.option_blockchain_analytica:
                                amount = out["value"]
                                block_stat["outputs"]["count"]["total"] += 1
                                block_stat["outputs"]["amount"]["total"] += amount
                                if block_stat["outputs"]["amount"]["min"]["pointer"] == 0 or \
                                        block_stat["outputs"]["amount"]["min"]["value"] > amount:
                                    block_stat["outputs"]["amount"]["min"]["pointer"] = pointer
                                    block_stat["outputs"]["amount"]["min"]["value"] = amount
                                if block_stat["outputs"]["amount"]["max"]["value"] < amount:
                                    block_stat["outputs"]["amount"]["max"]["pointer"] = pointer
                                    block_stat["outputs"]["amount"]["max"]["value"] = amount
                                amount_key = str(floor(log10(amount))) if amount else "null"
                                try:
                                    block_stat["outputs"]["amount"]["map"]["count"][amount_key] += 1
                                except:
                                    block_stat["outputs"]["amount"]["map"]["count"][amount_key] = 1
                                try:
                                    block_stat["outputs"]["amount"]["map"]["amount"][amount_key] += amount
                                except:
                                    block_stat["outputs"]["amount"]["map"]["amount"][amount_key] = amount


                        if self.option_merkle_proof:
                            self.transactions.append(((block["height"] << 39) + (t << 20),
                                                      tx["txId"],
                                                      block["time"],
                                                      raw_tx,
                                                      b''.join(inputs),
                                                      b''.join(merkle_proof(m_tree, t, return_hex=False))))
                        else:
                            self.transactions.append(((block["height"] << 39) + (t << 20),
                                                      tx["txId"],
                                                      block["time"],
                                                      raw_tx,
                                                      b''.join(inputs)))



                except:
                    self.log.critical(str(traceback.format_exc()))
                    self.log.critical(rh2s(tx["txId"]))
                    self.log.critical(str(self.start_blocks["transaction_start_block"]))
                if len(self.transactions) >= self.tx_batch_limit:
                    self.transactions_batches[block["height"]] = deque(self.transactions)
                    self.transactions = deque()

        except:
            self.log.critical(str(traceback.format_exc()))

    async def save_to_db(self):
        try:
            self.db_pool = await asyncpg.create_pool(self.psql_dsn, min_size=1, max_size=1)
            if self.option_merkle_proof:
                transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction", "inputs_data",
                                       "merkle_proof"]
            else:
                transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction", "inputs_data"]
            while True:
                q = time.time()
                if self.transactions_batches:
                    height, tx_batch = self.transactions_batches.pop()
                    async with self.db_pool.acquire() as conn:
                        await conn.copy_records_to_table('transaction',
                                                         columns=transaction_columns,
                                                         records=tx_batch)
                    self.checkpoint = height
                    if height % 1000 == 0:
                        self.log.debug("transactions batch %s;  "
                                       "height %s; time %s;" % (len(tx_batch),
                                                                height,
                                                                round(time.time() - q, 4)))
                else:
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.log.debug("tx_and_headers: save_to_db process canceled")
        except:
            self.log.critical("tx_and_headers: save_to_db process failed; terminate server ...")
            self.log.critical(str(traceback.format_exc()))
            self.loop.create_task(self.terminate_coroutine())

    async def message_loop(self):
        try:
            self.reader = await get_pipe_reader(self.in_reader, self.loop)
            self.writer = await get_pipe_writer(self.out_writer, self.loop)

            while True:
                msg_type, msg = await pipe_get_msg(self.reader)
                if msg_type == b'pipe_read_error':
                    return

                if msg_type == b'block':
                    await self.process_block(msg)
                    continue
                elif msg_type == b'flush':
                    h = pickle.loads(msg)
                    self.log.debug("Transaction table records count %s" % len(self.transactions))
                    self.log.debug("Transaction table checkpoint >> %s", self.checkpoint)
                    if self.transactions:
                        self.transactions_batches[h] = deque(self.transactions)
                    while self.checkpoint != h:
                        self.log.debug("Transaction table last checkpoint %s -> [%s]" % (self.checkpoint, h))
                        await asyncio.sleep(2)
                    self.log.debug("Transaction table checkpoint >> %s", self.checkpoint)
                    pipe_sent_msg(b'checkpoint', pickle.dumps(self.checkpoint), self.writer)


        except:
            print(">>>", traceback.format_exc())

    def _exc(self, a, b, c):
        return

    async def terminate_coroutine(self):
        sys.excepthook = self._exc
        self.log.warning('sync sub worker stop request received')
        [task.cancel() for task in self.tasks]
        if self.tasks: await asyncio.wait(self.tasks)

        try:
            await self.db_pool.close()
        except:
            pass

        self.log.warning("sync sub worker stopped")
        self.loop.stop()

    def terminate(self, a, b):
        sys.exit(0)
