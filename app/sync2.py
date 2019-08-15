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

import _pickle as pickle
from pybtc import MRU, rh2s, merkle_tree, merkle_proof
from pybtc import int_to_c_int, s2rh
from utils import *

class SynchronizationWorker:

    def __init__(self, in_reader, in_writer, out_reader,
                 out_writer, psql_dsn, start_blocks, option_merkle_proof, option_timeline, option_blockchain_analytica):
        setproctitle('btcapi: sync worker')
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
                        print(height, batch )
                    async with self.db_pool.acquire() as conn:
                        await conn.copy_records_to_table('transaction_map',
                                                         columns=["pointer", "address", "amount"],
                                                         records=batch)

                    height, h_batch = self.headers_batches.pop()
                    if not isinstance(height, int):
                        print(height, batch )
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
        in_reader, out_reader  = os.fdopen(in_reader,'rb'), os.fdopen(out_reader,'rb')
        in_writer, out_writer  = os.fdopen(in_writer,'wb'), os.fdopen(out_writer,'wb')

        # create new process
        self.tx_and_headers_subworker = Process(target=TxHeadersSubworker,
                                                args=(in_reader, in_writer,
                                                      out_reader, out_writer,
                                                      self.psql_dsn, self.start_blocks,
                                                      self.option_merkle_proof))
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


class TxHeadersSubworker:

    def __init__(self, in_reader, in_writer, out_reader,
                 out_writer, psql_dsn, start_blocks, option_merkle_proof):
        setproctitle('btcapi: sync subworker')
        in_writer.close()
        out_reader.close()
        self.out_writer = out_writer
        self.in_reader = in_reader
        self.start_blocks = start_blocks
        self.option_merkle_proof = option_merkle_proof
        self.psql_dsn = psql_dsn
        self.tx_batch_limit = 5000

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
            if block["height"] > self.start_blocks["transaction_start_block"]:

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
                                inputs_append(tx["vIn"][i]["coin"][2])
                                inputs_append(int_to_c_int(tx["vIn"][i]["coin"][1]))
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
                transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction", "inputs_data", "merkle_proof"]
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
                                                                round(time.time()-q, 4)))
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
                if msg_type ==  b'pipe_read_error':
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

        try: await self.db_pool.close()
        except: pass

        self.log.warning("sync sub worker stopped")
        self.loop.stop()


    def terminate(self,a,b):
        sys.exit(0)


if self.blockchain_analytica:

    bip69 = True
    rbf = False
    hp = None
    op = None
    oa = None
    pks = None
    inputs_amount = 0
    self.block_map_timestamp[block["height"]] = self.block_best_timestamp
    date = datetime.datetime.fromtimestamp(self.block_best_timestamp, datetime.timezone.utc)

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
        "tx_count_total": 0,
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
                #
                amount = inp["coin"][1]
                inputs_amount += amount

                #         # count
                self.blockchain_stat["ict"] += 1
                #         block_stat["inputs"]["count"]["total"] += 1
                #
                #         # amount
                self.blockchain_stat["iat"] += 1
                #         block_stat["inputs"]["amount"]["total"] += amount
                #
                #         # amount minimum
                #         if block_stat["inputs"]["amount"]["min"]["pointer"] == 0 or \
                #                 block_stat["inputs"]["amount"]["min"]["value"] > amount:
                #             block_stat["inputs"]["amount"]["min"]["pointer"] = pointer
                #             block_stat["inputs"]["amount"]["min"]["value"] = amount
                if self.blockchain_stat["iaminp"] == 0 or \
                        self.blockchain_stat["iaminv"] > amount:
                    self.blockchain_stat["iaminp"] = pointer
                    self.blockchain_stat["iaminv"] = amount
                #
                #         # amount maximum
                #         if block_stat["inputs"]["amount"]["max"]["value"] < amount:
                #             block_stat["inputs"]["amount"]["max"]["pointer"] = pointer
                #             block_stat["inputs"]["amount"]["max"]["value"] = amount
                if self.blockchain_stat["iamaxv"] < amount:
                    self.blockchain_stat["iamaxp"] = pointer
                    self.blockchain_stat["iamaxv"] = amount
                #
                amount_key = str(floor(log10(amount))) if amount else "null"
                #
                #         # amount map count
                #         try:
                #             block_stat["inputs"]["amount"]["map"]["count"][amount_key] += 1
                #         except:
                #             block_stat["inputs"]["amount"]["map"]["count"][amount_key] = 1
                try:
                    self.blockchain_stat["iamapc"][amount_key] += 1
                except:
                    self.blockchain_stat["iamapc"][amount_key] = 1
                #
                #         # amount map amount
                #         try:
                #             block_stat["inputs"]["amount"]["map"]["amount"][amount_key] += amount
                #         except:
                #             block_stat["inputs"]["amount"]["map"]["amount"][amount_key] = amount
                try:
                    self.blockchain_stat["iamapa"][amount_key] += amount
                except:
                    self.blockchain_stat["iamapa"][amount_key] = amount
                #
                #
                #
                #         age = (self.block_best_timestamp - self.block_map_timestamp[inp["coin"][0]>>39]) // 86400
                type = inp["coin"][2][0]
                #
                #         coin_days = age * amount
                #         coin_days_key = str(floor(log10(coin_days))) if coin_days else "null"
                #
                #         # type map count
                #
                #         try:
                #             block_stat["inputs"]["type"]["map"]["count"][type] += 1
                #         except:
                #             block_stat["inputs"]["type"]["map"]["count"][type] = 1
                try:
                    self.blockchain_stat["itypemapc"][type] += 1
                except:
                    self.blockchain_stat["itypemapc"][type] = 1
                #
                #
                #         # type map amount
                #         try:
                #             block_stat["inputs"]["type"]["map"]["amount"][type] += amount
                #         except:
                #             block_stat["inputs"]["type"]["map"]["amount"][type] = amount
                try:
                    self.blockchain_stat["itypemapa"][type] += amount
                except:
                    self.blockchain_stat["itypemapa"][type] = amount
                #
                #         # type map size
                #         try:
                #             block_stat["inputs"]["type"]["map"]["size"][type] += tx["size"]
                #         except:
                #             block_stat["inputs"]["type"]["map"]["size"][type] = tx["size"]
                try:
                    self.blockchain_stat["itypemaps"][type] += tx["size"]
                except:
                    self.blockchain_stat["itypemaps"][type] = tx["size"]
                #
                #         # coindays destroyed (only block stat)
                #
                #         block_stat["inputs"]["destroyed"]["count"]["total"] += 1
                #         block_stat["inputs"]["destroyed"]["value"]["total"] += coin_days
                #
                #         if block_stat["inputs"]["destroyed"]["value"]["min"]["pointer"] == 0 or \
                #                 block_stat["inputs"]["destroyed"]["value"]["min"]["value"] > coin_days:
                #             block_stat["inputs"]["destroyed"]["value"]["min"]["pointer"] = pointer
                #             block_stat["inputs"]["destroyed"]["value"]["min"]["pointer"] = coin_days
                #         if block_stat["inputs"]["destroyed"]["value"]["max"]["value"] < coin_days:
                #             block_stat["inputs"]["destroyed"]["value"]["max"]["pointer"] = pointer
                #             block_stat["inputs"]["destroyed"]["value"]["max"]["value"] = coin_days
                #
                #
                #         try:
                #             block_stat["inputs"]["destroyed"]["map"]["count"][coin_days_key] += 1
                #         except:
                #             block_stat["inputs"]["destroyed"]["map"]["count"][coin_days_key] = 1
                #
                #         try:
                #             block_stat["inputs"]["destroyed"]["map"]["amount"][coin_days_key] += amount
                #         except:
                #             block_stat["inputs"]["destroyed"]["map"]["amount"][coin_days_key] = amount
                #
                #         try:
                #             block_stat["inputs"]["destroyed"]["map"]["amount"][coin_days_key] += type
                #         except:
                #             block_stat["inputs"]["destroyed"]["map"]["amount"][coin_days_key] = type
                #
                # type

                if type == 1 or type == 6:
                    # P2SH or P2WSH

                    s = parse_script(inp["coin"][2][1:])
                    st = s["type"]
                    if st == "MULTISIG":
                        st += "_%s/%s" % (s["reqSigs"], s["pubKeys"])
                    #
                    #             try:
                    #                 block_stat["inputs"][tt]["type"]["map"]["count"][st] += 1
                    #             except:
                    #                 block_stat["inputs"][tt]["type"]["map"]["count"][st] = 1

                    try:
                        self.blockchain_stat["iP2SHmapc"][st] += 1
                    except:
                        self.blockchain_stat["iP2SHmapc"][st] = 1
                    #
                    #             try:
                    #                 block_stat["inputs"][tt]["type"]["map"]["amount"][st] += amount
                    #             except:
                    #                 block_stat["inputs"][tt]["type"]["map"]["amount"][st] = amount
                    try:
                        self.blockchain_stat["iP2SHmapa"][st] += amount
                    except:
                        self.blockchain_stat["iP2SHmapa"][st] = amount
                    #
                    #             try:
                    #                 block_stat["inputs"][tt]["type"]["map"]["size"][st] += tx["size"]
                    #             except:
                    #                 block_stat["inputs"][tt]["type"]["map"]["size"][st] = tx["size"]
                    try:
                        self.blockchain_stat["iP2SHmaps"][st] += tx["size"]
                    except:
                        self.blockchain_stat["iP2SHmaps"][st] = tx["size"]
        #
        # self.blockchain_stat["oamapc"][idate.year] -= 1
        # self.blockchain_stat["oamapa"][idate.year] -= amount
        # self.blockchain_stat["outputs"]["amount"]["map"][type][idate.year] -= 1
        #

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
            #     block_stat["outputs"]["count"]["total"] += 1
            self.blockchain_stat["oct"] += 1
            #
            #     block_stat["outputs"]["amount"]["total"] += amount
            self.blockchain_stat["oat"] += amount
            #
            #     if block_stat["outputs"]["amount"]["min"]["pointer"] == 0 or \
            #             block_stat["outputs"]["amount"]["min"]["value"] > amount:
            #         block_stat["outputs"]["amount"]["min"]["pointer"] = pointer
            #         block_stat["outputs"]["amount"]["min"]["value"] = amount
            if self.blockchain_stat["oaminp"] == 0 or \
                    self.blockchain_stat["oaminv"] > amount:
                self.blockchain_stat["oaminp"] = pointer
                self.blockchain_stat["oaminv"] = amount
            #
            #     if block_stat["outputs"]["amount"]["max"]["value"] < amount:
            #         block_stat["outputs"]["amount"]["max"]["pointer"] = pointer
            #         block_stat["outputs"]["amount"]["max"]["value"] = amount
            if self.blockchain_stat["oamaxv"] < amount:
                self.blockchain_stat["oamaxp"] = pointer
                self.blockchain_stat["oamaxv"] = amount
            #
            amount_key = str(floor(log10(amount))) if amount else "null"
            #     try:
            #         block_stat["outputs"]["amount"]["map"]["count"][amount_key] += 1
            #     except:
            #         block_stat["outputs"]["amount"]["map"]["count"][amount_key] = 1
            try:
                self.blockchain_stat["oamapc"][amount_key] += 1
            except:
                self.blockchain_stat["oamapc"][amount_key] = 1
            #
            #     try:
            #         block_stat["outputs"]["amount"]["map"]["amount"][amount_key] += amount
            #     except:
            #         block_stat["outputs"]["amount"]["map"]["amount"][amount_key] = amount
            try:
                self.blockchain_stat["oamapa"][amount_key] += amount
            except:
                self.blockchain_stat["oamapa"][amount_key] = amount

            # age

            try:
                self.blockchain_stat["oagemapc"][date.year] += 1
            except:
                self.blockchain_stat["oagemapc"][date.year] = 1

            try:
                self.blockchain_stat["oagemapa"][date.year] += amount
            except:
                self.blockchain_stat["oagemapa"][date.year] = amount

            try:
                self.blockchain_stat["oagemaptype"][out["nType"]]
            except:
                self.blockchain_stat["oagemaptype"][out["nType"]] = dict()
            try:
                self.blockchain_stat["oagemaptype"][out["nType"]][date.year] += 1
            except:
                self.blockchain_stat["oagemaptype"][out["nType"]][date.year] = 1

        # transaction

        pointer = (block["height"] << 19) + t
        amount = tx["amount"]
        amount_key = str(floor(log10(amount))) if amount else "null"

        # block_stat["tx_count_total"] += 1
        self.blockchain_stat["tct"] += 1

        # block_stat["transactions"]["amount"]["total"] += amount
        self.blockchain_stat["tat"] += amount

        # if block_stat["transactions"]["amount"]["min"]["pointer"] == 0 or \
        #         block_stat["transactions"]["amount"]["min"]["value"] > amount:
        #     block_stat["transactions"]["amount"]["min"]["pointer"] = pointer
        #     block_stat["transactions"]["amount"]["min"]["value"] = amount
        if self.blockchain_stat["taminp"] == 0 or \
                self.blockchain_stat["taminv"] > amount:
            self.blockchain_stat["taminp"] = pointer
            self.blockchain_stat["taminv"] = amount

        # if block_stat["transactions"]["amount"]["max"]["value"] < amount:
        #     block_stat["transactions"]["amount"]["max"]["pointer"] = pointer
        #     block_stat["transactions"]["amount"]["max"]["value"] = amount
        if self.blockchain_stat["tamaxv"] < amount:
            self.blockchain_stat["tamaxp"] = pointer
            self.blockchain_stat["tamaxv"] = amount

        # try:
        #     block_stat["transactions"]["amount"]["map"]["amount"][amount_key] += amount
        # except:
        #     block_stat["transactions"]["amount"]["map"]["amount"][amount_key] = amount
        try:
            self.blockchain_stat["tamapa"][amount_key] += amount
        except:
            self.blockchain_stat["tamapa"][amount_key] = amount

        # try:
        #     block_stat["transactions"]["amount"]["map"]["count"][amount_key] += 1
        # except:
        #     block_stat["transactions"]["amount"]["map"]["count"][amount_key] = 1
        try:
            self.blockchain_stat["tamapc"][amount_key] += 1
        except:
            self.blockchain_stat["tamapc"][amount_key] = 1

        # try:
        #     block_stat["transactions"]["amount"]["map"]["size"][amount_key] += tx["size"]
        # except:
        #     block_stat["transactions"]["amount"]["map"]["size"][amount_key] = tx["size"]
        try:
            self.blockchain_stat["tamaps"][amount_key] += tx["size"]
        except:
            self.blockchain_stat["tamaps"][amount_key] = tx["size"]

        fee = inputs_amount - amount
        fee_rate = int((fee / tx["size"]) * 100)
        v_fee_rate = int((fee / tx["vSize"]) * 100)
        fee_rate_key = int(floor(fee_rate / 10))
        v_fee_rate_key = int(floor(v_fee_rate / 10))
        size = tx["size"]
        # block_stat["transactions"]["size"]["total"]["size"] += size
        self.blockchain_stat["ttsize"] += size
        # block_stat["transactions"]["size"]["total"]["vSize"] += tx["vSize"]
        self.blockchain_stat["ttbSize"] += tx["vSize"]
        # block_stat["transactions"]["size"]["total"]["bSize"] += tx["bSize"]
        self.blockchain_stat["ttvSize"] += tx["bSize"]

        # if block_stat["transactions"]["size"]["min"]["pointer"] == 0 or \
        #         block_stat["transactions"]["size"]["min"]["value"] > size:
        #     block_stat["transactions"]["size"]["min"]["pointer"] = pointer
        #     block_stat["transactions"]["size"]["min"]["value"] = size
        if self.blockchain_stat["tsminp"] == 0 or \
                self.blockchain_stat["tsminv"] > size:
            self.blockchain_stat["tsminp"] = pointer
            self.blockchain_stat["tsminv"] = size

        # if block_stat["transactions"]["size"]["max"]["value"] < size:
        #     block_stat["transactions"]["size"]["max"]["pointer"] = pointer
        #     block_stat["transactions"]["size"]["max"]["value"] = size
        if self.blockchain_stat["tsmaxv"] < size:
            self.blockchain_stat["tsmaxp"] = pointer
            self.blockchain_stat["tsmaxv"] = size

        if size < 1000:
            size_key = str(floor(size / 100))
        else:
            size_key = "%sK" % floor(size / 1000)

        # try:
        #     block_stat["transactions"]["size"]["map"]["count"][size_key] += 1
        # except:
        #     block_stat["transactions"]["size"]["map"]["count"][size_key] = 1
        try:
            self.blockchain_stat["tsmapc"][size_key] += 1
        except:
            self.blockchain_stat["tsmapc"][size_key] = 1

        # try:
        #     block_stat["transactions"]["size"]["map"]["amount"][size_key] += 1
        # except:
        #     block_stat["transactions"]["size"]["map"]["amount"][size_key] = 1
        try:
            self.blockchain_stat["tsmapa"][size_key] += 1
        except:
            self.blockchain_stat["tsmapa"][size_key] = 1

        t_list = []
        if tx["segwit"]:  t_list.append("segwit")
        if bip69:  t_list.append("bip69")
        if rbf:  t_list.append("rbf")

        for ttp in t_list:
            #     try:
            #         block_stat["transactions"]["type"]["map"]["count"][ttp] += 1
            #     except:
            #         block_stat["transactions"]["type"]["map"]["count"][ttp] = 1
            try:
                self.blockchain_stat["ttypemapc"][ttp] += 1
            except:
                self.blockchain_stat["ttypemapc"][ttp] = 1
            #
            #     try:
            #         block_stat["transactions"]["type"]["map"]["amount"][ttp] += amount
            #     except:
            #         block_stat["transactions"]["type"]["map"]["amount"][ttp] = amount
            try:
                self.blockchain_stat["ttypemapa"][ttp] += amount
            except:
                self.blockchain_stat["ttypemapa"][ttp] = amount
            #
            #     try:
            #         block_stat["transactions"]["type"]["map"]["size"][ttp] += size
            #     except:
            #         block_stat["transactions"]["type"]["map"]["size"][ttp] = size
            try:
                self.blockchain_stat["ttypemaps"][ttp] += size
            except:
                self.blockchain_stat["ttypemaps"][ttp] = size

        # fee
        # if block_stat["transactions"]["fee"]["min"]["pointer"] == 0 or \
        #         block_stat["transactions"]["fee"]["min"]["value"] > fee:
        #     block_stat["transactions"]["fee"]["min"]["pointer"] = pointer
        #     block_stat["transactions"]["fee"]["min"]["value"] = fee
        if self.blockchain_stat["tfeeminp"] == 0 or \
                self.blockchain_stat["tfeeminv"] > fee:
            self.blockchain_stat["tfeeminp"] = pointer
            self.blockchain_stat["tfeeminv"] = fee
        #
        # if block_stat["transactions"]["fee"]["max"]["value"] < fee:
        #     block_stat["transactions"]["fee"]["max"]["pointer"] = pointer
        #     block_stat["transactions"]["fee"]["max"]["value"] = fee
        if self.blockchain_stat["tfeemaxv"] < fee:
            self.blockchain_stat["tfeemaxp"] = pointer
            self.blockchain_stat["tfeemaxv"] = fee

        # block_stat["transactions"]["fee"]["total"] += fee
        self.blockchain_stat["txfeet"] += fee

        # fee_rate
        # if block_stat["transactions"]["feeRate"]["min"]["pointer"] == 0 or \
        #         block_stat["transactions"]["feeRate"]["min"]["value"] > fee_rate:
        #     block_stat["transactions"]["feeRate"]["min"]["pointer"] = pointer
        #     block_stat["transactions"]["feeRate"]["min"]["value"] = fee_rate
        # if block_stat["transactions"]["feeRate"]["max"]["value"] < fee_rate:
        #     block_stat["transactions"]["feeRate"]["max"]["pointer"] = pointer
        #     block_stat["transactions"]["feeRate"]["max"]["value"] = fee_rate
        # block_stat["transactions"]["feeRate"]["total"] += fee_rate

        # try:
        #     block_stat["transactions"]["feeRate"]["map"]["amount"][fee_rate_key] += amount
        # except:
        #     block_stat["transactions"]["feeRate"]["map"]["amount"][fee_rate_key] = amount
        #
        # try:
        #     block_stat["transactions"]["feeRate"]["map"]["count"][fee_rate_key] += 1
        # except:
        #     block_stat["transactions"]["feeRate"]["map"]["count"][fee_rate_key] = 1
        #
        # try:
        #     block_stat["transactions"]["feeRate"]["map"]["size"][fee_rate_key] += tx["size"]
        # except:
        #     block_stat["transactions"]["feeRate"]["map"]["size"][fee_rate_key] = tx["size"]

        # v_fee_rate_key

        # if block_stat["transactions"]["vFeeRate"]["min"]["pointer"] == 0 or \
        #         block_stat["transactions"]["vFeeRate"]["min"]["value"] > v_fee_rate_key:
        #     block_stat["transactions"]["vFeeRate"]["min"]["pointer"] = pointer
        #     block_stat["transactions"]["vFeeRate"]["min"]["value"] = v_fee_rate_key
        # if block_stat["transactions"]["vFeeRate"]["max"]["value"] < v_fee_rate_key:
        #     block_stat["transactions"]["vFeeRate"]["max"]["pointer"] = pointer
        #     block_stat["transactions"]["vFeeRate"]["max"]["value"] = v_fee_rate_key
        # block_stat["tx_vFeeRate_total"] += v_fee_rate_key

        # try:
        #     block_stat["tx_vFeeRate_map_amount"][v_fee_rate_key] += amount
        # except:
        #     block_stat["tx_vFeeRate_map_amount"][v_fee_rate_key] = amount
        #
        # try:
        #     block_stat["tx_vFeeRate_map_count"][v_fee_rate_key] += 1
        # except:
        #     block_stat["tx_vFeeRate_map_count"][v_fee_rate_key] = 1
        #
        # try:
        #     block_stat["tx_vFeeRate_map_size"][v_fee_rate_key] += tx["size"]
        # except:
        #     block_stat["tx_vFeeRate_map_count"][v_fee_rate_key] = tx["size"]

    self.blocks_stat.append((block["height"], pickle.dumps(block_stat),
                             pickle.dumps(self.blockchain_stat)))


