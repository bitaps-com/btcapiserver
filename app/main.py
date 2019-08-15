import argparse
import asyncio
import configparser
import logging
import pickle
import signal
import sys
import traceback
from setproctitle import setproctitle
import time
import asyncpg
import colorlog
from multiprocessing import Process
from synchronization import SynchronizationWorker
from modules.address_state import AddressStateSync
import pybtc
from collections import deque
from pybtc import int_to_c_int, MRU
from pybtc import s2rh, rh2s, merkle_tree, merkle_proof, parse_script
import db_model
import os, json, sys
from utils import (pipe_get_msg,
                   pipe_sent_msg,
                   get_pipe_reader,
                   get_pipe_writer,
                   log_level_map,
                   deserialize_address_data,
                   serialize_address_data)
from concurrent.futures import ThreadPoolExecutor
import datetime
from math import *

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class App:
    def __init__(self, loop, logger, connector_logger, config):
        self.loop = loop
        self.log = logger
        self.config = config
        self.blockchain_stat = None
        # options

        self.transaction = True if config["OPTIONS"]["transaction"] == "on" else False
        self.merkle_proof = True if config["OPTIONS"]["merkle_proof"] == "on" else False
        self.address_state = True if config["OPTIONS"]["address_state"] == "on" else False
        self.address_timeline = True if config["OPTIONS"]["address_timeline"] == "on" else False
        self.blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
        self.blocks_data = True if config["OPTIONS"]["blocks_data"] == "on" else False
        self.transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False

        self.block_preload_workers = int(config["SYNCHRONIZATION"]["block_preload_workers"])
        self.block_preload_batch_size_limit = int(config["SYNCHRONIZATION"]["block_preload_batch_size"])
        self.utxo_cache_size = int(config["SYNCHRONIZATION"]["utxo_cache_size"])
        self.psql_dsn = config["POSTGRESQL"]["dsn"]
        self.psql_threads = int(config["POSTGRESQL"]["server_threads"])
        self.chain_tail = []
        self.block_map_timestamp = dict()
        self.shutdown = False
        self.force_shutdown = False
        self.executor = ThreadPoolExecutor(max_workers=5)
        setproctitle('btcapi engine')

        self.headers = deque()
        self.headers_batches = MRU()
        self.batch_limit = 5000
        self.transactions = deque()
        self.transactions_batches = MRU()

        self.tx_map = deque()
        self.tx_map_batches = MRU()
        self.stxo_batches = MRU()
        self.blocks_stat_batches = MRU()
        self.stxo = deque()
        self.blocks_stat = deque()

        self.db_pool = None
        self.rpc = None
        self.connector = None
        self.sync_worker = None
        self.sync_worker_writer = None
        self.sync_worker_reader = None

        self.transaction_map_start_block = 0
        self.block_start_block = 0
        self.start_checkpoint = 0
        self.address_state_block = 0
        self.address_state_rollback = 0
        self.address_state_process = asyncio.Future()
        self.address_state_process.set_result(True)
        self.block_best_timestamp = 0

        self.start_time = int(time.time())
        self.start_block_hashes = []
        self.total_tx = 0
        self.processes = []
        self.tasks = []
        self.log.info("BTCAPI server starting ...")

        self.blockchain_stat = {
                                  "oCountTotal": 0,  # total outputs count
                                  # What is the total quantity of
                                  # coins in bitcoin blockchain?

                                  "oAmountMinPointer": 0,  # output with minimal amount
                                  "oAmountMinValue": 0,  # What is the minimum amount of a coins?

                                  "oAmountMaxPointer": 0,  # output with maximal amount
                                  "oAmountMaxValue": 0,  # What is the maximal amount of a coins?

                                  "oAmountTotal": 0,  # total amount of all outputs

                                  "oAmountMapCount": dict(),  # quantity distribution by amount
                                  # How many coins exceed 1 BTC?

                                  "oAmountMapAmount": dict(),  # amounts distribution by amount
                                  # What is the total amount of all coins
                                  # exceeding 10 BTC?

                                  "oTypeMapCount": dict(),  # quantity distribution by type
                                  # How many P2SH coins?

                                  "oTypeMapAmount": dict(),  # amounts distribution by type
                                  # What is the total amount of
                                  # all P2PKH coins?

                                  "oTypeMapSize": dict(),  # sizes distribution by type
                                  # What is the total size
                                  # of all P2PKH coins?

                                  "oAgeMapCount": dict(),  # distribution of counts by age
                                  # How many coins older then 1 year?

                                  "oAgeMapAmount": dict(),  # distribution of amount by age
                                  # What amount of coins older then 1 month?

                                  "oAgeMapType": dict(),  # distribution of counts by type
                                  # How many P2SH coins older then 1 year?

                                  "iCountTotal": 0,  # total inputs count
                                  # What is the total quantity of
                                  # spent coins in bitcoin blockchain?

                                  "iAmountMinPointer": 0,  # input with minimal amount
                                  "iAmountMinValue": 0,  # What is the smallest coin spent?

                                  "iAmountMaxPointer": 0,  # input with maximal amount
                                  "iAmountMaxValue": 0,  # what is the greatest coin spent?

                                  "iAmountTotal": 0,  # total amount of all inputs
                                    # What is the total amount of
                                    # all spent coins?

                                  "iAmountMapCount": dict(),  # quantity distribution by amount
                                    # How many spent coins exceed 1 BTC?

                                  "iAmountMapAmount": dict(),  # amounts distribution by amount
                                    # What is the total amount of
                                    #  all spent coins exceeding 10 BTC?
                                  "iTypeMapCount": dict(),  # quantity distribution by type
                                    # How many P2SH  spent coins?

                                  "iTypeMapAmount": dict(),  # amounts distribution by type
                                    # What is the total amount
                                    # of all P2PKH spent?

                                  "iTypeMapSize": dict(),  # sizes distribution by type
                                    # What is the total
                                    # size of all P2PKH spent?

                                    # P2SH redeem script statistics
                                  "iP2SHtypeMapCount": dict(),
                                  "iP2SHtypeMapAmount": dict(),
                                  "iP2SHtypeMapSize": dict(),

                                    # P2WSH redeem script statistics
                                  "iP2WSHtypeMapCount": dict(),
                                  "iP2WSHtypeMapAmount": dict(),
                                  "iP2WSHtypeMapSize": dict(),

                                 "txCountTotal": 0,
                                    "txAmountMinPointer": 0,
                                    "txAmountMinValue": 0,
                                    "txAmountMaxPointer": 0,
                                    "txAmountMaxValue": 0,
                                    "txAmountMapCount": dict(),
                                    "txAmountMapAmount": dict(),
                                    "txAmountMapSize": dict(),
                                    "txAmountTotal": 0,

                                    "txSizeMinPointer": 0,
                                    "txSizeMinValue": 0,
                                    "txSizeMaxPointer": 0,
                                    "txSizeMaxValue": 0,
                                    "txSizeTotal": 0,
                                    "txVSizeTotal": 0,
                                    "txBSizeTotal": 0,

                                    "txSizeMapCount": dict(),
                                    "txSizeMapAmount": dict(),

                                    "txTypeMapCount": dict(),
                                    "txTypeMapSize": dict(),
                                    "txTypeMapAmount": dict(),
                                    "feeMinPointer": 0,
                                    "feeMinValue": 0,
                                    "feeMaxPointer": 0,
                                    "feeMaxValue": 0,
                                    "feeTotal": 0}


        # remap SIGINT and SIGTERM
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)

        self.loop.create_task(self.start(config, connector_logger))


    async def start(self, config, connector_logger):
        # init database
        self.log.info("Create/check database model")
        try:
            self.db_pool = await asyncpg.create_pool(dsn=self.psql_dsn, min_size=1, max_size=self.psql_threads)
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    await db_model.create_db_model(self, conn)
            self.log.info("Connecting to bitcoind daemon ...")

            self.connector = pybtc.Connector(config["CONNECTOR"]["rpc"],
                                             config["CONNECTOR"]["zeromq"],
                                             connector_logger,
                                             utxo_data = True,
                                             utxo_cache_size=self.utxo_cache_size,
                                             rpc_batch_limit=100,
                                             db_type="postgresql",
                                             db=self.psql_dsn,
                                             merkle_proof=self.merkle_proof,
                                             tx_map=self.transaction_history,
                                             analytica=self.blockchain_analytica,
                                             mempool_tx=True,
                                             chain_tail=self.chain_tail,
                                             block_timeout=300,
                                             deep_sync_limit=100,
                                             last_block_height=self.start_checkpoint,
                                             block_cache_workers = self.block_preload_workers,
                                             block_batch_handler=self.block_batch_handler,
                                             tx_handler=self.new_transaction_handler,
                                             orphan_handler=self.orphan_block_handler,
                                             flush_app_caches_handler=self.flush_app_caches_handler,
                                             synchronization_completed_handler=self.synchronization_completed_handler,
                                             block_handler=self.new_block_handler,
                                             block_preload_batch_size_limit = self.block_preload_batch_size_limit,
                                             app_proc_title="btcapi engine")
        except asyncio.CancelledError:
            pass
        except Exception as err:
            self.log.warning("Start failed: %s" % err)
            print(traceback.format_exc())
            self.log.warning("Reconnecting ...")
            await asyncio.sleep(3)
            self.loop.create_task(self.start(config, connector_logger))
            return
        try:
            await self.connector.connected
        except Exception as err:
            self.log.critical("Bitcoind connection failed: %s" % str(err))
            self.terminate(None,None)
            return

        self.log.info("Bitcoind connected")
        bootstrap_height = self.connector.node_last_block - 100

        if self.address_state:
            self.processes.append(Process(target=AddressStateSync, args=(self.psql_dsn,
                                                                         bootstrap_height,
                                                                         self.address_timeline,
                                                                         self.blockchain_analytica,
                                                                         self.log)))
            [p.start() for p in self.processes]
            self.tasks.append(self.loop.create_task(self.address_state_processor()))



    async def flush_app_caches_handler(self, height):
        if self.tx_map:
            self.tx_map_batches[height] = self.tx_map
            self.tx_map = deque()
        if self.stxo:
            self.stxo_batches[height] = self.stxo
            self.stxo = deque()

        if self.transaction:
            self.transactions_batches[height] = self.transactions
            self.transactions = deque()

        if self.headers:
            self.headers_batches[height] = self.headers
            self.headers = deque()

        if self.blocks_stat:
            self.blocks_stat_batches[height] = self.blocks_stat
            self.blocks_stat = deque()

        await self.save_batches()



    async def block_batch_handler(self, block):
        try:

            if self.block_best_timestamp < block["time"]:
                self.block_best_timestamp = block["time"]

            if block["height"] > self.start_checkpoint:

                tx_append = self.transactions.append
                for t in block["rawTx"]:
                    raw_tx = block["rawTx"][t]["rawTx"] if self.transaction else None
                    if self.merkle_proof:
                        tx_append(((block["height"] << 39) + (t << 20), block["rawTx"][t]["txId"],
                                   self.block_best_timestamp, raw_tx, block["rawTx"][t]["merkleProof"]))
                    else:
                        tx_append(((block["height"] << 39) + (t << 20),  block["rawTx"][t]["txId"],
                                   self.block_best_timestamp,  raw_tx))

                if self.transaction_history:
                    self.tx_map += block["txMap"]
                    self.stxo += block["stxo"]


                if self.blocks_data:
                    miner =  block["miner"]
                    data = json.dumps({"version": block["version"],
                            "previousBlockHash": block["previousBlockHash"],
                            "merkleRoot": block["merkleRoot"],
                            "bits": block["bits"],
                            "nonce": block["nonce"],
                            "weight": block["weight"],
                            "size": block["size"],
                            "strippedSize": block["strippedSize"],
                            "amount": block["amount"],
                            "target": block["target"],
                            "targetDifficulty": block["targetDifficulty"]
                            })
                    self.headers.append((block["height"], s2rh(block["hash"]),
                                         block["header"], self.block_best_timestamp, miner, data))
                else:
                    self.headers.append((block["height"], s2rh(block["hash"]), block["header"],
                                         self.block_best_timestamp))

                if self.blockchain_analytica:
                    stat = block["stat"]
                    self.blockchain_stat["oCountTotal"] += stat["oCountTotal"]
                    if self.blockchain_stat["oAmountMinPointer"] or \
                           self.blockchain_stat["oAmountMinValue"] > stat["oAmountMinValue"]:
                            self.blockchain_stat["oAmountMinValue"]  = stat["oAmountMinValue"]
                            self.blockchain_stat["oAmountMinPointer"]  = stat["oAmountMinPointer"]
                    if self.blockchain_stat["oAmountMaxValue"] < stat["oAmountMaxValue"]:
                        self.blockchain_stat["oAmountMaxValue"] = stat["oAmountMaxValue"]
                        self.blockchain_stat["oAmountMaxPointer"] = stat["oAmountMaxPointer"]

                    self.blockchain_stat["oAmountTotal"] += stat["oAmountTotal"]

                    for k in stat["oAmountMapCount"]:
                        try: self.blockchain_stat["oAmountMapCount"][k] += stat["oAmountMapCount"][k]
                        except: self.blockchain_stat["oAmountMapCount"][k] = stat["oAmountMapCount"][k]

                    for k in stat["oAmountMapAmount"]:
                        try: self.blockchain_stat["oAmountMapAmount"][k] += stat["oAmountMapAmount"][k]
                        except: self.blockchain_stat["oAmountMapAmount"][k] = stat["oAmountMapAmount"][k]

                    for k in stat["oTypeMapCount"]:
                        try: self.blockchain_stat["oTypeMapCount"][k] += stat["oTypeMapCount"][k]
                        except: self.blockchain_stat["oTypeMapCount"][k] = stat["oTypeMapCount"][k]

                    for k in stat["oTypeMapAmount"]:
                        try: self.blockchain_stat["oTypeMapAmount"][k] += stat["oTypeMapAmount"][k]
                        except: self.blockchain_stat["oTypeMapAmount"][k] = stat["oTypeMapAmount"][k]

                    for k in stat["oTypeMapSize"]:
                        try: self.blockchain_stat["oTypeMapSize"][k] += stat["oTypeMapSize"][k]
                        except: self.blockchain_stat["oTypeMapSize"][k] = stat["oTypeMapSize"][k]

                    self.blockchain_stat["iCountTotal"] += stat["iCountTotal"]
                    self.blockchain_stat["iAmountTotal"] += stat["iAmountTotal"]

                    if self.blockchain_stat["iAmountMinPointer"] or \
                           self.blockchain_stat["iAmountMinValue"] > stat["iAmountMinValue"]:
                            self.blockchain_stat["iAmountMinValue"]  = stat["iAmountMinValue"]
                            self.blockchain_stat["iAmountMinPointer"]  = stat["iAmountMinPointer"]

                    if self.blockchain_stat["iAmountMaxValue"] < stat["iAmountMaxValue"]:
                        self.blockchain_stat["iAmountMaxValue"] = stat["iAmountMaxValue"]
                        self.blockchain_stat["iAmountMaxPointer"] = stat["iAmountMaxPointer"]

                    for k in stat["iAmountMapCount"]:
                        try: self.blockchain_stat["iAmountMapCount"][k] += stat["iAmountMapCount"][k]
                        except: self.blockchain_stat["iAmountMapCount"][k] = stat["iAmountMapCount"][k]

                    for k in stat["iAmountMapAmount"]:
                        try: self.blockchain_stat["iAmountMapAmount"][k] += stat["iAmountMapAmount"][k]
                        except: self.blockchain_stat["iAmountMapAmount"][k] = stat["iAmountMapAmount"][k]

                    for k in stat["iAmountMapAmount"]:
                        try: self.blockchain_stat["iAmountMapAmount"][k] += stat["iAmountMapAmount"][k]
                        except: self.blockchain_stat["iAmountMapAmount"][k] = stat["iAmountMapAmount"][k]

                    for k in stat["iTypeMapAmount"]:
                        try: self.blockchain_stat["iTypeMapAmount"][k] += stat["iTypeMapAmount"][k]
                        except: self.blockchain_stat["iTypeMapAmount"][k] = stat["iTypeMapAmount"][k]

                    for k in stat["iP2SHtypeMapCount"]:
                        try: self.blockchain_stat["iP2SHtypeMapCount"][k] += stat["iP2SHtypeMapCount"][k]
                        except: self.blockchain_stat["iP2SHtypeMapCount"][k] = stat["iP2SHtypeMapCount"][k]

                    for k in stat["iP2SHtypeMapAmount"]:
                        try: self.blockchain_stat["iP2SHtypeMapAmount"][k] += stat["iP2SHtypeMapAmount"][k]
                        except: self.blockchain_stat["iP2SHtypeMapAmount"][k] = stat["iP2SHtypeMapAmount"][k]


                    for k in stat["iP2WSHtypeMapCount"]:
                        try: self.blockchain_stat["iP2WSHtypeMapCount"][k] += stat["iP2WSHtypeMapCount"][k]
                        except: self.blockchain_stat["iP2WSHtypeMapCount"][k] = stat["iP2WSHtypeMapCount"][k]

                    for k in stat["iP2WSHtypeMapAmount"]:
                        try:
                            self.blockchain_stat["iP2WSHtypeMapAmount"][k] += stat["iP2WSHtypeMapAmount"][k]
                        except:
                            self.blockchain_stat["iP2WSHtypeMapAmount"][k] = stat["iP2WSHtypeMapAmount"][k]

                    self.blockchain_stat["txCountTotal"] += stat["txCountTotal"]

                    if self.blockchain_stat["txAmountMinValue"] < stat["txAmountMinValue"]:
                        self.blockchain_stat["txAmountMinValue"] = stat["txAmountMinValue"]
                        self.blockchain_stat["txAmountMinPointer"] = stat["txAmountMinPointer"]


                    if self.blockchain_stat["txAmountMaxValue"] < stat["txAmountMaxValue"]:
                        self.blockchain_stat["txAmountMaxValue"] = stat["txAmountMaxValue"]
                        self.blockchain_stat["txAmountMaxPointer"] = stat["txAmountMaxPointer"]

                    for k in stat["txAmountMapCount"]:
                        try: self.blockchain_stat["txAmountMapCount"][k] += stat["txAmountMapCount"][k]
                        except: self.blockchain_stat["txAmountMapCount"][k] = stat["txAmountMapCount"][k]

                    for k in stat["txAmountMapAmount"]:
                        try: self.blockchain_stat["txAmountMapAmount"][k] += stat["txAmountMapAmount"][k]
                        except: self.blockchain_stat["txAmountMapAmount"][k] = stat["txAmountMapAmount"][k]

                    for k in stat["txAmountMapSize"]:
                        try: self.blockchain_stat["txAmountMapSize"][k] += stat["txAmountMapSize"][k]
                        except: self.blockchain_stat["txAmountMapSize"][k] = stat["txAmountMapSize"][k]

                    self.blockchain_stat["txAmountTotal"] += stat["txAmountTotal"]

                    if self.blockchain_stat["txSizeMinValue"] < stat["txSizeMinValue"]:
                        self.blockchain_stat["txSizeMinValue"] = stat["txSizeMinValue"]
                        self.blockchain_stat["txSizeMinPointer"] = stat["txSizeMinPointer"]

                    if self.blockchain_stat["txSizeMaxValue"] < stat["txSizeMaxValue"]:
                        self.blockchain_stat["txSizeMaxValue"] = stat["txSizeMaxValue"]
                        self.blockchain_stat["txSizeMaxPointer"] = stat["txSizeMaxPointer"]

                    self.blockchain_stat["txSizeTotal"] += stat["txSizeTotal"]
                    self.blockchain_stat["txVSizeTotal"] += stat["txVSizeTotal"]
                    self.blockchain_stat["txBSizeTotal"] += stat["txBSizeTotal"]

                    for k in stat["txSizeMapCount"]:
                        try: self.blockchain_stat["txSizeMapCount"][k] += stat["txSizeMapCount"][k]
                        except: self.blockchain_stat["txSizeMapCount"][k] = stat["txSizeMapCount"][k]

                    for k in stat["txSizeMapAmount"]:
                        try:
                            self.blockchain_stat["txSizeMapAmount"][k] += stat["txSizeMapAmount"][k]
                        except:
                            self.blockchain_stat["txSizeMapAmount"][k] = stat["txSizeMapAmount"][k]

                    for k in stat["txTypeMapCount"]:
                        try:
                            self.blockchain_stat["txTypeMapCount"][k] += stat["txTypeMapCount"][k]
                        except:
                            self.blockchain_stat["txTypeMapCount"][k] = stat["txTypeMapCount"][k]

                    for k in stat["txTypeMapSize"]:
                        try:
                            self.blockchain_stat["txTypeMapSize"][k] += stat["txTypeMapSize"][k]
                        except:
                            self.blockchain_stat["txTypeMapSize"][k] = stat["txTypeMapSize"][k]

                    for k in stat["txTypeMapAmount"]:
                        try:
                            self.blockchain_stat["txTypeMapAmount"][k] += stat["txTypeMapAmount"][k]
                        except:
                            self.blockchain_stat["txTypeMapAmount"][k] = stat["txTypeMapAmount"][k]

                    # if self.blockchain_stat["feeMinValue"] < stat["feeMinValue"]:
                    #     self.blockchain_stat["feeMinValue"] = stat["feeMinValue"]
                    #     self.blockchain_stat["feeMinPointer"] = stat["feeMinPointer"]
                    #
                    #
                    # if self.blockchain_stat["feeMaxValue"] < stat["feeMaxValue"]:
                    #     self.blockchain_stat["feeMaxValue"] = stat["feeMaxValue"]
                    #     self.blockchain_stat["feeMaxPointer"] = stat["feeMaxPointer"]
                    #
                    # self.blockchain_stat["feeTotal"] += stat["feeTotal"]


                    self.blocks_stat.append((block["height"],
                                             json.dumps(block["stat"]),
                                             json.dumps(self.blockchain_stat)))


                batch_ready = False

                if len(self.transactions) >= self.batch_limit: batch_ready = True
                elif len(self.tx_map) >= self.batch_limit: batch_ready = True
                elif len(self.headers) >= self.batch_limit: batch_ready = True

                if batch_ready:
                    self.headers_batches[block["height"]] = self.headers
                    self.headers = deque()

                    self.transactions_batches[block["height"]] = self.transactions
                    self.transactions = deque()

                    if self.transaction_history:
                        self.tx_map_batches[block["height"]] = self.tx_map
                        self.tx_map = deque()
                        self.stxo_batches[block["height"]] = self.stxo
                        self.stxo = deque()

                    if self.blockchain_analytica:
                        self.blocks_stat_batches[block["height"]] = self.blocks_stat
                        self.blocks_stat = deque()

                    self.loop.create_task(self.save_batches())
        except:
            print(traceback.format_exc())
            raise

    async def save_batches(self):
        try:
            while self.headers_batches:
                height, h_batch = self.headers_batches.pop()
                if height in self.transactions_batches:
                    tx_batch = self.transactions_batches.delete(height)
                else:
                    tx_batch = False
                async with self.db_pool.acquire() as conn:
                    async with conn.transaction():
                        if self.blocks_data:
                            blocks_columns = ["height", "hash", "header", "adjusted_timestamp", "miner", "data"]
                        else:
                            blocks_columns = ["height", "hash", "header", "adjusted_timestamp"]

                        await conn.copy_records_to_table('blocks', columns=blocks_columns, records=h_batch)

                        if self.merkle_proof:
                            transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction",
                                                   "merkle_proof"]
                        else:
                            transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction"]

                        if tx_batch:
                            await conn.copy_records_to_table('transaction',
                                                             columns=transaction_columns, records=tx_batch)

                        if height in self.tx_map_batches:
                            batch = self.tx_map_batches.delete(height)
                            await conn.copy_records_to_table('transaction_map',
                                                             columns=["pointer", "address", "amount"],
                                                             records=batch)

                        if height in self.stxo_batches:
                            stxo_batch = self.stxo_batches.delete(height)
                            if stxo_batch:
                                await conn.copy_records_to_table('stxo',
                                                                 columns=["pointer", "s_pointer"],
                                                                 records=stxo_batch)
                        if height in self.blocks_stat_batches:
                            h_batch = self.blocks_stat_batches.delete(height)

                            await conn.copy_records_to_table('blocks_stat',
                                                             columns=["height", "block", "blockchain"],
                                                             records=h_batch)

                    self.connector.app_last_block = height

        except asyncio.CancelledError:
            self.log.debug("save_to_db process canceled")
        except:
            self.log.critical("save_to_db process failed; terminate server ...")
            self.log.critical(str(traceback.format_exc()))
            self.loop.create_task(self.terminate_coroutine())


    async def create_indexes(self):
        tasks = [self.loop.create_task(self.create_tx_index()),
                 self.loop.create_task(self.create_tx_history_index()),
                 self.loop.create_task(self.create_address_utxo_index()),
                 self.loop.create_task(self.create_blocks_hash_index())]
        self.log.info("create indexes ...")
        await asyncio.wait(tasks)
        self.log.info("create indexes completed")


    async def create_tx_index(self):
        async with self.db_pool.acquire() as conn:
            await conn.execute("CREATE INDEX IF NOT EXISTS transactions_map_tx_id "
                               "ON transaction USING BTREE (tx_id);")


    async def create_tx_history_index(self):
        if self.transaction_history:
            async with self.db_pool.acquire() as conn:
                await conn.execute("CREATE INDEX IF NOT EXISTS txmap_address_map_pointer "
                                   "ON transaction_map USING BTREE (address, pointer);")

    async def create_address_utxo_index(self):
        if self.transaction_history:
            async with self.db_pool.acquire() as conn:
                await conn.execute("CREATE INDEX IF NOT EXISTS address_map_utxo "
                                   "ON connector_utxo USING BTREE (address, pointer);")
                await conn.execute("CREATE INDEX IF NOT EXISTS address_map_uutxo "
                                   "ON connector_unconfirmed_utxo USING BTREE (address);")


    async def create_blocks_hash_index(self):
        async with self.db_pool.acquire() as conn:
            await conn.execute("CREATE INDEX IF NOT EXISTS blocks_hash "
                               "ON blocks USING BTREE (hash);")


    async def orphan_block_handler(self, data, conn):
        if not self.address_state_process.done():
            self.log.debug("Wait for address state module block process completed ...")
            await self.address_state_process

        # rollback address table in case address table synchronized
        if self.address_state_block == data["height"]:
            rows = await conn.fetch("SELECT pointer, address, amount FROM transaction_map "
                                    "WHERE pointer >= $1 and pointer < $2;", data["height"], data["height"] + 1)

            affected, address, removed, update_records = set(), dict(), set(), list()
            [affected.add(row["address"]) for row in rows]
            if affected:
                a_rows = await conn.fetch("SELECT address, data FROM address WHERE address = ANY($1)", affected)
                for row in a_rows:
                    address[row["address"]] = deserialize_address_data(row["data"])
                for row in rows:
                    r = 0 if row["pointer"] & 524288 else 1
                    rc, ra, c, sc, sa, dc = address[row["address"]]
                    if r: address[row["address"]] = (rc, ra, c, sc - 1, sa - row["amount"], dc - 1)
                    else: address[row["address"]] = (rc - 1, ra - row["amount"], c - 1, sc, sa, dc)
                for a in address:
                    if address[a][0]: update_records.append((a, serialize_address_data(*address[a])))
                    else: removed.add(a)
                if update_records:
                    await conn.execute("""
                                          UPDATE address SET data = r.data
                                          FROM 
                                               (SELECT address, data  FROM
                                                UNNEST($1::address[])) AS r 
                                          WHERE address.address = r.address;
                                      """, update_records)
                if removed: await conn.execute("DELETE FROM address WHERE address = ANY($1);", removed)

            await conn.execute("UPDATE service SET value = $1 WHERE name = 'address_last_block'", str(data["height"]-1))

        # transaction table
        rows = await conn.fetch("DELETE FROM transaction WHERE pointer >= $1 "
                                "RETURNING  tx_id,"
                                "           raw_transaction,"
                                "           pointer, "
                                "           timestamp,"
                                "           inputs_data;", data["height"] << 39)
        pointer_map_tx_id = dict()
        batch = []
        for row in rows:
            batch.append((row["tx_id"],
                          row["raw_transaction"],
                          row["timestamp"],
                          row["inputs_data"]))
            pointer_map_tx_id[row["pointer"]] = row["tx_id"]

        rows = await conn.fetch("DELETE FROM invalid_transaction WHERE tx_id = ANY($1) "
                                "RETURNING  tx_id,"
                                "           raw_transaction,"
                                "           timestamp,"
                                "           inputs_data;", data["mempool"]["tx"])
        for row in rows:
            batch.append((row["tx_id"],
                          row["raw_transaction"],
                          row["timestamp"],
                          row["inputs_data"]))

        await conn.copy_records_to_table('unconfirmed_transaction',
                                         columns=["tx_id", "raw_transaction",
                                                  "timestamp", "inputs_data"],
                                         records=batch)

        # transaction map table
        if self.transaction_history:
            rows = await conn.fetch("DELETE FROM transaction_map WHERE pointer >= $1 " 
                                    "RETURNING  pointer, address, amount;",  data["height"] << 39)

            batch = []
            t = int(time.time())
            for row in rows:
                pointer = (t << 32) + row["pointer"] & ((1<<20) - 1)
                batch.append((pointer_map_tx_id[(row["pointer"] >> 20) << 20],
                              pointer,
                              row["address"],
                              row["amount"]))


            rows = await conn.fetch("DELETE FROM invalid_transaction_map WHERE tx_id = ANY($1) "
                                    "RETURNING  tx_id, pointer, address, amount;",  data["mempool"]["tx"])

            for row in rows:
                batch.append((row["tx_id"],
                              row["pointer"],
                              row["address"],
                              row["amount"]))

            await conn.copy_records_to_table('unconfirmed_transaction_map',
                                             columns=["tx_id", "pointer",
                                                      "address", "amount"],
                                             records=batch)

        # blocks table

        await conn.execute("DELETE FROM blocks WHERE height = $1;", data["height"])

        if self.address_state_block == data["height"]:
            self.address_state_block -= 1


    async def synchronization_completed_handler(self):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("TRUNCATE TABLE unconfirmed_transaction;")
                if self.transaction_history:
                    await conn.execute("TRUNCATE TABLE unconfirmed_transaction_map;")
                await conn.execute("UPDATE service SET value = '1' WHERE name = 'bootstrap_completed';")
        await self.create_indexes()




    async def new_block_handler(self, block, conn):
        try:
            hash_list = [s2rh(t) for t in block["tx"]]
            if self.merkle_proof:

                m_tree = merkle_tree(hash_list)
                transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction",  "merkle_proof"]
            else:
                transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction"]

            # unconfirmed_transaction table

            rows = await conn.fetch("DELETE FROM unconfirmed_transaction WHERE tx_id = ANY($1) "
                                    "RETURNING  tx_id, raw_transaction, timestamp, inputs_data;", hash_list)
            batch = []
            for row in rows:
                index = block["tx"].index(rh2s(row["tx_id"]))
                if self.merkle_proof:
                    batch.append(((block["height"] << 39) + (index << 20),
                                  row["tx_id"],
                                  row["timestamp"],
                                  row["raw_transaction"],
                                  b''.join(merkle_proof(m_tree, index, return_hex=False))))
                else:
                    batch.append(((block["height"] << 39) + (index << 20),
                                  row["tx_id"],
                                  row["timestamp"],
                                  row["raw_transaction"]))
            await conn.copy_records_to_table('transaction', columns=transaction_columns, records=batch)

            # invalid_transaction table

            if block["mempoolInvalid"]["tx"]:
                rows = await conn.fetch("DELETE FROM unconfirmed_transaction WHERE tx_id = ANY($1) "
                                        "RETURNING  tx_id, raw_transaction, timestamp, inputs_data;",
                                        block["mempoolInvalid"]["tx"])
                batch = []
                for row in rows:
                    batch.append((row["tx_id"], row["raw_transaction"],
                                  row["timestamp"], row["inputs_data"]))
                await conn.copy_records_to_table('invalid_transaction',
                                                 columns=["tx_id", "raw_transaction",
                                                          "timestamp", "inputs_data"],
                                                 records=batch)


            if self.transaction_history:
                # invalid_transaction_map
                if block["mempoolInvalid"]["tx"]:
                    rows = await conn.fetch("DELETE FROM unconfirmed_transaction_map WHERE tx_id = ANY($1) "
                                            "RETURNING  tx_id, pointer, address, amount;",
                                            block["mempoolInvalid"]["tx"])
                    batch = []

                    for row in rows:
                        batch.append((row["tx_id"], row["pointer"], row["address"], row["amount"]))

                    await conn.copy_records_to_table('invalid_transaction_map',
                                                     columns=["tx_id", "pointer", "address", "amount"],
                                                     records=batch)


                # unconfirmed_transaction_map
                if self.transaction_history:
                    rows = await conn.fetch("DELETE FROM unconfirmed_transaction_map WHERE tx_id = ANY($1) "
                                            "RETURNING  tx_id, pointer, address, amount;", hash_list)
                    utxo_batch = []
                    for row in rows:
                        index = block["tx"].index(rh2s(row["tx_id"]))
                        pointer = (block["height"] << 39) + (index << 20) + (row["pointer"] & 1048575)
                        utxo_batch.append((pointer, row["address"], row["amount"]))

                    # transaction map table
                    await conn.copy_records_to_table('transaction_map',
                                                     columns=["pointer", "address", "amount"],
                                                     records=utxo_batch)


            # if address state table synchronized
            if self.address_state_block + 1 == block["height"]:
                affected, address, exist = set(), dict(), set()
                new_records, update_records = [], []
                [affected.add(r[1]) for r in batch]

                if affected:
                    a_rows = await conn.fetch("SELECT address, data FROM address  WHERE address = ANY($1)", affected)
                    for row in a_rows:
                        address[row["address"]] = deserialize_address_data(row["data"])
                        exist.add(row["address"])
                    for row in batch:
                        r = 0 if row[0] & 524288 else 1
                        try:
                            rc, ra, c, sc, sa, dc = address[row[1]]
                            if r: address[row[1]] = (rc, ra, c, sc + 1, sa + row[1], dc + 1)
                            else: address[row[1]] = (rc + 1, ra + row[1], c + 1, sc, sa, dc)
                        except:
                            if r: address[row[1]] = (0, 0, 0, 1, row[1], 1)
                            else: address[row[1]] = (1, row[1], 1, 0, 0, 0)

                for a in exist:
                    v = address.pop(a)
                    update_records.append((a, serialize_address_data(*v)))
                for a in address:
                    new_records.append((a, serialize_address_data(*address[a])))

                if new_records:
                    await conn.copy_records_to_table('address', columns=["address", "data"], records=new_records)
                if update_records:
                    await conn.execute("""UPDATE address SET data = r.data FROM 
                                          (SELECT address, data  FROM
                                                UNNEST($1::address[])) AS r 
                                           WHERE address.address = r.address;""", update_records)

                await conn.execute("""UPDATE service SET value = $1 
                                      WHERE name = 'address_last_block'; """, str(block["height"]))
                self.address_state_block = block["height"]

            # blocks table

            if self.block_best_timestamp < block["time"]:  self.block_best_timestamp = block["time"]
            await conn.copy_records_to_table('blocks',
                                             columns=["height", "hash", "header", "timestamp_received",
                                                      "adjusted_timestamp"],
                                             records=[(block["height"], s2rh(block["hash"]),
                                                       block["header"], int(time.time()), self.block_best_timestamp)])

        except:
            print(traceback.format_exc())
            raise


    async def new_transaction_handler(self, tx, timestamp, conn):
        try:
            raw_tx = tx.serialize(hex=False)
            tx_map = []
            tx_map_append = tx_map.append
            inputs = []
            inputs_append = inputs.append
            if not tx["coinbase"] and self.transaction_history:
                for i in tx["vIn"]:
                    inputs_append(tx["vIn"][i]["coin"][2])
                    inputs_append(int_to_c_int(tx["vIn"][i]["coin"][1]))
                    tx_map_append((tx["txId"],
                                   (timestamp << 32) + (0 << 19) + i,
                                   tx["vIn"][i]["coin"][2],
                                   tx["vIn"][i]["coin"][1]))



            if self.transaction_history:
                for i in tx["vOut"]:
                    out = tx["vOut"][i]
                    if out["nType"] in (8, 3): continue

                    if "addressHash" not in out:
                        address = b"".join((bytes([out["nType"]]), out["scriptPubKey"]))
                    else:
                        address = b"".join((bytes([out["nType"]]), out["addressHash"]))
                    tx_map_append((tx["txId"], (timestamp << 32) + (1 << 19) + i, address, out["value"]))

                await conn.copy_records_to_table('unconfirmed_transaction_map',
                                                 columns=["tx_id", "pointer",
                                                          "address", "amount"], records=tx_map)


            await conn.execute("""INSERT INTO unconfirmed_transaction (tx_id,
                                                                       raw_transaction,
                                                                       timestamp,
                                                                       inputs_data)
                                  VALUES ($1, $2, $3, $4);
                                """, tx["txId"], raw_tx, int(time.time()), b''.join(inputs))
        except:
            print(traceback.format_exc())
            raise


    async def address_state_processor(self):
        while True:
            try:
                if not self.address_state_block:
                    async with self.db_pool.acquire() as conn:
                        b = await conn.fetchval("SELECT value FROM service WHERE name = 'address_last_block' LIMIT 1;")
                        if b:
                            self.address_state_block = int(b)
                        else:
                            self.address_state_block = 0
                        await asyncio.sleep(20)
                        continue

                if self.connector.app_last_block > self.address_state_block:

                    if self.connector.active_block.done():
                        try:
                            self.address_state_process = asyncio.Future()
                            async with self.db_pool.acquire() as conn:
                                rows = await conn.fetch("SELECT pointer, address, amount FROM transaction_map "
                                                        "WHERE pointer >= $1 and pointer < $2;",
                                                        (self.address_state_block + 1) << 39,
                                                        (self.address_state_block + 2) << 39)

                            # day = ceil(self.blocks_map_time[row["pointer"] >> 39] / 86400)
                            # d = datetime.datetime.fromtimestamp(self.blocks_map_time[row["pointer"] >> 39],
                            #                                     datetime.timezone.utc)
                            # month = 12 * d.year + d.month

                            affected, address, exist = set(), dict(), set()
                            new_records, update_records = [], []
                            [affected.add(row["address"]) for row in rows]

                            if affected:
                                async with self.db_pool.acquire() as conn:
                                    a_rows = await conn.fetch("""
                                                                 SELECT address, data FROM address  
                                                                 WHERE address = ANY($1);
                                                              """, affected)
                                    for row in a_rows:
                                        address[row["address"]] = deserialize_address_data(row["data"])
                                        exist.add(row["address"])

                                for row in rows:
                                    r = 0 if row["pointer"] & 524288 else 1
                                    try:
                                        rc, ra, c, sc, sa, dc = address[row["address"]]
                                        if r:
                                            address[row["address"]] = (rc, ra, c, sc + 1, sa + row["amount"], dc + 1)
                                        else:
                                            address[row["address"]] = (rc + 1, ra + row["amount"], c + 1, sc, sa, dc)
                                    except:
                                            address[row["address"]] = (1, row["amount"], 1, 0, 0, 0)

                            for a in exist:
                                v = address.pop(a)
                                update_records.append((a, serialize_address_data(*v)))

                            [new_records.append((a, serialize_address_data(*address[a]))) for a in address]
                            async with self.db_pool.acquire() as conn:
                                async with conn.transaction():
                                    if new_records:
                                        await conn.copy_records_to_table('address',
                                                                         columns=["address", "data"],
                                                                         records=new_records)
                                    if update_records:
                                        await conn.execute("""
                                                              UPDATE address SET data = r.data
                                                              FROM 
                                                                   (SELECT address, data  FROM
                                                                    UNNEST($1::address[])) AS r 
                                                              WHERE address.address = r.address;
                                                          """, update_records)

                                    await conn.execute("""
                                                          UPDATE service SET value = $1
                                                          WHERE name =  'address_last_block';
                                                   """, str(self.address_state_block + 1))

                            self.address_state_block += 1
                        finally:
                            self.address_state_process.set_result(True)
                        continue

            except asyncio.CancelledError:
                self.log.warning("Address state processor stopped")
                break
            await asyncio.sleep(5)



    def _exc(self, a, b, c):
        return


    def terminate(self, a, b):
        if not self.shutdown:
            self.shutdown = True
            self.loop.create_task(self.terminate_coroutine())
        else:
            if not self.force_shutdown:
                self.log.critical("Shutdown in progress please wait ... (or press CTRL + C to force shutdown)")
                self.force_shutdown = True
            else:
                sys.exit(0)


    async def terminate_coroutine(self):
        sys.excepthook = self._exc
        self.log.error('Stop request received')
        if self.connector:
            self.log.warning("Stop node connector")
            await self.connector.stop()

        [process.terminate() for process in self.processes]
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks: await asyncio.wait(self.tasks)

        try: await self.db_pool.close()
        except: pass

        self.log.info("server stopped")
        self.loop.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="BTCAPI server v 0.0.1")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-c", "--config", help = "config file", type=str, nargs=1, metavar=('PATH',))
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="count", default=0)
    parser.add_argument("-w", "--connector", help="increase output verbosity for connector",
                        action="count",
                        default=0)
    args = parser.parse_args()
    config_file = "../config/btcapi-server.conf"
    log_level = logging.WARNING
    logger = logging.getLogger("server")
    logger_connector = logging.getLogger("connector")
    if args.config is not None:
        config_file = args.config[0]
    config = configparser.ConfigParser()
    config.read(config_file)
    if args.verbose > 0:
        log_level = logging.INFO
    if args.verbose > 1:
        log_level = logging.DEBUG

    connector_log_level = logging.INFO
    if args.connector > 0:
        connector_log_level = logging.WARNING
    if args.connector > 1:
        connector_log_level = logging.INFO
    if args.connector > 2:
        connector_log_level = logging.DEBUG

    ch = logging.StreamHandler()
    formatter = colorlog.ColoredFormatter('%(log_color) s%(asctime)s: %(message)s')
    formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s %(levelname)s: %(message)s (%(module)s:%(lineno)d)')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger_connector.addHandler(ch)


    # check config
    try:
        config["CONNECTOR"]["zeromq"]
        config["CONNECTOR"]["rpc"]
        config["CONNECTOR"]["testnet"]
        config["POSTGRESQL"]["dsn"]
        config["POSTGRESQL"]["server_threads"]
        config["POSTGRESQL"]["client_threads"]

        config["SYNCHRONIZATION"]["utxo_cache_size"]
        config["SYNCHRONIZATION"]["block_preload_workers"]
        config["SYNCHRONIZATION"]["block_preload_batch_size"]

        config["OPTIONS"]["merkle_proof"]
        config["OPTIONS"]["address_state"]
        config["OPTIONS"]["transaction"]
        config["OPTIONS"]["address_timeline"]
        config["OPTIONS"]["blockchain_analytica"]
        config["OPTIONS"]["transaction_history"]



        if int(config["SYNCHRONIZATION"]["block_preload_workers"]) not in range(1,9):
            raise Exception("SYNCHRONIZATION -> block_preload_workers invalid; acceptable value is [1-8]")


        if int(config["SYNCHRONIZATION"]["utxo_cache_size"]) < 0:
            raise Exception("SYNCHRONIZATION -> utxo_cache_size invalid; acceptable value is > 0")

        try:
            connector_log_level = log_level_map[config["CONNECTOR"]["log_level"]]
        except:
            pass

        try:
            log_level = log_level_map[config["SERVER"]["log_level"]]
        except:
            pass


    except Exception as err:
        logger.critical("Configuration failed: %s" % err)
        logger.critical("Shutdown")
        logger.critical(str(traceback.format_exc()))
        sys.exit(0)
    connector_log_level = logging.DEBUG
    logger.setLevel(log_level)
    logger_connector.setLevel(connector_log_level)
    loop = asyncio.get_event_loop()
    app = App(loop, logger, logger_connector, config)
    loop.run_forever()

    pending = asyncio.Task.all_tasks()
    for task in pending:
        task.cancel()
    if pending:
        loop.run_until_complete(asyncio.wait(pending))
    loop.close()

