import argparse
import asyncio
import configparser
import logging
import signal
import traceback
from setproctitle import setproctitle
import time
import asyncpg
import colorlog
from multiprocessing import Process
from collections import deque
import json, sys
from concurrent.futures import ThreadPoolExecutor
from sortedcontainers import SortedSet
import uvloop
import pickle

from pybtc import Connector, encode_gcs, int_to_var_int, ripemd160, double_sha256, sha256
from pybtc import int_to_c_int, MRU, bytes_to_int
from pybtc import map_into_range, siphash
from pybtc import s2rh, rh2s, merkle_tree, merkle_proof, parse_script


import db_model
from modules.filter_compressor import FilterCompressor
from utils import log_level_map, deserialize_address_data, serialize_address_data



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
        self.transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False

        self.block_filters = True if config["OPTIONS"]["block_filters"] == "on" else False
        self.blocks_data = True if config["OPTIONS"]["blocks_data"] == "on" else False

        self.address_state = True if config["OPTIONS"]["address_state"] == "on" else False
        self.address_timeline = True if config["OPTIONS"]["address_timeline"] == "on" else False

        self.blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False


        self.coinbase_maturity =  int(config["SYNCHRONIZATION"]["coinbase_maturity"])

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


        self.headers = deque()
        self.headers_batches = MRU()

        self.filters = deque()
        self.filters_batches = MRU()
        self.filters_batch_map = {1: dict(), 2: dict(), 4: dict(), 8: dict(), 16:dict()}
        self.filters_element_index = {1: 0, 2: 0, 4: 0, 8: 0, 16: 0}
        self.filters_last_header = {1: None, 2: None, 4: None, 8: None, 16: None}

        self.batch_limit = 10000
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
        self.start_checkpoint = 0
        self.address_state_block = 0
        self.address_state_rollback = 0
        self.address_state_process = asyncio.Future()
        self.address_state_process.set_result(True)
        self.block_best_timestamp = 0

        self.start_time = int(time.time())
        self.total_tx = 0
        self.processes = []
        self.tasks = []

        setproctitle('btcapi engine')
        self.log.info("BTCAPI server starting ...")


        # remap SIGINT and SIGTERM
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)

        self.loop.create_task(self.start(config, connector_logger))


    async def start(self, config, connector_logger):
        # init database
        self.log.info("Create/check database model; Get start point block; Load block chain tail ...")
        try:
            self.db_pool = await asyncpg.create_pool(dsn=self.psql_dsn, min_size=1, max_size=self.psql_threads)
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    await db_model.create_db_model(self, conn)
            self.log.info("Connecting to bitcoind daemon ...")

            self.connector =  Connector(config["CONNECTOR"]["rpc"],
                                        config["CONNECTOR"]["zeromq"],
                                        connector_logger,
                                        utxo_data = True,
                                        utxo_cache_size=self.utxo_cache_size,
                                        rpc_batch_limit=100,
                                        block_batch_handler = self.block_batch_handler,
                                        db_type="postgresql",
                                        test_orphans=False,
                                        db=self.psql_dsn,
                                        block_filters=self.block_filters,
                                        merkle_proof=self.merkle_proof,
                                        tx_map=self.transaction_history,
                                        analytica=self.blockchain_analytica,
                                        mempool_tx=True,
                                        chain_tail=self.chain_tail,
                                        block_timeout=300,
                                        deep_sync_limit=self.coinbase_maturity,
                                        last_block_height=self.start_checkpoint,
                                        block_cache_workers = self.block_preload_workers,
                                        tx_handler=self.new_transaction_handler,
                                        orphan_handler=self.orphan_block_handler,
                                        flush_app_caches_handler=self.flush_app_caches_handler,
                                        synchronization_completed_handler=self.synchronization_completed_handler,
                                        block_handler=self.new_block_handler,
                                        block_preload_batch_size_limit = self.block_preload_batch_size_limit,
                                        app_proc_title="btcapi engine connector")
        except asyncio.CancelledError:
            pass
        except Exception as err:
            self.log.warning("Start failed: %s" % err)
            # print(traceback.format_exc())
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
        # bootstrap_height = self.connector.node_last_block - 100

        if self.block_filters:
            self.processes.append(Process(target=FilterCompressor, args=(self.psql_dsn, self.log)))

        # if self.address_state:
        #     self.processes.append(Process(target=AddressStateSync, args=(self.psql_dsn,
        #                                                                  bootstrap_height,
        #                                                                  self.address_timeline,
        #                                                                  self.blockchain_analytica,
        #                                                                  self.log)))
        #     self.tasks.append(self.loop.create_task(self.address_state_processor()))

        [p.start() for p in self.processes]


    async def block_batch_handler(self, block):
        try:
            if self.block_best_timestamp < block["time"]:
                self.block_best_timestamp = block["time"]

            if block["height"] > self.start_checkpoint:

                if self.transaction:
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

                if self.block_filters:
                    self.filters.append((block["height"], block["filter"]))

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

                batch_ready = False

                if len(self.transactions) >= self.batch_limit: batch_ready = True
                elif len(self.tx_map) >= self.batch_limit: batch_ready = True
                elif len(self.headers) >= self.batch_limit: batch_ready = True

                if batch_ready:
                    self.headers_batches[block["height"]] = self.headers
                    self.headers = deque()

                    if self.transaction:
                        self.transactions_batches[block["height"]] = self.transactions
                        self.transactions = deque()

                    if self.transaction_history:
                        self.tx_map_batches[block["height"]] = self.tx_map
                        self.tx_map = deque()
                        self.stxo_batches[block["height"]] = self.stxo
                        self.stxo = deque()

                    if self.block_filters:
                        self.filters_batches[block["height"]] = self.filters
                        self.filters = deque()


                    self.loop.create_task(self.save_batches())
        except:
            print(traceback.format_exc())
            raise


    async def flush_app_caches_handler(self, height):
        while self.save_batches_process:
            await asyncio.sleep(5)
            print('debug wait flush_app_caches_handler')
        if self.tx_map:
            self.tx_map_batches[height] = self.tx_map
            self.tx_map = deque()
        if self.stxo:
            self.stxo_batches[height] = self.stxo
            self.stxo = deque()

        if self.transactions:
            self.transactions_batches[height] = self.transactions
            self.transactions = deque()

        if self.headers:
            self.headers_batches[height] = self.headers
            self.headers = deque()

        if self.blocks_stat:
            self.blocks_stat_batches[height] = self.blocks_stat
            self.blocks_stat = deque()

        if self.block_filters:
            self.filters_batches[height] = self.filters
            self.filters = deque()

        await self.save_batches()


    async def synchronization_completed_handler(self):
        while True:
            try:
                async with self.db_pool.acquire() as conn:
                    if self.block_filters:
                        c = await conn.fetchval("SELECT count(height) FROM raw_block_filters;")
                        if c > 0:
                            self.log.info("Creating block filters %s blocks left ..." % c)
                            await asyncio.sleep(60)
                            continue


                    async with conn.transaction():
                        await conn.execute("TRUNCATE TABLE unconfirmed_transaction;")
                        if self.transaction_history:
                            await conn.execute("TRUNCATE TABLE unconfirmed_transaction_map;")
                        await conn.execute("UPDATE service SET value = '1' WHERE name = 'bootstrap_completed';")
                await self.create_indexes()
                return
            except asyncio.CancelledError:
                return
            except Exception as err:
                self.log.error("Synchronization completed handler: %s" % err)
                await asyncio.sleep(10)



    async def new_transaction_handler(self, tx, timestamp, conn):
        try:
            raw_tx = tx.serialize(hex=False)
            tx_map = []
            tx_map_append = tx_map.append

            if self.transaction_history:
                if not tx["coinbase"]:
                    for i in tx["vIn"]:
                        tx_map_append((tx["txId"],
                                       (timestamp << 32) + (0 << 19) + i,
                                       tx["vIn"][i]["coin"][2],
                                       tx["vIn"][i]["coin"][1]))

                for i in tx["vOut"]:
                    out = tx["vOut"][i]
                    if out["nType"] in (8, 3): continue

                    try:
                        if tx["vOut"][i]["nType"] == 2:
                            raise Exception("PUBKEY")
                        address = b"".join((bytes([tx["vOut"][i]["nType"]]), tx["vOut"][i]["addressHash"]))
                    except:
                        address = b"".join((bytes([tx["vOut"][i]["nType"]]), tx["vOut"][i]["scriptPubKey"]))

                    tx_map_append((tx["txId"], (timestamp << 32) + (1 << 19) + i, address, out["value"]))

                await conn.copy_records_to_table('unconfirmed_transaction_map',
                                                 columns=["tx_id", "pointer",
                                                          "address", "amount"], records=tx_map)


            if self.transaction:
                await conn.execute("""INSERT INTO unconfirmed_transaction (tx_id,
                                                                           raw_transaction,
                                                                           timestamp)
                                      VALUES ($1, $2, $3);
                                    """, tx["txId"], raw_tx, int(time.time()))
        except:
            print(traceback.format_exc())
            raise


    async def orphan_block_handler(self, data, conn):

        if self.address_state:
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
        if self.transaction:
            rows = await conn.fetch("DELETE FROM transaction WHERE pointer >= $1 "
                                    "RETURNING  tx_id,"
                                    "           raw_transaction,"
                                    "           pointer, "
                                    "           timestamp;", data["height"] << 39)
            pointer_map_tx_id = dict()
            batch = []
            for row in rows:
                pointer_map_tx_id[row["pointer"]] = row["tx_id"]
                if row["tx_id"] == data["coinbase_tx_id"]: continue
                batch.append((row["tx_id"],
                              row["raw_transaction"],
                              row["timestamp"]))


            await conn.copy_records_to_table('unconfirmed_transaction',
                                             columns=["tx_id", "raw_transaction", "timestamp"],
                                             records=batch)


        # transaction map table
        if self.transaction_history:
            await conn.execute("DELETE FROM stxo WHERE s_pointer >= $1;",  data["height"] << 39)

            rows = await conn.fetch("DELETE FROM transaction_map WHERE pointer >= $1 " 
                                    "RETURNING  pointer, address, amount;",  data["height"] << 39)

            batch = []
            t = int(time.time())
            for row in rows:
                pointer = (t << 32) + row["pointer"] & ((1<<20) - 1)
                tx_id = pointer_map_tx_id[(row["pointer"] >> 20) << 20]
                if tx_id == data["coinbase_tx_id"]: continue
                batch.append((tx_id, pointer, row["address"], row["amount"]))

            await conn.copy_records_to_table('unconfirmed_transaction_map',
                                             columns=["tx_id", "pointer", "address", "amount"],
                                             records=batch)

        # blocks table

        await conn.execute("DELETE FROM blocks WHERE height = $1;", data["height"])

        if self.block_filters:
            await conn.execute("DELETE FROM block_filters_batch WHERE height = $1;", data["height"])
            await conn.execute("DELETE FROM block_filter WHERE height = $1;", data["height"])

        if self.address_state:
            if self.address_state_block == data["height"]:
                self.address_state_block -= 1


    async def new_block_handler(self, block, conn):
        try:
            hash_list = [s2rh(t) for t in block["tx"]]
            if self.transaction:
                if self.merkle_proof:
                    m_tree = merkle_tree(hash_list)
                    transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction",  "merkle_proof"]
                else:
                    transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction"]

                # unconfirmed_transaction table
                rows = await conn.fetch("DELETE FROM unconfirmed_transaction WHERE tx_id = ANY($1) "
                                        "RETURNING  tx_id, raw_transaction, timestamp;", hash_list)
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
                    await conn.execute("DELETE FROM unconfirmed_transaction WHERE tx_id = ANY($1);",
                                        block["mempoolInvalid"]["tx"])

            if self.transaction_history:
                # invalid_transaction_map
                if block["mempoolInvalid"]["tx"]:
                    await conn.execute("DELETE FROM unconfirmed_transaction_map WHERE tx_id = ANY($1); ",
                                        block["mempoolInvalid"]["tx"])

                # unconfirmed_transaction_map
                rows = await conn.fetch("DELETE FROM unconfirmed_transaction_map WHERE tx_id = ANY($1) "
                                        "RETURNING  tx_id, pointer, address, amount;", hash_list)
                utxo_batch = []

                for row in rows:
                    index = block["tx"].index(rh2s(row["tx_id"]))
                    pointer = (block["height"] << 39) + (index << 20) + (row["pointer"] & 1048575)
                    utxo_batch.append((pointer, row["address"], row["amount"]))

                await conn.copy_records_to_table('transaction_map', columns=["pointer", "address", "amount"],
                                                 records=utxo_batch)

                # create stxo records
                tx_map_pointer = dict()
                for s in block["stxo"]:
                    tx_map_pointer[s[2]] = None
                rows = await conn.fetch("SELECT pointer, tx_id  FROM transaction WHERE tx_id = ANY($1) ",
                                        tx_map_pointer.keys())
                for row in rows:
                    tx_map_pointer[row["tx_id"]] = row["pointer"]
                assert len(rows) == len(tx_map_pointer)
                stxo = deque()
                for s in block["stxo"]:
                    index = block["tx"].index(rh2s(s[3]))
                    s_pointer =  (block["height"]<<39)+(index<<20)+s[4]
                    pointer = tx_map_pointer[s[2]]+(1<<19)+bytes_to_int(s[0][32:])
                    stxo.append((pointer, s_pointer))
                    # print((pointer, s_pointer))

                await conn.copy_records_to_table('stxo', columns=["pointer", "s_pointer"],
                                                 records=stxo)


            # block filters
            if self.block_filters:
                r = await conn.fetchval("SELECT data FROM block_filters_batch WHERE height = $1", block['height'] - 1)
                result = pickle.loads(r)
                last_hash = result['last_hash']
                batch_map = result['batch_map']
                element_index = result['element_index']

                if (block["height"]) % 1000 == 0:
                    batch_map = {1: dict(), 2: dict(), 4: dict(), 8: dict(), 16: dict()}
                    element_index = {1: 0, 2: 0, 4: 0, 8: 0, 16: 0}

                F = 2 ** 32
                elements = {1: SortedSet(), 2: SortedSet(), 4: SortedSet(), 8: SortedSet(), 16: SortedSet()}
                duplicates = {1: set(), 2: set(), 4: set(), 8: set(), 16: set()}
                tx_filters = {1: dict(), 2: dict(), 4: dict(), 8: dict(), 16: dict()}
                n_type_map_filter_type = {0: 2, 1: 4, 2: 1, 5: 8, 6: 16}

                for i in sorted(block["tx_filters"].keys()):
                    for address in block["tx_filters"][i]:
                        if address[0] not in (0, 1, 2, 5, 6):
                            continue
                        f_type = n_type_map_filter_type[address[0]]
                        if address[0] == 2:
                            k = parse_script(bytes(address[1:]))
                            e = k["addressHash"][:20]
                        else:
                            e = bytes(address[1:21])
                        q = map_into_range(siphash(e), F)

                        try:
                            tx_filters[f_type][i].add(q.to_bytes(4, "little"))
                        except:
                            tx_filters[f_type][i] = SortedSet({q.to_bytes(4, "little")})

                        if q in batch_map[f_type]:
                            duplicates[f_type].add(q)
                        else:
                            elements[f_type].add(q)

                for f_type in n_type_map_filter_type.values():
                    if elements[f_type]:
                        for x in elements[f_type]:
                            batch_map[f_type][x] = element_index[f_type]
                            element_index[f_type] += 1
                        d = encode_gcs(elements[f_type], sort=False)
                        f = b"".join([int_to_var_int(len(d)), d])
                    else:
                        f = int_to_var_int(0)

                    if duplicates[f_type]:
                        pointers_set = set()
                        # convert values to pointers
                        for x in duplicates[f_type]:
                            pointers_set.add(batch_map[f_type][x])

                        encoded_pointers = encode_gcs(pointers_set)
                        fd = b"".join([int_to_var_int(len(encoded_pointers)),
                                       encoded_pointers])
                        f += fd

                    else:
                        f += int_to_var_int(0)

                    if f != b"\x00\x00":
                        d = bytearray()
                        for i in sorted(tx_filters[f_type].keys()):
                            d += b"".join(tx_filters[f_type][i])
                        f += ripemd160(sha256(d))

                    if last_hash[f_type] and f != b"\x00\x00":
                        last_hash[f_type] = double_sha256(double_sha256(f) + last_hash[f_type])

                    elif f != b"\x00\x00":
                        last_hash[f_type] = double_sha256(double_sha256(f) + b"\00" * 32)

                    if last_hash[f_type]:
                        await conn.copy_records_to_table('block_filter',
                                                         columns=["height", "type", "hash", "filter"],
                                                         records=[(block["height"],
                                                                  f_type,
                                                                  last_hash[f_type],
                                                                  f)])
                await conn.execute("INSERT INTO block_filters_batch (height, data) "
                                   "VALUES ($1, $2);", block["height"],
                                   pickle.dumps({'last_hash': last_hash,
                                                 'batch_map': batch_map,
                                                 'element_index': element_index}))

            # if address state table synchronized
            if self.address_state:
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
        # await asyncio.sleep(10)
        # assert 0




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

    async def save_batches(self):
        try:
            self.save_batches_process = True
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


                        if self.block_filters:
                            if height in self.filters_batches:
                                batch = self.filters_batches.delete(height)
                                await conn.copy_records_to_table('raw_block_filters',
                                                                 columns=["height", "filter"], records=batch)
                        if self.transaction:
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
        finally:
            self.save_batches_process = False

    async def create_indexes(self):
        tasks = [self.loop.create_task(self.create_tx_index()),
                 self.loop.create_task(self.create_tx_history_index()),
                 self.loop.create_task(self.create_address_utxo_index()),
                 self.loop.create_task(self.create_blocks_hash_index())]
        self.log.info("create indexes ...")
        await asyncio.wait(tasks)
        self.log.info("create indexes completed")

    async def create_tx_index(self):
        if self.transaction:
            async with self.db_pool.acquire() as conn:
                await conn.execute("CREATE INDEX IF NOT EXISTS transactions_map_tx_id "
                                   "ON transaction USING BTREE (tx_id);")

    async def create_tx_history_index(self):
        if self.transaction_history:
            async with self.db_pool.acquire() as conn:
                await conn.execute("CREATE INDEX IF NOT EXISTS txmap_address_map_pointer "
                                   "ON transaction_map USING BTREE (address, pointer);")
                await conn.execute("CREATE INDEX IF NOT EXISTS stxo_s_pointer "
                                   "ON stxo USING BTREE (s_pointer);")

    async def create_address_utxo_index(self):
        async with self.db_pool.acquire() as conn:
            await conn.execute("CREATE INDEX IF NOT EXISTS address_map_utxo "
                               "ON connector_utxo USING BTREE (address, pointer);")
            await conn.execute("CREATE INDEX IF NOT EXISTS address_map_utxo_pointer "
                               "ON connector_utxo USING BTREE (pointer);")
            await conn.execute("CREATE INDEX IF NOT EXISTS address_map_uutxo "
                               "ON connector_unconfirmed_utxo USING BTREE (address);")

    async def create_blocks_hash_index(self):
        async with self.db_pool.acquire() as conn:
            await conn.execute("CREATE INDEX IF NOT EXISTS blocks_hash "
                               "ON blocks USING BTREE (hash);")



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
                        action="count", default=0)
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

