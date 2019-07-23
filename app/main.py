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
from pybtc import int_to_c_int
from pybtc import s2rh, rh2s, merkle_tree, merkle_proof
import db_model
import os
from utils import (pipe_get_msg,
                   pipe_sent_msg,
                   get_pipe_reader,
                   get_pipe_writer,
                   log_level_map,
                   deserialize_address_data,
                   serialize_address_data)

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class App:
    def __init__(self, loop, logger, connector_logger, config):
        self.loop = loop
        self.log = logger
        self.config = config

        # options

        self.transaction = True if config["OPTIONS"]["transaction"] == "on" else False
        self.merkle_proof = True if config["OPTIONS"]["merkle_proof"] == "on" else False
        self.address_state = True if config["OPTIONS"]["address_state"] == "on" else False
        self.address_timeline = True if config["OPTIONS"]["address_timeline"] == "on" else False
        self.blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
        self.transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False

        self.block_preload_workers = int(config["SYNCHRONIZATION"]["block_preload_workers"])
        self.block_preload_batch_size_limit = int(config["SYNCHRONIZATION"]["block_preload_batch_size"])
        self.utxo_cache_size = int(config["SYNCHRONIZATION"]["utxo_cache_size"])
        self.psql_dsn = config["POSTGRESQL"]["dsn"]
        self.psql_threads = int(config["POSTGRESQL"]["server_threads"])
        self.chain_tail = []
        self.shutdown = False
        setproctitle('btcapi engine')


        self.db_pool = None
        self.rpc = None
        self.connector = None
        self.sync_worker = None
        self.sync_worker_writer = None
        self.sync_worker_reader = None

        self.transaction_map_start_block = 0
        self.transaction_start_block = 0
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
                                             mempool_tx=True,
                                             chain_tail=self.chain_tail,
                                             block_timeout=300,
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

            await self.connector.connected
            self.log.info("Bitcoind connected")
            bootstrap_height = self.connector.node_last_block - 100

            if self.address_state:
                self.processes.append(Process(target=AddressStateSync, args=(self.psql_dsn,
                                                                             bootstrap_height,
                                                                             self.address_timeline,
                                                                             self.log)))
                [p.start() for p in self.processes]
                self.tasks.append(self.loop.create_task(self.address_state_processor()))

        except Exception as err:
            self.log.warning("Start failed: %s" % err)
            print(traceback.format_exc())
            self.log.warning("Reconnecting ...")
            await asyncio.sleep(3)
            self.loop.create_task(self.start(config, connector_logger))


    async def flush_app_caches_handler(self, height):
        if self.sync_worker_writer:
            pipe_sent_msg(b'flush', pickle.dumps(height), self.sync_worker_writer)
            await self.sync_worker_writer.drain()
        # create indexes


    async def block_batch_handler(self, block):
        while self.sync_worker_writer is None:
            if self.sync_worker is None:
                self.loop.create_task(self.sync_worker_process())
                await asyncio.sleep(1)
            else:
                await asyncio.sleep(1)
        pipe_sent_msg(b'block', pickle.dumps(block), self.sync_worker_writer)
        await asyncio.wait([self.sync_worker_writer.drain(),])


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
        if self.sync_worker:
            self.sync_worker.terminate()
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("TRUNCATE TABLE unconfirmed_transaction;")
                    await conn.execute("TRUNCATE TABLE unconfirmed_transaction_map;")
                    await conn.execute("UPDATE service SET value = '1' WHERE name = 'bootstrap_completed';")


    async def new_block_handler(self, block, conn):
        try:
            if not self.address_state_process.done():
                self.log.debug("Wait for address state module block process completed ...")
                await self.address_state_process

            hash_list = [s2rh(t) for t in block["tx"]]
            if self.merkle_proof:
                m_tree = merkle_tree(hash_list)
                transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction",
                                       "inputs_data", "merkle_proof"]
            else:
                transaction_columns = ["pointer", "tx_id", "timestamp", "raw_transaction", "inputs_data"]

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
                                  row["inputs_data"],
                                  b''.join(merkle_proof(m_tree, index, return_hex=False))))
                else:
                    batch.append(((block["height"] << 39) + (index << 20),
                                  row["tx_id"],
                                  row["timestamp"],
                                  row["raw_transaction"],
                                  row["inputs_data"]))
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
            if self.address_timeline:
                if self.block_best_timestamp < block["time"]:
                    self.block_best_timestamp = block["time"]
                await conn.copy_records_to_table('blocks',
                                                 columns=["height", "hash", "header",
                                                          "timestamp", "timestamp_received",
                                                          "adjusted_timestamp"],
                                                 records=[(block["height"], s2rh(block["hash"]),
                                                           block["header"], int(time.time()),
                                                           block["time"], self.block_best_timestamp)])
            else:
                await conn.copy_records_to_table('blocks',
                                                 columns=["height", "hash", "header",
                                                          "timestamp", "timestamp_received"],
                                                 records=[(block["height"], s2rh(block["hash"]),
                                                           block["header"], block["time"], int(time.time()))])

        except:
            print(traceback.format_exc())
            raise


    async def new_transaction_handler(self, tx, timestamp, conn):
        raw_tx = tx.serialize(hex=False)
        tx_map = []
        tx_map_append = tx_map.append
        inputs = []
        inputs_append = inputs.append
        if not tx["coinbase"]:
            for i in tx["vIn"]:
                inputs_append(tx["vIn"][i]["coin"][2])
                inputs_append(int_to_c_int(tx["vIn"][i]["coin"][1]))
                tx_map_append((tx["txId"],
                               (timestamp << 32) + (0 << 19) + i,
                               tx["vIn"][i]["coin"][2],
                               tx["vIn"][i]["coin"][1]))

                    # prepare outputs
        for i in tx["vOut"]:
            out = tx["vOut"][i]
            if out["nType"] in (7, 8, 3, 4): continue

            if "addressHash" not in out:
                address = b"".join((bytes([out["nType"]]), out["scriptPubKey"]))
            else:
                address = b"".join((bytes([out["nType"]]), out["addressHash"]))
            tx_map_append((tx["txId"], (timestamp << 32) +  (1 << 19) + i, address, out["value"]))


        await conn.copy_records_to_table('unconfirmed_transaction_map',
                                         columns=["tx_id", "pointer",
                                                  "address", "amount"], records=tx_map)


        await conn.execute("""INSERT INTO unconfirmed_transaction (tx_id,
                                                                   raw_transaction,
                                                                   timestamp,
                                                                   inputs_data)
                              VALUES ($1, $2, $3, $4);
                            """, tx["txId"], raw_tx, int(time.time()), b''.join(inputs))


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






    async def sync_worker_process(self):
        self.log.warning('Start synchronization worker')
        # prepare pipes for communications
        in_reader, in_writer = os.pipe()
        out_reader, out_writer = os.pipe()
        in_reader, out_reader  = os.fdopen(in_reader,'rb'), os.fdopen(out_reader,'rb')
        in_writer, out_writer  = os.fdopen(in_writer,'wb'), os.fdopen(out_writer,'wb')

        start_blocks = {"transaction_map_start_block": self.transaction_map_start_block,
                        "transaction_start_block": self.transaction_start_block,
                        "headers_start_block": self.block_start_block}


        # create new process
        self.sync_worker = Process(target=SynchronizationWorker, args=(in_reader, in_writer,
                                                                       out_reader, out_writer,
                                                                       self.psql_dsn,
                                                                       start_blocks,
                                                                       self.merkle_proof,
                                                                       self.address_timeline,
                                                                       self.blockchain_analytica))
        self.sync_worker.start()
        in_reader.close()
        out_writer.close()
        # get stream reader
        self.sync_worker_reader = await get_pipe_reader(out_reader, self.loop)
        self.sync_worker_writer = await get_pipe_writer(in_writer, self.loop)

        # start message loop
        self.tasks.append(self.loop.create_task(self.sync_worker_message_loop()))
        # wait if process crash
        await self.loop.run_in_executor(None, self.sync_worker.join)
        self.log.warning('Synchronization worker stopped')
        self.sync_worker = None
        self.sync_worker_writer = None
        self.sync_worker_reader = None


    async def sync_worker_message_loop(self):
        try:
            while True:
                msg_type, msg = await pipe_get_msg(self.sync_worker_reader)
                if msg_type == b'checkpoint':
                    self.connector.app_last_block = pickle.loads(msg)
        except asyncio.CancelledError:
            self.log.debug("sync worker message loop  canceled")
        except:
            self.log.critical("broken pipe; terminate server ...")
            self.log.critical(str(traceback.format_exc()))
            self.loop.create_task(self.terminate_coroutine())


    def _exc(self, a, b, c):
        return


    def terminate(self, a, b):
        if not self.shutdown:
            self.shutdown = True
            self.loop.create_task(self.terminate_coroutine())
        else:
            self.log.critical("Shutdown in progress please wait ...")


    async def terminate_coroutine(self):
        sys.excepthook = self._exc
        self.log.error('Stop request received')
        if self.connector:
            self.log.warning("Stop node connector")
            await self.connector.stop()

        self.log.warning('sync worker stop request received')
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

