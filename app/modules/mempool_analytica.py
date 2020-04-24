import asyncio
import signal
from setproctitle import setproctitle
import asyncpg
from collections import deque
import time
import json
import math
from pybtc import rh2s


class MempoolAnalytica():

    def __init__(self, dsn, logger):
        setproctitle('btcapi server: mempool analytica')
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.dsn = dsn
        self.log = logger
        self.task = None
        self.block_map_timestamp = dict()
        self.last_block = -1
        self.last_hash = None
        self.last_minute = 0
        self.last_hour = 0
        self.last_day = 0
        self.bootstrap_completed = False
        self.loop = asyncio.get_event_loop()
        signal.signal(signal.SIGTERM, self.terminate)
        self.loop.create_task(self.start())
        self.loop.run_forever()


    async def start(self):
        try:
            self.db_pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=3)
            self.log.info("Mempool analytica module started")
            self.task = self.loop.create_task(self.processor())
        except Exception as err:
            self.log.warning("Start mempool analytica module failed: %s" % err)
            await asyncio.sleep(3)
            self.loop.create_task(self.start())


    async def load_block_map(self):
        # init db pool
        async with self.db_pool.acquire() as conn:
            stmt = await conn.prepare("SELECT adjusted_timestamp, height  "
                                      "FROM blocks WHERE height > $1 order by height asc;")
            rows = await stmt.fetch(self.last_block)
        for row in rows:
            self.block_map_timestamp[row["height"]] = row["adjusted_timestamp"]
            self.last_block = row["height"]


    def refresh_stat(self):
        outputs = {"count": 0,
                   "amount": {"max": {"value": None,
                                      "txId": None},
                              "min": {"value": None,
                                      "txId": None},
                              "total": 0
                              },
                   "typeMap": {},
                   "amountMap": {}}
        inputs = {"count": 0,
                  "amount": {"max": {"value": None,
                                     "txId": None},
                             "min": {"value": None,
                                     "txId": None},
                             "total": 0
                             },
                  "typeMap": {},
                  "amountMap": {},
                  "ageMap": {}}

        transactions = {"fee": {"max": {"value": None,
                                        "txId": None},
                                "min": {"value": None,
                                        "txId": None},
                                "total": 0
                                },
                        "size": {"max": {"value": None,
                                         "txId": None},
                                 "min": {"value": None,
                                         "txId": None},
                                 "total": 0
                                 },
                        "amount": {"max": {"value": None,
                                           "txId": None},
                                   "min": {"value": None,
                                           "txId": None},
                                   "total": 0
                                   },
                        "feeRate": {"max": {"value": None,
                                            "txId": None},
                                    "min": {"value": None,
                                            "txId": None}
                                    },
                        "segwitCount": 0,
                        "rbfCount": 0,
                        "doublespend": {"count": 0, "size": 0, "amount": 0},
                        "doublespendChilds": {"count": 0, "size": 0, "amount": 0},
                        "feeRateMap": {},
                        "count": 0}
        return outputs, inputs, transactions

    async def processor(self):
        utxo_sequence = 0
        stxo_sequence = 0
        tx_sequence = 0
        dbs = set()
        dbs_childs = set()
        outputs, inputs, transactions = self.refresh_stat()

        while True:
            try:
                if not self.bootstrap_completed:
                    async with self.db_pool.acquire() as conn:
                        v = await conn.fetchval("SELECT value FROM  service "
                                                "WHERE name = 'bootstrap_completed' LIMIT 1;")
                    if v == '1':
                        self.bootstrap_completed = True
                        self.log.info("Mempool analytica task started")
                        async with self.db_pool.acquire() as conn:
                            self.last_day = await conn.fetchval("SELECT max(day) FROM  mempool_analytica")
                            self.last_hour = await conn.fetchval("SELECT max(hour) FROM  mempool_analytica")
                    else:
                        await asyncio.sleep(60)

                q = time.time()
                await self.load_block_map()

                async with self.db_pool.acquire() as conn:
                    async with conn.transaction():
                        last_hash = await conn.fetchval("SELECT height FROM blocks order by height desc  LIMIT 1;")

                        if last_hash != self.last_hash:
                            self.last_hash = last_hash
                            outputs, inputs, transactions = self.refresh_stat()
                            utxo_sequence = 0
                            stxo_sequence = 0
                            tx_sequence = 0
                            dbs = set()
                            dbs_childs = set()

                        stxo = await conn.fetch("SELECT tx_id, out_tx_id, address, amount, pointer, sequence, id  "
                                                "FROM connector_unconfirmed_stxo "
                                                "WHERE id > $1;", stxo_sequence)
                        utxo = await conn.fetch("SELECT out_tx_id as tx_id, address, amount, id "
                                                "FROM connector_unconfirmed_utxo "
                                                "WHERE id > $1;", utxo_sequence)
                        tx = await conn.fetch("SELECT tx_id, size, b_size, rbf, fee, "
                                              "amount, segwit, timestamp, id  FROM unconfirmed_transaction "
                                              "WHERE id > $1;", tx_sequence)

                txsi = set()
                txso = set()
                inputs["count"] += len(stxo)
                for row in stxo:
                    if stxo_sequence < row["id"]:
                        stxo_sequence = row["id"]
                    if row["sequence"] > 0:
                        dbs.add(row["tx_id"])
                    txsi.add(row["tx_id"])
                    inputs["amount"]["total"] += row["amount"]
                    if inputs["amount"]["max"]["value"] is None or \
                            inputs["amount"]["max"]["value"] < row["amount"]:
                        inputs["amount"]["max"]["value"] = row["amount"]
                        inputs["amount"]["max"]["txId"] = rh2s(row["tx_id"])

                    if inputs["amount"]["min"]["value"] is None or \
                            inputs["amount"]["min"]["value"] > row["amount"]:
                        inputs["amount"]["min"]["value"] = row["amount"]
                        inputs["amount"]["min"]["txId"] = rh2s(row["tx_id"])

                    try:
                        inputs["typeMap"][row["address"][0]]["count"] += 1
                        inputs["typeMap"][row["address"][0]]["amount"] += row["amount"]
                    except:
                        inputs["typeMap"][row["address"][0]] = {"count": 1,
                                                                          "amount": row["amount"]}
                    amount = row["amount"]
                    key = None if amount == 0 else str(math.floor(math.log10(amount)))

                    try:
                        inputs["amountMap"][key]["count"] += 1
                        inputs["amountMap"][key]["amount"] += row["amount"]
                    except:
                        inputs["amountMap"][key] = {"count": 1, "amount": row["amount"]}

                    try:
                        key = time.time() - self.block_map_timestamp[row["pointer"] >> 39]
                        if key < 3600:
                            key = "1h"
                        elif key < 43200:
                            key = "12h"
                        elif key < 86400:
                            key = "1d"
                        elif key < 259200:
                            key = "3d"
                        elif key < 604800:
                            key = "1w"
                        elif key < 2592000:
                            key = "1m"
                        else:
                            key = "%sy" % (int(key // 31536000) + 1)
                    except:
                        key = None

                    try:
                        inputs["ageMap"][key]["count"] += 1
                        inputs["ageMap"][key]["amount"] += row["amount"]
                    except:
                        inputs["ageMap"][key] = {"count": 1, "amount": row["amount"]}

                l_dbs_size = 0
                while True:
                    for row in stxo:
                        if row["out_tx_id"] in dbs:
                            dbs_childs.add(row["tx_id"])
                        if row["out_tx_id"] in dbs_childs:
                            dbs_childs.add(row["tx_id"])
                    if l_dbs_size != len(dbs_childs):
                        l_dbs_size = len(dbs_childs)
                    else:
                        break

                outputs["count"] += len(utxo)
                for row in utxo:
                    if utxo_sequence < row["id"]:
                        utxo_sequence = row["id"]
                    txso.add(row["tx_id"])
                    outputs["amount"]["total"] += row["amount"]
                    if outputs["amount"]["max"]["value"] is None or \
                            outputs["amount"]["max"]["value"] < row["amount"]:
                        outputs["amount"]["max"]["value"] = row["amount"]
                        outputs["amount"]["max"]["txId"] = rh2s(row["tx_id"])

                    if outputs["amount"]["min"]["value"] is None or \
                            outputs["amount"]["min"]["value"] > row["amount"]:
                        if row["amount"] > 0:
                            outputs["amount"]["min"]["value"] = row["amount"]
                            outputs["amount"]["min"]["txId"] = rh2s(row["tx_id"])
                    try:
                        outputs["typeMap"][row["address"][0]]["count"] += 1
                        outputs["typeMap"][row["address"][0]]["amount"] += row["amount"]
                    except:
                        outputs["typeMap"][row["address"][0]] = {"count": 1,
                                                                          "amount": row["amount"]}
                    amount = row["amount"]
                    key = None if amount == 0 else str(math.floor(math.log10(amount)))

                    try:
                        outputs["amountMap"][key]["count"] += 1
                        outputs["amountMap"][key]["amount"] += row["amount"]
                    except:
                        outputs["amountMap"][key] = {"count": 1, "amount": row["amount"]}

                transactions["doublespend"]["count"] = len(dbs)
                transactions["doublespendChilds"]["count"] = len(dbs_childs)
                transactions["count"] += len(tx)
                dbs_records = deque()
                dbs_childs_records = deque()

                for row in tx:
                    if tx_sequence < row["id"]:
                        tx_sequence = row["id"]
                    if row["tx_id"] in dbs:
                        transactions["doublespend"]["amount"] += row["amount"]
                        transactions["doublespend"]["size"] += row["size"]
                        dbs_records.append((row["tx_id"], row["timestamp"]))
                    if row["tx_id"] in dbs_childs:
                        transactions["doublespendChilds"]["amount"] += row["amount"]
                        transactions["doublespendChilds"]["size"] += row["size"]
                        dbs_childs_records.append((row["tx_id"], row["timestamp"]))

                    if row["amount"] > 0:
                        transactions["amount"]["total"] += row["amount"]
                        if transactions["amount"]["max"]["value"] is None or \
                                transactions["amount"]["max"]["value"] < row["amount"]:
                            transactions["amount"]["max"]["value"] = row["amount"]
                            transactions["amount"]["max"]["txId"] = rh2s(row["tx_id"])

                        if transactions["amount"]["min"]["value"] is None or \
                                transactions["amount"]["min"]["value"] > row["amount"]:
                            transactions["amount"]["min"]["value"] = row["amount"]
                            transactions["amount"]["min"]["txId"] = rh2s(row["tx_id"])

                    if row["fee"] is not None:
                        transactions["fee"]["total"] += row["fee"]
                        if transactions["fee"]["max"]["value"] is None or \
                                transactions["fee"]["max"]["value"] < row["fee"]:
                            transactions["fee"]["max"]["value"] = row["fee"]
                            transactions["fee"]["max"]["txId"] = rh2s(row["tx_id"])

                        if transactions["fee"]["min"]["value"] is None or \
                                transactions["fee"]["min"]["value"] > row["fee"]:
                            transactions["fee"]["min"]["value"] = row["fee"]
                            transactions["fee"]["min"]["txId"] = rh2s(row["tx_id"])
                        v_size = math.ceil((row["b_size"] * 3 + row["size"]) / 4)

                        fee_rate =  math.ceil(row["fee"] / v_size)

                        if transactions["feeRate"]["max"]["value"] is None or \
                                transactions["feeRate"]["max"]["value"] < fee_rate:
                            transactions["feeRate"]["max"]["value"] = fee_rate
                            transactions["feeRate"]["max"]["txId"] = rh2s(row["tx_id"])

                        if transactions["feeRate"]["min"]["value"] is None or \
                                transactions["feeRate"]["min"]["value"] > fee_rate:
                            transactions["feeRate"]["min"]["value"] = fee_rate
                            transactions["feeRate"]["min"]["txId"] = rh2s(row["tx_id"])

                        key = fee_rate
                        if key > 10 and key < 20:
                            key = math.ceil(key / 2) * 2
                        elif key > 20 and  key < 200:
                            key = math.ceil(key / 10) * 10
                        elif key > 200:
                            key = math.ceil(key / 25) * 25
                        try:
                            transactions["feeRateMap"][key]["count"] += 1
                            transactions["feeRateMap"][key]["size"] += row["size"]
                        except:
                            transactions["feeRateMap"][key] = {"count": 1,
                                                               "size": row["size"],
                                                               "amount":  row["amount"]}
                    if row["rbf"]:
                        transactions["rbfCount"] += 1
                    if row["segwit"]:
                        transactions["segwitCount"] += 1
                    if row["size"]:
                        transactions["size"]["total"] += row["size"]
                        if transactions["size"]["max"]["value"] is None or \
                                transactions["size"]["max"]["value"] < row["size"]:
                            transactions["size"]["max"]["value"] = row["size"]
                            transactions["size"]["max"]["txId"] = rh2s(row["tx_id"])

                        if transactions["size"]["min"]["value"] is None or \
                                transactions["size"]["min"]["value"] > row["size"]:
                            transactions["size"]["min"]["value"] = row["size"]
                            transactions["size"]["min"]["txId"] = rh2s(row["tx_id"])


                async with self.db_pool.acquire() as conn:
                    await conn.execute("truncate table  mempool_dbs;")
                    await conn.execute("truncate table  mempool_dbs_childs;")
                    await conn.copy_records_to_table('mempool_dbs',
                                                     columns=["tx_id", "timestamp"],
                                                     records=dbs_records)

                    await conn.copy_records_to_table('mempool_dbs_childs',
                                                     columns=["tx_id", "timestamp"],
                                                     records=dbs_childs_records)
                    s_minute = int(time.time()) // 60
                    if s_minute % 60 == 0 and self.last_hour < s_minute // 60:
                        s_hour = s_minute // 60
                        self.last_hour = s_hour
                        if s_hour % 24 == 0 and self.last_day < s_hour // 24:
                            s_day = s_hour // 24
                            self.last_day = s_day
                        else:
                            s_day = None
                    else:
                        s_hour = None
                        s_day = None


                    await conn.execute("INSERT INTO mempool_analytica "
                                       "(minute, hour, day, inputs, outputs, transactions)"
                                       " VALUES "
                                       "($1, $2, $3, $4, $5, $6) "
                                       "ON CONFLICT (minute) "
                                       "DO UPDATE SET "
                                       " inputs = $4,"
                                       " outputs = $5, "
                                       " transactions = $6",
                                       s_minute,
                                       s_hour,
                                       s_day,
                                       json.dumps(inputs),
                                       json.dumps(outputs),
                                       json.dumps(transactions))

                if s_hour is not None:
                    self.log.warning("Mempool analytica hourly point saved %s" % s_hour)
                    self.log.info("Mempool transactions %s; STXO : %s; UTXO %s; DBS %s; round time %s;" %
                                   (transactions["count"],
                                    inputs["count"],
                                    outputs["count"],
                                    transactions["doublespend"]["count"], q))
                q = time.time() - q
                if q < 1:
                    await asyncio.sleep(1 - q)
                if q > 10:
                    self.log.warning("Mempool analytica is to slow %s" % q)

                if self.last_minute != s_minute:
                    self.last_minute = s_minute
                    self.log.debug("Mempool transactions %s; STXO : %s; UTXO %s; DBS %s; round time %s;" %
                                   (transactions["count"],
                                    inputs["count"],
                                    outputs["count"],
                                    transactions["doublespend"]["count"], q))


                # assert len(tx) == len(txsi)
                # assert len(tx) == len(txso)

            except asyncio.CancelledError:
                self.log.warning("Mempool analytica task canceled")
                break
            except Exception as err:
                self.log.error("Mempool analytica task error: %s" % err)
                await asyncio.sleep(10)




    def terminate(self, a, b):
        self.loop.create_task(self.terminate_coroutine())

    async def terminate_coroutine(self):
        self.active = False
        if self.task is not None:
            self.task.cancel()
            r = await asyncio.wait([self.task])
            try:
                r.result()
            except:
                pass
        self.log.info("Mempool analytica module stopped")
        self.loop.stop()
