import asyncio
import signal
from setproctitle import setproctitle
import asyncpg
from collections import deque
import time
import json
import math
from pybtc import rh2s
from utils import format_bytes, format_vbytes, ListCache


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
                        "vSize": {"max": {"value": None,
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
                                            "txId": None},
                                    "best": 1,
                                    "best4h": 1,
                                    "bestHourly": 1
                                    },
                        "segwitCount": 0,
                        "rbfCount": 0,
                        "doublespend": {"count": 0, "size": 0, "vSize": 0, "amount": 0},
                        "doublespendChilds": {"count": 0, "size": 0, "vSize": 0, "amount": 0},
                        "feeRateMap": {},
                        "count": 0}
        return outputs, inputs, transactions

    async def processor(self):
        utxo_sequence = 0
        stxo_sequence = 0
        tx_sequence = 0
        best_fee = 1
        dbs = set()
        dbs_childs = set()
        outputs, inputs, transactions = self.refresh_stat()
        truncate_dbs_table = True

        best_fee_hourly =ListCache(60 * 60)
        best_fee_4h =ListCache(60 * 60 * 4)
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT minute, transactions->'feeRate'->'best' as best FROM  mempool_analytica "
                                    "order by minute desc LIMIT 240;")
        c = 0
        for row in rows:
            if row["best"] is not None:
                if c < 60:
                    best_fee_hourly.set(float(row["best"]))
                best_fee_4h.set(float(row["best"]))
            c += 1

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
                            truncate_dbs_table = True

                        stxo = await conn.fetch("SELECT tx_id, out_tx_id, address, amount, pointer, sequence, outpoint,  id  "
                                                "FROM connector_unconfirmed_stxo "
                                                "WHERE id > $1;", stxo_sequence)
                        utxo = await conn.fetch("SELECT out_tx_id as tx_id, address, amount, id "
                                                "FROM connector_unconfirmed_utxo "
                                                "WHERE id > $1;", utxo_sequence)
                        tx = await conn.fetch("SELECT tx_id, size, b_size, rbf, fee, "
                                              "amount, segwit, timestamp, id  FROM unconfirmed_transaction "
                                              "WHERE id > $1;", tx_sequence)
                        row = await conn.fetchval("select min(feerate)  "
                                                    "from (select feerate, sum((size + b_size * 4)/4) "
                                                    "over (order by feerate desc) as block "
                                                    "from unconfirmed_transaction) t where block <= 920000;")
                        if row is not None:
                            best_fee = row

                txsi = set()
                txso = set()
                dbs_outs = set()
                dbs_set = set()
                inputs["count"] += len(stxo)
                for row in stxo:
                    if stxo_sequence < row["id"]:
                        stxo_sequence = row["id"]
                    if row["sequence"] > 0:
                        dbs_outs.add(row["outpoint"])
                        dbs_set.add(row["tx_id"])
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



                async with self.db_pool.acquire() as conn:
                    dbs_rows = await conn.fetch("SELECT tx_id, outpoint  "
                                            "FROM connector_unconfirmed_stxo "
                                            "WHERE outpoint = ANY($1);", dbs_outs)
                    out_map= set()


                    for row in dbs_rows:
                        if row["outpoint"] in out_map:
                            if row["tx_id"] in dbs_set:
                                dbs.add(row["tx_id"])
                        else:
                            out_map.add(row["outpoint"])


                l_dbs_size = 0
                while True:
                    for row in stxo:
                        if row["out_tx_id"] in dbs or row["out_tx_id"] in dbs_childs:
                            if row["tx_id"] not in dbs:
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


                for row in tx:
                    v_size = math.ceil((row["b_size"] * 3 + row["size"]) / 4)
                    if tx_sequence < row["id"]:
                        tx_sequence = row["id"]
                    if row["tx_id"] in dbs:
                        transactions["doublespend"]["amount"] += row["amount"]
                        transactions["doublespend"]["size"] += row["size"]
                        transactions["doublespend"]["vSize"] += v_size
                        dbs_records.append((row["tx_id"], row["timestamp"], 0))
                    if row["tx_id"] in dbs_childs:
                        transactions["doublespendChilds"]["amount"] += row["amount"]
                        transactions["doublespendChilds"]["size"] += row["size"]
                        transactions["doublespendChilds"]["vSize"] += v_size
                        dbs_records.append((row["tx_id"], row["timestamp"], 1))

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
                            key = math.floor(key / 2) * 2
                        elif key > 20 and  key < 200:
                            key = math.floor(key / 10) * 10
                        elif key > 200:
                            key = math.floor(key / 25) * 25
                        try:
                            transactions["feeRateMap"][key]["count"] += 1
                            transactions["feeRateMap"][key]["size"] += row["size"]
                            transactions["feeRateMap"][key]["vSize"] += v_size
                        except:
                            transactions["feeRateMap"][key] = {"count": 1,
                                                               "size": row["size"],
                                                               "vSize": v_size}


                    if row["rbf"]:
                        transactions["rbfCount"] += 1
                    if row["segwit"]:
                        transactions["segwitCount"] += 1
                    if row["size"]:
                        transactions["size"]["total"] += row["size"]
                        transactions["vSize"]["total"] += v_size
                        if transactions["size"]["max"]["value"] is None or \
                                transactions["size"]["max"]["value"] < row["size"]:
                            transactions["size"]["max"]["value"] = row["size"]
                            transactions["size"]["max"]["txId"] = rh2s(row["tx_id"])

                        if transactions["vSize"]["max"]["value"] is None or \
                                transactions["vSize"]["max"]["value"] < v_size:
                            transactions["vSize"]["max"]["value"] = v_size
                            transactions["vSize"]["max"]["txId"] = rh2s(row["tx_id"])


                        if transactions["size"]["min"]["value"] is None or \
                                transactions["size"]["min"]["value"] > row["size"]:
                            transactions["size"]["min"]["value"] = row["size"]
                            transactions["size"]["min"]["txId"] = rh2s(row["tx_id"])

                        if transactions["vSize"]["min"]["value"] is None or \
                                transactions["vSize"]["min"]["value"] > v_size:
                            transactions["vSize"]["min"]["value"] = v_size
                            transactions["vSize"]["min"]["txId"] = rh2s(row["tx_id"])

                if transactions["vSize"]["total"] > 1000000:
                    transactions["feeRate"]["best"] = round(best_fee, 2)
                else:
                    transactions["feeRate"]["best"] = 1



                async with self.db_pool.acquire() as conn:
                    async with conn.transaction():
                        if truncate_dbs_table:
                            await conn.execute("truncate table  mempool_dbs;")
                            truncate_dbs_table = False
                        await conn.copy_records_to_table('mempool_dbs',
                                                         columns=["tx_id", "timestamp", "child"],
                                                         records=dbs_records)

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

                        if self.last_minute != s_minute or transactions["feeRate"]["bestHourly"] == 1:
                            best_fee_hourly.set(transactions["feeRate"]["best"])
                            f = 0
                            for i in best_fee_hourly.items:
                                f += i
                            f4 = 0
                            for i in best_fee_4h.items:
                                f4 += i
                            transactions["feeRate"]["bestHourly"] = round(f / len(best_fee_hourly.items), 2)
                            transactions["feeRate"]["best4h"] = round(f4 / len(best_fee_4h.items), 2)

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
                                    transactions["doublespend"]["count"] +
                                    transactions["doublespendChilds"]["count"], q))
                q = time.time() - q
                if q < 1:
                    await asyncio.sleep(1 - q)
                if q > 10:
                    self.log.warning("Mempool analytica is to slow %s" % q)

                if self.last_minute != s_minute or transactions["feeRate"]["best4h"] == 1:
                    self.last_minute = s_minute
                    self.log.debug("Mempool TX %s; STXO %s; UTXO %s; DBS %s; %s; %s; Best fee  %s/%s/%s; Round time %s;" %
                                   (transactions["count"],
                                    inputs["count"],
                                    outputs["count"],
                                    transactions["doublespend"]["count"] +
                                    transactions["doublespendChilds"]["count"],
                                    format_bytes(transactions["size"]["total"]),
                                    format_vbytes(transactions["vSize"]["total"]),
                                    transactions["feeRate"]["best"],
                                    transactions["feeRate"]["bestHourly"],
                                    transactions["feeRate"]["best4h"],
                                    round(q,4)))


                # assert len(tx) == len(txsi)
                # assert len(tx) == len(txso)
                #
                # async with self.db_pool.acquire() as conn:
                #     v = await conn.fetch("SELECT invalid_transaction.tx_id FROM  invalid_transaction "
                #                                 " JOIN connector_unconfirmed_stxo ON connector_unconfirmed_stxo.tx_id = invalid_transaction.tx_id "
                #                             " ;")
                #     k = [t["tx_id"] for t in v]
                #     for t in v:
                #         print(rh2s(t["tx_id"]))
                #     v = await conn.fetch("SELECT  outpoint, sequence FROM  connector_unconfirmed_stxo WHERE tx_id = ANY($1);", k)
                #     print("u", len(v))
                #     uu = set()
                #     pp = set()
                #     for r in v:
                #         uu.add(r["outpoint"])
                #         pp.add((r["outpoint"], r["sequence"]))
                #     v = await conn.fetch("SELECT  outpoint, sequence FROM  invalid_stxo WHERE tx_id = ANY($1);", k)
                #     print("i", len(v))
                #     ii = set()
                #     for r in v:
                #         ii.add((r["outpoint"], r["sequence"]))
                #     e = 0
                #     for i in ii:
                #         if i[0] not in uu:
                #             print("none", i[1])
                #         else:
                #             e += 1
                #     print(">>", e)
                #
                #     v = await conn.fetch("SELECT  count(*)  from connector_unconfirmed_utxo WHERE out_tx_id = ANY($1);", k)
                #     print("connector_unconfirmed_utxo", v)
                #     v = await conn.fetch("SELECT  count(*)  from unconfirmed_transaction WHERE tx_id = ANY($1);", k)
                #     print("unconfirmed_transaction", v)
                #     v = await conn.fetch("SELECT  count(*)  from unconfirmed_transaction_map WHERE tx_id = ANY($1);", k)
                #     print("unconfirmed_transaction_map", v)
                #     ff = 0
                #     for i in pp:
                #         v = await conn.fetchval("SELECT  count(*)  from invalid_stxo WHERE outpoint = $1 and sequence = $2;", i[0], i[1])
                #         ff += v
                #     print("ff", ff)
                #     ll = list()
                #     v = await conn.fetch("SELECT  outpoint, sequence, out_tx_id, tx_id, input_index, address, amount, pointer from connector_unconfirmed_stxo WHERE tx_id = ANY($1);", k)
                #     for i in v:
                #         ll.append((i["outpoint"],
                #                 i["sequence"],
                #                 i["out_tx_id"],
                #                 i["tx_id"],
                #                 i["input_index"],
                #                 i["address"],
                #                 i["amount"],
                #                 i["pointer"],
                #                 ))
                #     print("ll", len(ll))
                #     try:
                #         # await conn.copy_records_to_table('invalid_stxo',
                #         #                                  columns=["outpoint",
                #         #                                           "sequence",
                #         #                                           "out_tx_id",
                #         #                                           "tx_id",
                #         #                                           "input_index",
                #         #                                           "address",
                #         #                                           "amount",
                #         #                                           "pointer",],
                #         #                                  records=ll)
                #         # print("iok")
                #          ###v = await conn.fetch("DELETE  FROM  connector_unconfirmed_stxo WHERE tx_id = ANY($1);", k)
                #     except Exception as err:
                #         print(err)
                #     await asyncio.sleep(50000)





                #     v = await conn.fetch("DELETE  FROM  unconfirmed_transaction_map WHERE tx_id = ANY($1);", k)
                #     print(v)
                #     # v = await conn.fetch("DELETE  FROM  connector_unconfirmed_stxo WHERE tx_id = ANY($1);", k)
                #     # print(v)
                # v = await conn.fetch("SELECT  tx_id FROM  connector_unconfirmed_stxo WHERE tx_id = ANY($1);", k)
                # print(v)
                # v = await conn.fetch("SELECT  out_tx_id FROM  connector_unconfirmed_utxo WHERE out_tx_id = ANY($1);", k)
                # print(v)
                # v = await conn.fetch("DELETE  FROM  connector_unconfirmed_utxo WHERE out_tx_id = ANY($1);", k)
                # print(v)
                # v = await conn.fetch("SELECT  out_tx_id FROM  connector_unconfirmed_utxo WHERE out_tx_id = ANY($1);", k)
                # print(v)
                # if v == []:
                #     await conn.fetch("DELETE  FROM  unconfirmed_transaction WHERE tx_id = ANY($1);", k)
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
