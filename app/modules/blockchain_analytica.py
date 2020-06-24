import asyncio
import signal
from setproctitle import setproctitle
import asyncpg
from collections import deque
import time
import json
import math, traceback
from pybtc import rh2s, target_to_difficulty, var_int_to_int
from utils import format_bytes, format_vbytes, ListCache


class BlockchainAnalyticaAgregator():

    def __init__(self, dsn, logger):
        setproctitle('btcapi server: blockchain analytica')
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
            self.log.info("Blockchain analytica module started")
            self.task = self.loop.create_task(self.processor())
        except Exception as err:
            self.log.warning("Start blockchain analytica module failed: %s" % err)
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


    def init_stat(self):
        outputs = {"count": 0,
                   "amount": 0,
                   "typeMap": {}}
        inputs = {"count": 0,
                   "amount": 0,
                   "typeMap": {}}

        transactions = {"fee": {"max": {"value": None,
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
                                            "txId": None}
                                    },
                        "segwitCount": 0,
                        "rbfCount": 0,
                        "feeRateMap": {},
                        "count": 0}
        blockchain =   {
                          "size": {"total": 0,
                                   "max": {"value": None, "height": None},
                                   "min": {"value": None, "height": None}
                                   },
                          "transactions": {"count": {"total": 0,
                                                    "max":{"value": None, "height": None}
                                                    }},
                          "amount":  {"max": {"value": None, "height": None},
                                      "min": {"value": None, "height": None}},
                          "difficulty":  {"max": {"value": None, "height": None}},
                          "feeReward":  {"max": {"value": None, "height": None},
                                         "total": 0},
                          "blockEmissionReward":   0,
                          }

        return {"outputs": outputs,
                "inputs": inputs,
                "transactions": transactions,
                "blockchain": blockchain}

    async def processor(self):
        bstat = self.init_stat()
        height = -1
        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT height, blockchian_stat.blockchian as b  FROM  "
                                          "blockchian_stat  order by height desc LIMIT 1;")
            if row is not None:
                bstat = json.loads(row["b"])
                height = row["height"]
        except:
            print(traceback.format_exc())
        while True:
            try:

                rows = []
                async with self.db_pool.acquire() as conn:
                    async with conn.transaction():
                        row = await conn.fetchrow("SELECT height, blockchian_stat.blockchian as b FROM  "
                                                  "blockchian_stat  order by height desc LIMIT 1;")
                        if row is not None:
                            bstat = json.loads(row["b"])
                            height = row["height"]
                        rows = await conn.fetch("SELECT blocks.height, blocks.hash, blocks.header, "
                                                "block_stat.block, blocks.data FROM  "
                                                "block_stat  "
                                                "JOIN blocks "
                                                "ON block_stat.height = blocks.height "
                                                "where block_stat.height > $1 ORDER BY block_stat.height "
                                                "LIMIT 10000;", height)

                if rows:
                    stat_records = []
                    for row in rows:
                        block = json.loads(row["block"])
                        data = json.loads(row["data"])

                        # outputs
                        outs_stat = block["outputs"]

                        bstat["outputs"]["count"] += outs_stat["count"]
                        bstat["outputs"]["amount"] += outs_stat["amount"]["total"]

                        for key in outs_stat["typeMap"]:
                            try:
                                bstat["outputs"]["typeMap"][key]["count"] += outs_stat["typeMap"][key]["count"]
                                bstat["outputs"]["typeMap"][key]["amount"] += outs_stat["typeMap"][key]["amount"]
                            except:
                                bstat["outputs"]["typeMap"][key]= {"count": 1,
                                                                   "amount": outs_stat["typeMap"][key]["amount"],
                                                                   "amountMap": {}}

                            for akey in outs_stat["typeMap"][key]["amountMap"]:
                                try:
                                    bstat["outputs"]["typeMap"][key]["amountMap"][akey]["count"] += 1
                                    bstat["outputs"]["typeMap"][key]["amountMap"][akey]["amount"] += outs_stat["typeMap"][key]["amountMap"][akey]["amount"]
                                except:
                                    bstat["outputs"]["typeMap"][key]["amountMap"][akey] = {"count": 1,
                                                                                           "amount": outs_stat["typeMap"][key]["amountMap"][akey]["amount"]}

                        # inputs
                        in_stats = block["inputs"]

                        bstat["inputs"]["count"] += in_stats["count"]
                        bstat["inputs"]["amount"] += in_stats["amount"]["total"]

                        for key in in_stats["typeMap"]:
                            a = in_stats["typeMap"][key]["amount"]
                            try:
                                bstat["inputs"]["typeMap"][key]["count"] += in_stats["typeMap"][key]["count"]
                                bstat["inputs"]["typeMap"][key]["amount"] += a
                            except:
                                bstat["inputs"]["typeMap"][key] = {"count": 1,
                                                                   "amount": a,
                                                                   "amountMap": {}}

                            for akey in in_stats["typeMap"][key]["amountMap"]:
                                a = in_stats["typeMap"][key]["amountMap"][akey]["amount"]
                                c = in_stats["typeMap"][key]["amountMap"][akey]["count"]
                                try:
                                    bstat["inputs"]["typeMap"][key]["amountMap"][akey]["count"] += c
                                    bstat["inputs"]["typeMap"][key]["amountMap"][akey]["amount"] += a
                                except:
                                    bstat["inputs"]["typeMap"][key]["amountMap"] = {"count": c,
                                                                                    "amount": a}
                       #transactions
                        tx_stats = block["transactions"]
                        bstat["transactions"]["count"] += tx_stats["count"]
                        bstat["transactions"]["segwitCount"] += tx_stats["typeMap"]["segwit"]["count"]
                        bstat["transactions"]["rbfCount"] += tx_stats["typeMap"]["rbf"]["count"]

                        keys = ("amount", "fee", "size", "vSize", "feeRate")

                        for key in keys:
                            try:
                                bstat["transactions"][key]["total"] += tx_stats[key]["total"]
                            except:
                                pass

                            a = tx_stats[key]["max"]["value"]
                            t = tx_stats[key]["max"]["txId"]

                            if a is not None:
                                if bstat["transactions"][key]["max"]["value"] is None or \
                                        bstat["transactions"][key]["max"]["value"] < a:
                                    bstat["transactions"][key]["max"]["value"] = a
                                    bstat["transactions"][key]["max"]["txId"] = t


                        for x_key in ("feeRateMap",):

                            for key in tx_stats[x_key]:
                                try:
                                    bstat["transactions"][x_key][key]["count"] += 1
                                    bstat["transactions"][x_key][key]["vSize"] += tx_stats[x_key][key]["vSize"]
                                    bstat["transactions"][x_key][key]["size"] += tx_stats[x_key][key]["size"]
                                except:
                                    bstat["transactions"][x_key][key] = {"count": 1,
                                                                         "vSize": tx_stats[x_key][key]["vSize"],
                                                                         "size": tx_stats[x_key][key]["size"]}

                        # blocks
                        bstat["blockchain"]["feeReward"]["total"] += bstat["transactions"]["fee"]["total"]
                        a = bstat["transactions"]["fee"]["total"]
                        if bstat["blockchain"]["feeReward"]["max"]["value"] is None or \
                                bstat["blockchain"]["feeReward"]["max"]["value"] < a:
                            bstat["blockchain"]["feeReward"]["max"]["value"] = a
                            bstat["blockchain"]["feeReward"]["max"]["height"] = row["height"]

                        d = target_to_difficulty(int.from_bytes(row["hash"], byteorder="little"))

                        if bstat["blockchain"]["difficulty"]["max"]["value"] is None or \
                                bstat["blockchain"]["difficulty"]["max"]["value"] < d:
                            bstat["blockchain"]["difficulty"]["max"]["value"] = d
                            bstat["blockchain"]["difficulty"]["max"]["height"] = row["height"]

                        bstat["blockchain"]["blockEmissionReward"] += block["outputs"]["amount"]["total"] -  block["inputs"]["amount"]["total"]

                        bstat["blockchain"]["size"]["total"] += data["size"]
                        txc = var_int_to_int(row["header"][80:])
                        bstat["blockchain"]["transactions"]["count"]["total"] += txc

                        if bstat["blockchain"]["transactions"]["count"]["max"]["value"] is None or \
                                bstat["blockchain"]["transactions"]["count"]["max"]["value"] < txc:
                            bstat["blockchain"]["transactions"]["count"]["max"]["value"] = txc
                            bstat["blockchain"]["transactions"]["count"]["max"]["height"] = row["height"]

                        stat_records.append([row["height"], json.dumps(bstat)])

                    async with self.db_pool.acquire() as conn:
                        async with conn.transaction():
                            check_height = await conn.fetchval("SELECT height FROM  "
                                                      "blockchian_stat  order by height desc LIMIT 1 FOR UPDATE;")
                            if check_height is None:
                                check_height = -1

                            if check_height != height:
                                continue

                            await conn.copy_records_to_table('blockchian_stat',
                                                             columns=["height", "blockchian"], records=stat_records)
                            if stat_records:
                                self.log.info("Blockchain statistics  +%s "
                                              "blocks last block %s" % (len(stat_records), height + len(stat_records)))
                else:
                    await asyncio.sleep(1)


            except asyncio.CancelledError:
                self.log.warning("Blockchain analytica task canceled")
                break
            except Exception as err:
                self.log.error("Blockchain analytica task error: %s" % err)
                print(traceback.format_exc())

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
        self.log.info("Blockchain analytica module stopped")
        self.loop.stop()
