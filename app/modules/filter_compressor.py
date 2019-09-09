import asyncio
import signal
from setproctitle import setproctitle
import asyncpg
from collections import deque
from pybtc import bytes_to_int, create_gcs, int_to_c_int, sha256
from pybtc import c_int_to_int, c_int_len, decode_gcs
import time
import traceback
import zlib

class FilterCompressor():

    def __init__(self, dsn, option_block_filter_fps, option_block_filter_bits, option_coinbase_maturity, logger):
        setproctitle('btcapi server: filter compressor')
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.dsn = dsn
        self.option_block_filter_fps = option_block_filter_fps
        self.option_block_filter_bits = option_block_filter_bits
        self.option_coinbase_maturity = option_coinbase_maturity
        self.option_coinbase_maturity = 100
        self.log = logger

        self.active = True
        self.db_pool = None
        self.start_time = time.time()
        self.last_pointer = 0

        self.compressor_task = None
        self.loop = asyncio.get_event_loop()
        signal.signal(signal.SIGTERM, self.terminate)
        self.loop.create_task(self.start())
        self.loop.run_forever()


    async def start(self):
        try:
            self.db_pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=1)
            self.log.info("Filter compressor module started")
            self.compressor_task = self.loop.create_task(self.compressor())
        except Exception as err:
            self.log.warning("Start filter compressor module failed: %s" % err)
            await asyncio.sleep(3)
            self.loop.create_task(self.start())


    async def compressor(self):
        accum = set()
        bootstrap_completed = False
        bf = 0
        delete_records = deque()
        filter_records = deque()
        filter_checkpoints_records = deque()

        while True:
            try:
                if not bootstrap_completed:
                    async with self.db_pool.acquire() as conn:
                        bf = await conn.fetchval("SELECT height  FROM block_filters  ORDER BY height DESC LIMIT 1;")
                        bfc = await conn.fetchval("SELECT height  FROM block_range_filters "
                                                  "ORDER BY height DESC LIMIT 1;")
                        if bf is None: bf = -1
                        if bfc is None: bfc = 0

                        if bfc < bf:
                            self.log.info("Bootstrap block filters checkpoints ...")
                            rows = await conn.fetch("SELECT filter, height  FROM block_filters WHERE height > $1;", bfc)
                            accum = set()

                            for row in rows:
                                f = row["filter"]
                                f = zlib.decompress(f)
                                N = c_int_to_int(f)
                                i = c_int_len(N)

                                f = decode_gcs(f[i:], N, P=self.option_block_filter_bits)
                                accum = accum.union(f)
                                await asyncio.sleep(0)



                        bootstrap_completed = True



                async with self.db_pool.acquire() as conn:
                    bf = await conn.fetchval("SELECT height  FROM block_filters "
                                             "ORDER BY height DESC LIMIT 1;")
                    if bf is None: bf = -1
                    bfu = await conn.fetchval("SELECT height  FROM block_filters_uncompressed "
                                              "ORDER BY height DESC LIMIT 1;")
                    if bfu is None: bfu = 0
                    if (bfu - bf) <= (144 + self.option_coinbase_maturity):
                        await asyncio.sleep(10)
                        continue

                    rows = await conn.fetch("SELECT height, filter  "
                                            "FROM block_filters_uncompressed "
                                            "WHERE height > $1 and height <= $1 + 144;", bf)

                if len(rows) != 144:
                    continue

                t = set()
                for row in rows:
                    delete_records.append(row["height"])
                    f = row["filter"]
                    hash_list = set([bytes_to_int(f[i:i+8], byteorder="big") for i in range(0, len(f), 8)])

                    f = create_gcs(hash_list, hashed=True,
                                   M=self.option_block_filter_fps,
                                   P=self.option_block_filter_bits ,hex=0)
                    f = int_to_c_int(len(hash_list)) + f
                    filter_hash = sha256(f, hex=0)
                    t = t.union(hash_list)
                    filter_data = zlib.compress(f, level=9)
                    filter_records.append((row["height"], filter_data, filter_hash))

                accum = accum.union(t)

                if len(accum) >= 5000000:
                    f = create_gcs(accum, hashed=True,
                                   M=self.option_block_filter_fps,
                                   P=self.option_block_filter_bits, hex=0)
                    f = int_to_c_int(len(accum)) + f
                    filter_hash = sha256(f, hex=0)
                    filter_data = zlib.compress(f, level=9)
                    filter_checkpoints_records.append((row["height"], filter_data, filter_hash))
                    accum = set()


                async with self.db_pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.copy_records_to_table('block_filters',
                                                         columns=["height", "filter", "hash"],
                                                         records=filter_records)
                        await conn.execute("DELETE FROM block_filters_uncompressed "
                                           "WHERE height = ANY($1);", delete_records)
                        if filter_checkpoints_records:
                            await conn.copy_records_to_table('block_range_filters',
                                                             columns=["height", "filter", "hash"],
                                                             records=filter_checkpoints_records)
                    delete_records = deque()
                    filter_records = deque()
                    filter_checkpoints_records = deque()

            except asyncio.CancelledError:
                self.log.warning("Filter compressor module canceled")
                break

            except Exception as err:
                self.log.error("filter compressor error: %s" % bf)
                print(traceback.format_exc())
                delete_records = deque()

                filter_records = deque()
                filter_checkpoints_records = deque()
                await asyncio.sleep(10)






    def terminate(self, a, b):
        self.loop.create_task(self.terminate_coroutine())

    async def terminate_coroutine(self):
        self.active = False
        if self.compressor_task:
            self.compressor_task.cancel()
            r = await self.compressor_task
            try: r.result()
            except: pass
        self.log.info("Filter compressor module stopped")
        self.loop.stop()
