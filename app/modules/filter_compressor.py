import asyncio
import signal
from setproctitle import setproctitle
import asyncpg
from collections import deque
from pybtc import bytes_to_int, create_gcs, int_to_c_int
import time
import traceback
import zlib

class FilterCompressor():

    def __init__(self, dsn, option_block_filter_fps, option_block_filter_bits, logger):
        setproctitle('btcapi server: filter compressor')
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.dsn = dsn
        self.option_block_filter_fps = option_block_filter_fps
        self.option_block_filter_bits = option_block_filter_bits
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

    async def get_tx_map_batch(self, offset, limit):
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT pointer, address, amount FROM transaction_map "
                                    "WHERE pointer > $1 OFFSET $2 LIMIT $3;", self.last_pointer,
                                    offset * limit, limit)
        return rows


    async def compressor(self):
        accum = set()
        accum2 = set()
        while True:

            try:
                async with self.db_pool.acquire() as conn:
                    rows = await conn.fetch("SELECT height, filter  "
                                            "FROM block_filters_uncompressed "
                                            "ORDER BY height ASC LIMIT 100;")
                if not rows:
                    await asyncio.sleep(10)
                    continue

                delete_records = deque()
                filter_records = deque()
                filter_checkpoints_records = deque()
                filter_checkpoints_records2 = deque()

                for row in rows:
                    delete_records.append(row["height"])
                    f = row["filter"]
                    h = set([bytes_to_int(f[i:i+8], byteorder="big") for i in range(0, len(f), 8)])

                    f = create_gcs(h, hashed=True,
                                   M=self.option_block_filter_fps,
                                   P=self.option_block_filter_bits ,hex=0)
                    accum = accum.union(h)
                    accum2 = accum2.union(h)

                    filter_records.append((row["height"], int_to_c_int(len(h)) + zlib.compress(f)))
                print(">>", len(accum))
                if len(accum) >= 500000:
                    f = create_gcs(accum, hashed=True,
                                   M=self.option_block_filter_fps,
                                   P=self.option_block_filter_bits, hex=0)
                    filter_checkpoints_records.append((row["height"], int_to_c_int(len(accum))+ zlib.compress(f)))
                    accum = set()

                if len(accum2) >= 1000000:
                    f = create_gcs(accum2, hashed=True,
                                   M=self.option_block_filter_fps,
                                   P=self.option_block_filter_bits, hex=0)
                    filter_checkpoints_records2.append((row["height"], int_to_c_int(len(accum2))+ zlib.compress(f)))
                    accum2 = set()
                async with self.db_pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.copy_records_to_table('block_filters',
                                                         columns=["height", "filter"],
                                                         records=filter_records)
                        await conn.execute("DELETE FROM block_filters_uncompressed "
                                           "WHERE height = ANY($1);", delete_records)
                        if filter_checkpoints_records:
                            await conn.copy_records_to_table('block_filters_checkpoints',
                                                             columns=["height", "filter"],
                                                             records=filter_checkpoints_records)
                        if filter_checkpoints_records2:
                            await conn.copy_records_to_table('block_filters_checkpoints2',
                                                             columns=["height", "filter"],
                                                             records=filter_checkpoints_records2)
            except asyncio.CancelledError:
                self.log.warning("Filter compressor module canceled")
                break

            except:
                print(traceback.format_exc())
                try:
                    await self.db_pool.close()
                except:
                    pass
                await asyncio.sleep(5)
                try:
                    self.db_pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=1)
                except:
                    pass






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
