import asyncio
import signal
import traceback
from setproctitle import setproctitle
import asyncpg
import datetime
from collections import deque
from utils import chunks, deserialize_address_data, serialize_address_data

from pybtc import MRU
import time
import json
from math import *
import sys


class AddressStateSync():

    def __init__(self, dsn, bootstrap_height, timeline, blockchain_analytica, logger):
        setproctitle('btcapi server: address state sync')
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.dsn = dsn
        self.new_address = MRU()
        self.day = 0
        self.month = 0
        self.address_daily = dict()
        self.address_monthly = dict()
        self.existed_address = MRU()
        self.blocks_map_time = dict()
        self.log = logger
        self.threads = 5
        self.cache_limit = 15000000
        self.active = True
        self.db_pool = None
        self.synchronization_task = None
        self.bootstrap_completed = False
        self.start_time = time.time()
        self.last_block = None
        self.batch_last_block = 0
        self.load = set()
        self.last_pointer = 0
        self.bootstrap_height = bootstrap_height
        self.tx_map_limit = 100000
        self.exist = set()
        self.timeline = timeline
        self.blockchain_analytica = blockchain_analytica
        self.saved_records = 0
        self.saved_daily_records = 0
        self.saved_monthly_records = 0
        self.updated_records = 0
        self.timeline_records = 0
        self.loaded_addresses = 0
        self.requested_addresses = 0
        self.saved_address_stat_records = 0
        self.block_addresses_stat = {
            "inputs": {"count": {"total": 0, "reused": 0},
                       "amount": {"map": {"count": dict(),
                                          "amount": dict()}}

                       },
            "outputs": {"count": {"total": 0, "new": 0},
                        "amount": {"map": {"count": dict(),
                                           "amount": dict()}}
                        }
        }

        self.blockchain_addresses_stat = {
            "count": {"total": 0,
                      "inputs": 0,
                      "outputs": 0},
            "neverUsed": {
                "count": {"total": 0},
                "amount": {"total": 0},
                "type": {"map": {"count": dict(),
                                 "amount": dict()}}
                },
            "amount": {
                "total": 0,
                "map": {"count": dict(),
                        "amount": dict()}
                       },
            "receivedTxCount": {"map": {"count": dict()}},
            "sentTxCount": {"map": {"count": dict()}},
            "txCount": {"map": {"count": dict()}},
        }

        self.loop = asyncio.get_event_loop()
        signal.signal(signal.SIGTERM, self.terminate)
        self.loop.create_task(self.start())
        self.loop.run_forever()


    async def start(self):
        try:
            self.db_pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=30)
            self.log.info("Address state sync module started")
            self.synchronization_task = self.loop.create_task(self.synchronization())
        except Exception as err:
            self.log.warning("Start address state sync module failed: %s" % err)
            await asyncio.sleep(3)
            self.loop.create_task(self.start())

    async def is_synchronized(self):
        async with self.db_pool.acquire() as conn:
            if self.last_block is None:
                await conn.execute("TRUNCATE TABLE address;")
                await conn.execute("TRUNCATE TABLE address_payments;")
                await conn.execute("TRUNCATE TABLE address_daily;")
                await conn.execute("TRUNCATE TABLE address_monthly;")
                await conn.execute("TRUNCATE TABLE blocks_addresses_stat;")
                await conn.execute("""
                                      UPDATE service SET value = $1
                                      WHERE name =  'address_last_block';
                                   """, str(0))
                self.last_block = 0
            # self.last_block = await conn.fetchval("SELECT value FROM service WHERE "
            #                                       "name = 'address_last_block' LIMIT 1;")
            # self.last_block = int(self.last_block)
            a = await conn.fetchval("SELECT value FROM service WHERE "
                                                  "name = 'bootstrap_completed' LIMIT 1;")
            if a == '1':
                self.bootstrap_completed = True
                return True

            return False

    async def get_tx_map_batch(self, offset, limit):
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT pointer, address, amount FROM transaction_map "
                                    "WHERE pointer > $1 OFFSET $2 LIMIT $3;", self.last_pointer,
                                    offset * limit, limit)
        return rows


    async def get_tx_map(self):
        rows = list()
        if not self.bootstrap_completed:
            c = ceil(self.tx_map_limit/self.threads)
            tasks = [self.loop.create_task(self.get_tx_map_batch(i, c)) for i in range(self.threads)]
            await asyncio.wait(tasks)
            for pt in tasks:
                for r in pt.result():
                    rows.append(r)
        else:
            # complete recent block
            async with self.db_pool.acquire() as conn:
                rw = await conn.fetch("SELECT pointer, address, amount FROM transaction_map "
                                        "WHERE pointer > $1 and pointer < $2;", self.last_pointer,
                                        (((self.last_pointer >> 39)+1) << 39))
            [rows.append(r) for r in rw]
        affected_blocks = set()

        load_add = self.load.add

        for row in rows:
            if not self.new_address.has_key(row["address"]) \
                and not self.existed_address.has_key(row["address"]):
                load_add(row["address"])
            affected_blocks.add(row["pointer"] >> 39)

        if rows:
            self.last_pointer = row["pointer"]



        if affected_blocks:
            async with self.db_pool.acquire() as conn:
                xrows = await conn.fetch("SELECT height, adjusted_timestamp  FROM blocks "
                                        "WHERE height = ANY($1);", affected_blocks)
            self.blocks_map_time = dict()
            for row in xrows:
                self.blocks_map_time[row["height"]] = row["adjusted_timestamp"]
        return rows

    async def load_addresses_batch(self, batch):
        async with self.db_pool.acquire() as conn:
            a_rows = await conn.fetch("""
                                         SELECT  address, data FROM address  
                                         WHERE address = ANY($1);
                                      """, batch)
            for row in a_rows:
                self.existed_address[row["address"]] = row["data"]
        return len(a_rows)

    async def load_addresses(self):
        self.requested_addresses += len(self.load)
        batches = chunks(self.load, self.threads, 20000)
        tasks = [self.loop.create_task(self.load_addresses_batch(b)) for b in batches]
        count = 0
        if tasks:
            await asyncio.wait(tasks)
            for task in tasks:
                count += task.result()

        self.loaded_addresses += count

    async def synchronization(self):
        new_records = deque()
        existed_records = deque()
        address_payments = deque()
        daily_records = deque()
        monthly_records = deque()
        address_stat_records = deque()
        while (self.last_block is None or self.last_block < self.bootstrap_height) \
                and not self.bootstrap_completed:

            try:
                self.load = set()
                s = self.loop.create_task(self.save_records(new_records))
                u = self.loop.create_task(self.update_records(existed_records))
                h = self.loop.create_task(self.save_timeline_records(address_payments))
                d = self.loop.create_task(self.save_daily_records(daily_records))
                mr = self.loop.create_task(self.save_monthly_records(monthly_records))
                ad = self.loop.create_task(self.save_address_stat_records(address_stat_records))

                is_synchronized = await self.is_synchronized()
                if is_synchronized:
                    await asyncio.wait([s, u, h, d, mr, ad])
                    new_records = deque()
                    existed_records = deque()
                    address_payments = deque()
                    daily_records = deque()
                    monthly_records = deque()
                    address_stat_records = deque()
                    continue

                # get tx_map records
                m = self.loop.create_task(self.get_tx_map())
                await asyncio.wait([s, u, h, d, mr, ad, m])
                tx_map = m.result()

                new_records = deque()
                existed_records = deque()
                address_payments = deque()
                daily_records = deque()
                monthly_records = deque()
                address_stat_records = deque()

                if not tx_map:
                    await asyncio.sleep(10)
                    continue
                if len(self.new_address) + len(self.existed_address) >= self.cache_limit:
                    await self.load_addresses()

                address_payments  = deque()


                # update cached addresses state
                for row in tx_map:
                    block_height = row["pointer"] >> 39


                    while self.last_block < block_height:
                        if self.blockchain_analytica:
                            block_stat = json.dumps(self.block_addresses_stat)
                            blockchain_stat = json.dumps(self.blockchain_addresses_stat)
                            address_stat_records.append((block_height, block_stat, blockchain_stat))
                            self.block_addresses_stat = {
                                "inputs": {"count": {"total": 0, "reused": 0},
                                           "amount": {"map": {"count": dict(),
                                                              "amount": dict()}}

                                           },
                                "outputs": {"count": {"total": 0, "new": 0},
                                            "amount": {"map": {"count": dict(),
                                                               "amount": dict()}}
                                            }
                            }
                        self.last_block += 1



                    day = ceil(self.blocks_map_time[block_height] / 86400)
                    d = datetime.datetime.fromtimestamp(self.blocks_map_time[block_height],
                                                        datetime.timezone.utc)
                    month = 12 * d.year + d.month

                    if self.day < day:
                        for a in self.address_daily:
                            daily_records.append((a, self.day, self.address_daily[a]))
                        self.address_daily = dict()
                        self.day = day

                    if self.month < month:
                        for a in self.address_monthly:
                            monthly_records.append((a, self.month, self.address_monthly[a]))
                        self.address_monthly = dict()
                        self.month = month

                    try:
                        try:
                            rc, ra, c, sc, sa, dc = deserialize_address_data(self.new_address[row["address"]])
                            if not row["pointer"] & 524288:
                                # receiving tx
                                v = serialize_address_data(rc, ra, c, sc + 1, sa + row["amount"], dc + 1)
                                self.new_address[row["address"]] = v
                            else:
                                # sending tx
                                v = serialize_address_data(rc + 1, ra + row["amount"], c + 1, sc, sa, dc)
                                self.new_address[row["address"]] = v
                        except:
                            rc, ra, c, sc, sa, dc = deserialize_address_data(self.existed_address[row["address"]])
                            if not row["pointer"] & 524288:
                                # receiving tx
                                v = serialize_address_data(rc, ra, c, sc + 1, sa + row["amount"], dc + 1)
                                self.existed_address[row["address"]] = v
                            else:
                                # sending tx
                                v = serialize_address_data(rc + 1, ra + row["amount"], c + 1, sc, sa, dc)
                                self.existed_address[row["address"]] = v
                        if self.timeline and rc + sc > 50:
                            address_payments.append((row["pointer"], v))
                            self.address_daily[row["address"]] = v
                            self.address_monthly[row["address"]] = v


                    except:
                        # receiving tx
                        amount = row["amount"]
                        self.new_address[row["address"]] = serialize_address_data(1, amount, 1, 0, 0, 0)
                        if self.blockchain_analytica:
                            self.block_addresses_stat["outputs"]["count"]["total"] += 1
                            self.blockchain_addresses_stat["count"]["total"] += 1
                            self.blockchain_addresses_stat["count"]["outputs"] += 1
                            self.blockchain_addresses_stat["neverUsed"]["count"]["total"] += 1
                            self.blockchain_addresses_stat["neverUsed"]["amount"]["total"] += amount
                            self.blockchain_addresses_stat["amount"]["total"] += amount

                            self.block_addresses_stat["outputs"]["count"]["new"] += 1

                            amount_key = str(floor(log10(amount))) if amount else "null"
                            rtx_key = str(floor(log10(1)))
                            stx_key = "null"
                            tx_key = rtx_key
                            type_key = row["address"][0]
                            try:
                                self.block_addresses_stat["outputs"]["amount"]["map"]["count"][amount_key] += 1
                            except:
                                self.block_addresses_stat["outputs"]["amount"]["map"]["count"][amount_key] = 1

                            try:
                                self.block_addresses_stat["outputs"]["amount"]["map"]["amount"][amount_key] += amount
                            except:
                                self.block_addresses_stat["outputs"]["amount"]["map"]["amount"][amount_key] = amount

                            # never used type map
                            try:
                                self.blockchain_addresses_stat["neverUsed"]["type"]["map"]["count"][type_key] += 1
                            except:
                                self.blockchain_addresses_stat["neverUsed"]["type"]["map"]["count"][type_key] = 1
                            try:
                                self.blockchain_addresses_stat["neverUsed"]["type"]["map"]["amount"][type_key] += amount
                            except:
                                self.blockchain_addresses_stat["neverUsed"]["type"]["map"]["amount"][type_key] = amount

                            # blockchain amount maps
                            try:
                                self.blockchain_addresses_stat["amount"]["map"]["count"][amount_key] += 1
                            except:
                                self.blockchain_addresses_stat["amount"]["map"]["count"][amount_key] = 1

                            try:
                                self.blockchain_addresses_stat["receivedTxCount"]["map"]["count"][rtx_key] += 1
                            except:
                                self.blockchain_addresses_stat["receivedTxCount"]["map"]["count"][rtx_key] = 1

                            try:
                                self.blockchain_addresses_stat["txCount"]["map"]["count"][rtx_key] += 1
                            except:
                                self.blockchain_addresses_stat["txCount"]["map"]["count"][rtx_key] = 1



                new_records = deque()
                new_records_append = new_records.append

                existed_records = deque()
                existed_records_append = existed_records.append

                while len(self.new_address) + len(self.existed_address) > self.cache_limit:
                    if len(self.new_address) > len(self.existed_address):
                        a, v = self.new_address.pop()
                        new_records_append((a, v))
                    else:
                        a, v = self.existed_address.pop()
                        existed_records_append((a, v))




                self.log.info("Address state synchronization -> %s  " % self.last_block)
                self.log.debug("- Address state cache -----")
                self.log.debug("    New address %s; Existed addresses %s  " % (len(self.new_address),
                                                                              len(self.existed_address)))
                self.log.debug("    Requested address %s; Loaded addresses %s  " % (self.requested_addresses,
                                                                                    self.loaded_addresses))
                if self.timeline:
                    self.log.debug("    Saved timeline  %s; Daily  %s; Monthly %s " % (self.timeline_records,
                                                                                       self.saved_daily_records,
                                                                                       self.saved_monthly_records))
                if self.blockchain_analytica:
                    self.log.debug("    Saved address stat  %s;" % self.saved_address_stat_records)

                self.log.debug("    Total saved addresses %s; Total updated addresses %s" % (self.saved_records,
                                                                                             self.updated_records))


            except asyncio.CancelledError:
                self.log.warning("Address synchronization canceled")
                break
            except:
                print(traceback.format_exc())


        await self.flush_cache()
        self.log.warning("Address synchronization completed")
        self.loop.create_task(self.terminate_coroutine())






    async def save_records_batch(self, b):
        async with self.db_pool.acquire() as conn:
            await conn.copy_records_to_table('address', columns=["address", "data"], records=b)

    async def save_records(self, batch):
        if batch:
            batches = chunks(batch, self.threads, 20000)
            b = [self.loop.create_task(self.save_records_batch(b)) for b in batches]
            await asyncio.wait(b)
            self.saved_records += len(batch)


    async def save_timeline_records_batch(self, batch):
        async with self.db_pool.acquire() as conn:
            await conn.copy_records_to_table('address_payments', columns=["pointer", "data"], records=batch)

    async def save_timeline_records(self, batch):
        if batch:
            batches = chunks(batch, self.threads, 20000)
            await asyncio.wait([self.loop.create_task(self.save_timeline_records_batch(b)) for b in batches])
            self.timeline_records += len(batch)

    async def save_daily_records_batch(self, batch):
        async with self.db_pool.acquire() as conn:
            await conn.copy_records_to_table('address_daily', columns=["address", "day", "data"],
                                             records=batch)

    async def save_daily_records(self, batch):
        if batch:
            # batches = chunks(batch, self.threads, 10000)
            # await asyncio.wait([self.loop.create_task(self.save_daily_records_batch(b)) for b in batches])
            await self.save_daily_records_batch(batch)
            self.saved_daily_records += len(batch)

    async def save_monthly_records_batch(self, batch):
        async with self.db_pool.acquire() as conn:
            await conn.copy_records_to_table('address_monthly', columns=["address", "month", "data"],
                                             records=batch)

    async def save_monthly_records(self, batch):
        if batch:
           await self.save_monthly_records_batch(batch)
           self.saved_monthly_records += len(batch)

    async def save_address_stat_records(self, batch):
        if batch:
            async with self.db_pool.acquire() as conn:
                await conn.copy_records_to_table('blocks_addresses_stat', columns=["height", "block", "blockchain"],
                                                 records=batch)
            self.saved_address_stat_records += len(batch)

    async def update_records(self, batch):
        if batch:
            t = time.time()
            batches = chunks(batch, self.threads, 20000)
            await asyncio.wait([self.loop.create_task(self.update_records_batch(b)) for b in batches])
            self.updated_records += len(batch)

    async def update_records_batch(self, batch):
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                                  UPDATE address SET data = r.data 
                                  FROM 
                                  (SELECT address, data FROM UNNEST($1::Address[])) AS r 
                                  WHERE  address.address = r.address;
                               """, batch)


    async def flush_cache(self):
        self.log.info("Flushing address cache ...")
        t = time.time()
        while self.new_address and self.existed_address:
            new_records = deque()
            apop = self.new_address.pop
            new_records_append = new_records.append
            while self.new_address and (len(self.new_address) > len(self.new_address) - 500000):
                a, v = apop()
                new_records_append((a, v))
            self.log.debug("Flushing new address cache %s" % len(self.new_address))
            await self.save_records(new_records)

            existed_records = deque()
            apop = self.existed_address.pop
            existed_records_append = existed_records.append
            while self.existed_address and (len(self.existed_address) > len(self.existed_address) - 500000):
                a, v = apop()
                existed_records_append((a, v))
            self.log.debug("Flushing new address cache %s" % len(self.existed_address))
            await self.update_records(existed_records)

        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                                  UPDATE service SET value = $1
                                  WHERE name =  'address_last_block';
                           """, str(self.last_block))

        self.log.info("Flushing address state cache completed %s" ,round( time.time() - t, 2))



    def terminate(self, a, b):
        self.loop.create_task(self.terminate_coroutine())

    async def terminate_coroutine(self):
        self.active = False
        if self.synchronization_task:
            self.synchronization_task.cancel()
            r = await self.synchronization_task
            try: r.result()
            except: pass
        self.log.info("address synchronization module stopped")
        self.loop.stop()
