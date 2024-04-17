import asyncio
import signal
import traceback
from setproctitle import setproctitle
import asyncpg
import datetime
from collections import deque
from utils import chunks, deserialize_address_data, serialize_address_data

from pybtc import MRU, LRU
import time
import json
from math import *
import sys


class AddressState():

    def __init__(self, dsn, address_state_cache_size, logger):
        setproctitle('btcapi server: addresses state module')
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.dsn = dsn

        self.address_cache = LRU(address_state_cache_size)
        self.affected_existed = MRU()
        self.affected_new = MRU()
        self.missed_addresses = set()



        self.log = logger
        self.threads = 20
        self.cache_limit = 15000000
        self.active = True
        self.db_pool = None
        self.synchronization_task = None
        self.bootstrap_completed = False
        self.start_time = time.time()
        self.last_block = None
        self.batch_last_block = 0


        self.loaded_addresses = 0
        self.requested_addresses = 0


        self.loop = asyncio.get_event_loop()
        signal.signal(signal.SIGTERM, self.terminate)
        self.loop.create_task(self.start())
        self.loop.run_forever()

    async def start(self):
        try:
            self.db_pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=30)
            self.db_pool_2 = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=3)
            self.log.info("Addresses state sync module started")
            self.synchronization_task = self.loop.create_task(self.processor())
        except Exception as err:
            self.log.warning("Start addresses state sync module failed: %s" % err)
            await asyncio.sleep(3)
            self.loop.create_task(self.start())

    async def fetch_records(self, query, params):
        async with self.db_pool_2.acquire() as conn:
            rows = await conn.fetch(query, *params)
        return rows

    async def get_records(self, height, limit):
        q = time.time()
        stxo_t = self.loop.create_task(self.fetch_records("SELECT address, s_pointer as p, amount as a FROM "
                                                          "stxo WHERE s_pointer >= $1 and s_pointer < $2 "
                                                          "order by s_pointer asc",
                                                          ((height + 1) << 39, (height + 1 + limit + 1) << 39)))

        q = time.time()
        ustxo_t = self.loop.create_task(self.fetch_records("SELECT address, pointer as p, amount as a "
                                                           "FROM  stxo WHERE "
                                                           "pointer >= $1 and pointer < $2  "
                                                           "order by pointer asc",
                                                           (
                                                               (height + 1) << 39, (height + 1 + limit + 1) << 39)))

        q = time.time()
        utxo_t = self.loop.create_task(self.fetch_records("SELECT address, pointer as p, amount  as a FROM "
                                                          "connector_utxo WHERE  pointer >= $1 and pointer < $2 "
                                                          "order by pointer asc",
                                                          ((height + 1) << 39,
                                                           (height + 1 + limit + 1) << 39)))

        q = time.time()
        await asyncio.wait({stxo_t, ustxo_t, utxo_t})

        stxo = stxo_t.result()
        utxo = utxo_t.result()
        ustxo = ustxo_t.result()
        return stxo, utxo, ustxo, height, limit



    async def processor(self):
        blockchain_stat = {"inputs": 0,
                           "outputs": 0,
                           "reused": 0,
                           "amountMap": dict()}
        height = -1
        previous_height = -1
        last_block_height = -1
        limit = 200
        next_batch = None
        address_cache = self.address_cache
        affected_new = self.affected_new
        affected_existed = self.affected_existed
        block_stat_records = []
        blockchain_stat_records = []
        batch_new = []
        batch_existed = []
        commit_task = False

        while True:
            try:

                qt = time.time()

                ql = time.time()

                if block_stat_records:
                    commit = self.loop.create_task(
                        self.commit_changes(previous_height, block_stat_records, blockchain_stat_records,
                                            batch_new, batch_existed))
                    commit_task = True
                block_stat_records = []

                async with self.db_pool.acquire() as conn:

                    if not self.bootstrap_completed:
                        v = await conn.fetchval("SELECT value FROM service WHERE name = 'bootstrap_completed' LIMIT 1;")
                        if v == '1':
                            self.bootstrap_completed = True
                        else:
                            await asyncio.sleep(10)
                            continue

                    async with conn.transaction():
                        h = await conn.fetchval("SELECT height  FROM  blocks  order by height desc LIMIT 1;")
                        if h is None:
                            await asyncio.sleep(1)
                            continue
                        max_h = h

                        if height == -1:
                            row = await conn.fetchrow("SELECT height, addresses as b FROM  "
                                                      "blockchian_address_stat  order by height desc LIMIT 1;")
                            if row is not None:
                                blockchain_stat = json.loads(row["b"])
                                height = row["height"]
                        else:
                            height = last_block_height

                    if height + limit > max_h:
                        limit = max_h - height - 1

                if next_batch is None:
                    stxo, utxo, ustxo, height, recent_limit = await self.get_records(height, limit)
                else:
                    await next_batch
                    stxo, utxo, ustxo, height, recent_limit = next_batch.result()
                last_block_height = height + 1 + recent_limit
                first_block_height = height + 1

                if last_block_height + limit > max_h:
                    limit = last_block_height - height - 1
                if last_block_height > max_h:
                    last_block_height = max_h
                next_batch = self.loop.create_task(self.get_records(last_block_height, limit))

                ql = round(time.time() - ql, 2)


                if not stxo and not utxo and not ustxo:
                    try:
                        await commit
                    except:
                        pass
                    next_batch = None
                    limit = 1
                    async with self.db_pool.acquire() as conn:
                        i = await conn.fetchval("select count(*)  from pg_indexes"
                                                " where  indexname = 'address_rich_list'")
                        if i == 0:
                            self.log.warning("Create index on address table ...")
                            await conn.fetchval("CREATE INDEX IF NOT EXISTS  "
                                                " address_rich_list ON address (balance DESC);")
                            self.log.warning("Create index on address completed")
                    # clear cache
                    self.address_cache.clear()
                    self.affected_existed.clear()
                    self.affected_new.clear()
                    self.missed_addresses = set()
                    await asyncio.sleep(1)
                    continue

                qg = time.time()
                block_stat_records = []
                blockchain_stat_records = []
                self.missed_addresses = set()
                missed_addresses = self.missed_addresses

                for w in (stxo, utxo, ustxo):
                    for i in w:
                        if i["address"][0] in (0, 1, 2, 5, 6, 9):
                            if not address_cache.get(i["address"]):
                                missed_addresses.add(i["address"])

                l_records = len(stxo) + len(utxo) + len(ustxo)
                if l_records > address_cache.get_size():
                    address_cache.set_size(l_records)

                len_missed_address = len(missed_addresses)

                await self.load_addresses()
                qg = round(time.time() - qg, 2)

                qc = time.time()
                before_block_balance = dict()
                after_block_balance = dict()
                new_addresses = set()
                input_addresses = set()
                out_addresses = set()
                tx_address = dict()
                ytx_address = dict()
                ztx_address = dict()

                inputs_first_used = 0
                inputs_reused = 0
                inputs_new = 0
                outs_new = 0

                i, y, z = -1, -1, -1
                ihl, yhl, zhl = first_block_height, first_block_height, first_block_height
                itxl, itx, ztx = -1, -1, -1
                ytxl, ytx, ztxl = -1, -1, -1
                zh = first_block_height

                while True:
                    try:
                        i += 1
                        inp = stxo[i]
                        itx = inp["p"] >> 20
                        ih = inp["p"] >> 39
                        ia = inp["address"]
                    except:
                        i = None
                        ih = last_block_height + 1

                    is_new_block = ih != ihl or i is None
                    is_new_tx = (itxl > -1 and itx != itxl) or i is None

                    if is_new_tx:
                        # save previous tx data
                        for addr in tx_address:
                            try:
                                rc, ra, c, frp, lra, lrp, \
                                sc, sa, cd, fsp, lsa, lsp = address_cache[addr]
                            except:
                                rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp = (0, 0, 0, None,
                                                                                       None, None,
                                                                                       0, 0, 0, None,
                                                                                       None, None)

                            if addr not in before_block_balance:
                                before_block_balance[addr] = [rc, ra - sa]
                            sc, sa, cd = sc + 1, sa + tx_address[addr][1], cd + tx_address[addr][0]
                            if fsp is None:
                                fsp, inputs_first_used = itxl, inputs_first_used + 1
                            if sc == 2:
                                inputs_reused += 1
                            if sc == 1:
                                inputs_new += 1
                            lsp = itxl
                            lsa = tx_address[addr][1]
                            address_cache[addr] = (rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp)
                            if rc == 0:
                                new_addresses.add(addr)
                            if addr in new_addresses:
                                affected_new[addr] = (rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp)
                            else:
                                affected_existed[addr] = (rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp)
                            after_block_balance[addr] = ra - sa
                        tx_address = dict()

                    if is_new_block:
                        while True:
                            try:
                                y += 1
                                out = utxo[y]
                                yh = out["p"] >> 39
                                ytx = out["p"] >> 20
                                ya = out["address"]
                            except:
                                y = None
                                yh = last_block_height + 1

                            is_new_block = yh != yhl or y is None
                            is_new_tx = (ytxl > -1 and ytx != ytxl) or y is None

                            if is_new_tx:
                                # save previous tx data
                                for addr in ytx_address:
                                    try:
                                        rc, ra, c, frp, lra, lrp, \
                                        sc, sa, cd, fsp, lsa, lsp = address_cache[addr]
                                    except:
                                        rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp = (0, 0, 0, None,
                                                                                               None, None,
                                                                                               0, 0, 0, None,
                                                                                               None, None)

                                    if addr not in before_block_balance:
                                        before_block_balance[addr] = [rc, ra - sa]
                                    coins, amount = ytx_address[addr]
                                    rc += 1
                                    ra += amount
                                    c += coins
                                    if frp is None:
                                        frp = ytxl
                                        new_addresses.add(addr)
                                        outs_new += 1
                                    lrp = ytxl
                                    lra = amount
                                    address_cache[addr] = (rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp)
                                    if addr in new_addresses:
                                        affected_new[addr] = (rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp)
                                    else:
                                        affected_existed[addr] = (rc, ra, c, frp, lra, lrp,
                                                                  sc, sa, cd, fsp, lsa, lsp)
                                    after_block_balance[addr] = ra - sa
                                ytx_address = dict()

                            if is_new_block:
                                while True:
                                    try:
                                        z += 1
                                        out2 = ustxo[z]
                                        zh = out2["p"] >> 39
                                        ztx = out2["p"] >> 20
                                        za = out2["address"]
                                    except:
                                        z = None
                                        zh = last_block_height + 1
                                    is_new_block = zh != zhl or z is None
                                    is_new_tx = (ztxl > -1 and ztx != ztxl) or z is None

                                    if is_new_tx:
                                        # save previous tx data
                                        for addr in ztx_address:
                                            try:
                                                rc, ra, c, frp, lra, lrp, \
                                                sc, sa, cd, fsp, lsa, lsp = address_cache[addr]
                                            except:
                                                rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp = (0, 0, 0, None,
                                                                                                       None, None,
                                                                                                       0, 0, 0, None,
                                                                                                       None, None)

                                            if addr not in before_block_balance:
                                                before_block_balance[addr] = [rc, ra - sa]
                                            coins, amount = ztx_address[addr]
                                            rc += 1
                                            ra += amount
                                            c += coins
                                            if frp is None:
                                                frp = ztxl
                                                new_addresses.add(addr)
                                                outs_new += 1
                                            lrp = ztxl
                                            lra = amount
                                            address_cache[addr] = (rc, ra, c, frp, lra, lrp,
                                                                   sc, sa, cd, fsp, lsa, lsp)
                                            if addr in new_addresses:
                                                affected_new[addr] = (rc, ra, c, frp, lra, lrp,
                                                                      sc, sa, cd, fsp, lsa, lsp)
                                            else:
                                                affected_existed[addr] = (rc, ra, c, frp, lra, lrp,
                                                                          sc, sa, cd, fsp, lsa, lsp)
                                            after_block_balance[addr] = ra - sa
                                        ztx_address = dict()

                                    if is_new_block:
                                        while True:
                                            block_stat = {"inputs": {"count": {"total": len(input_addresses),
                                                                               "reused": inputs_reused}},
                                                          "outputs": {"count": {"total": len(out_addresses),
                                                                                "new": outs_new}},
                                                          "total": len(input_addresses | out_addresses)}
                                            blockchain_stat["outputs"] += outs_new
                                            blockchain_stat["inputs"] += inputs_new
                                            blockchain_stat["reused"] += inputs_reused

                                            for amount in after_block_balance.values():
                                                if amount < 0:
                                                    print(amount)
                                                try:
                                                    key = 'null' if amount == 0 else str(floor(log10(amount)))
                                                except:
                                                    key = 'null'
                                                try:
                                                    blockchain_stat["amountMap"][key]["amount"] += amount
                                                    blockchain_stat["amountMap"][key]["count"] += 1
                                                except:
                                                    blockchain_stat["amountMap"][key] = {"amount": amount, "count": 1}

                                            for v in before_block_balance.values():
                                                if v[0] == 0:
                                                    continue
                                                amount = v[1]
                                                try:
                                                    key = 'null' if amount == 0 else str(floor(log10(amount)))
                                                except:
                                                    key = 'null'
                                                blockchain_stat["amountMap"][key]["amount"] -= amount
                                                blockchain_stat["amountMap"][key]["count"] -= 1
                                            block_stat_records.append((ihl, json.dumps(block_stat)))
                                            blockchain_stat_records.append((ihl, json.dumps(blockchain_stat)))
                                            ihl += 1
                                            if yhl < ihl:
                                                yhl += 1
                                            if zhl < yhl:
                                                zhl += 1
                                            input_addresses = set()
                                            out_addresses = set()
                                            inputs_reused = 0
                                            inputs_new = 0
                                            outs_new = 0
                                            after_block_balance = dict()
                                            before_block_balance = dict()
                                            if zhl >= zh or yhl >= yh:
                                                break
                                        await asyncio.sleep(0)

                                    if z is not None:
                                        # handel output record
                                        if ihl == ih:
                                            z -= 1
                                            break
                                        if yh < zh:
                                            z -= 1
                                            break

                                        if za[0] in (0, 1, 2, 5, 6, 9):
                                            out_addresses.add(za)
                                            try:
                                                # [coins, amount]
                                                ztx_address[za][0] += 1
                                                ztx_address[za][1] += out2["a"]
                                            except:
                                                ztx_address[za] = [1, out2["a"]]
                                        ztxl = ztx

                                    if is_new_block:
                                        if ih < zh:
                                            break
                                        if z is None:
                                            break

                            if ih <= yhl:
                                if y is None:
                                    y = len(utxo)
                                else:
                                    y -= 1
                                break

                            if is_new_block:
                                if yhl >= yh and yhl >= ih:
                                    break

                            # handel output record
                            if yh <= zh:
                                if y is not None and ya[0] in (0, 1, 2, 5, 6, 9):
                                    out_addresses.add(ya)
                                    try:
                                        # [coins, amount]
                                        ytx_address[ya][0] += 1
                                        ytx_address[ya][1] += out["a"]
                                    except:
                                        ytx_address[ya] = [1, out["a"]]
                                ytxl = ytx
                            else:
                                if y is None:
                                    y = len(utxo)
                                else:
                                    y -= 1
                    # handel input record
                    if i is not None and ia[0] in (0, 1, 2, 5, 6, 9):
                        input_addresses.add(ia)
                        try:
                            # [coins_destroyed, amount]
                            tx_address[ia][0] += 1
                            tx_address[ia][1] += inp["a"]
                        except:
                            tx_address[ia] = [1, inp["a"]]
                    itxl = itx

                    if is_new_block:
                        if ihl > ih or i is None:
                            break

                batch_new = deque()
                affected_new_pop = affected_new.pop
                batch_new_append = batch_new.append
                while affected_new:
                    a, v = affected_new_pop()
                    balance = v[1] - v[7]
                    data = serialize_address_data(*v)
                    batch_new_append((a, balance, data))
                batch_existed = deque()

                affected_existed_pop = affected_existed.pop
                batch_existed_append = batch_existed.append
                while affected_existed:
                    a, v = affected_existed_pop()
                    balance = v[1] - v[7]
                    data = serialize_address_data(*v)
                    batch_existed_append((a, balance, data))
                qc = round(time.time() - qc, 2)

                qs = time.time()
                previous_height = height
                if commit_task:
                    height = await commit
                else:
                    height = last_block_height

                qs = round(time.time() - qs, 2)

                qt = round(time.time() - qt, 2)
                self.log.debug(
                    "Address state processor round %s; Get records %s; Load addresses %s; Computation %s; Save %s" % (
                    qt, ql, qg, qc, qs))
                self.log.info("Address state/analytica +%s blocks; last block %s;" % (last_block_height -
                                                                                      first_block_height + 1,
                                                                                      last_block_height))
                if l_records > 2000000:
                    if limit > 10:
                        limit -= 2
                elif len_missed_address < 200000 and limit < 1000:
                    limit += 2

            except asyncio.CancelledError:
                self.log.warning("Addresses state task canceled")
                break
            except Exception as err:
                self.address_cache.clear()
                self.affected_existed.clear()
                self.affected_new.clear()
                self.missed_addresses = set()
                block_stat_records = []
                height = -1

                try:
                    commit.cancel()
                    await commit
                except:
                    pass

                commit_task = False
                next_batch = None
                self.log.error("Addresses state task error: %s" % err)
                print(traceback.format_exc())

                await asyncio.sleep(1)

    async def commit_changes(self, height, block_stat_records, blockchain_stat_records, batch_new, batch_existed):
        async with self.db_pool.acquire() as conn:
            c = await conn.fetchval(" SELECT n_dead_tup FROM pg_stat_user_tables  "
                                    " WHERE relname = 'address' LIMIT 1;")
            if c and c > 100000000:
                qm = time.time()
                self.log.warning("Address table maintenance ...")
                await conn.execute("VACUUM FULL address;")
                await conn.execute("ANALYZE address;")

                self.log.warning("Address table maintenance completed %s" % round(time.time() - qm, 2))

            async with conn.transaction():
                check_height = await conn.fetchval("SELECT height FROM  "
                                                   "blockchian_address_stat  order by height desc LIMIT 1 FOR UPDATE;")

                if check_height is None:
                    check_height = -1

                if check_height != height:
                    # data changed during process recalculate again
                    raise Exception("data changed")
                await conn.copy_records_to_table('block_address_stat',
                                                 columns=["height", "addresses"], records=block_stat_records)

                await conn.copy_records_to_table('blockchian_address_stat',
                                                 columns=["height", "addresses"], records=blockchain_stat_records)
                await conn.copy_records_to_table('address', columns=["address", "balance", "data"], records=batch_new)
                await conn.execute("""
                                      UPDATE address SET data = r.data,  balance = r.balance 
                                      FROM 
                                      (SELECT address, balance, data FROM UNNEST($1::Address[])) AS r 
                                      WHERE  address.address = r.address;
                                   """, batch_existed)
                check_height = await conn.fetchval("SELECT height FROM  "
                                                   "blockchian_address_stat  order by height desc LIMIT 1 FOR UPDATE;")

                return check_height

    async def load_addresses_batch(self, batch):
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT  address, data FROM address WHERE address = ANY($1);", batch)
            for row in rows:
                self.address_cache[row["address"]] = deserialize_address_data(row["data"])
        return len(rows)

    async def load_addresses(self):
        q = time.time()
        self.log.debug("Request %s addresses from db ... " % len(self.missed_addresses))
        self.requested_addresses += len(self.missed_addresses)
        batches = chunks(self.missed_addresses, self.threads, 10000)
        tasks = [self.loop.create_task(self.load_addresses_batch(b)) for b in batches]
        count = 0
        if tasks:
            await asyncio.wait(tasks)
            for task in tasks:
                count += task.result()

        self.loaded_addresses += count
        self.log.debug("Loaded %s addresses [%s]" % (count, round(time.time() - q, 2)))

    def terminate(self, a, b):
        self.loop.create_task(self.terminate_coroutine())

    async def terminate_coroutine(self):
        self.active = False
        if self.synchronization_task:
            self.synchronization_task.cancel()
            r = await self.synchronization_task
            try:
                r.result()
            except:
                pass
        self.log.info("address state module stopped")
        self.loop.stop()
