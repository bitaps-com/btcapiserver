import asyncpg
from pybtc import *
from utils import *
from .blocks import last_n_blocks
import time
import aiojsonrpc
from statistics import median



async def init_db_pool(app):
    # init db pool
    app["loop"] = asyncio.get_event_loop()
    app["log"].info("Init db pool ... ")
    app["rpc"] = aiojsonrpc.rpc(app["node_rpc_url"], app["loop"])
    app["db_pool"] = await \
        asyncpg.create_pool(dsn=app["dsn"],
                            loop=app["loop"],
                            min_size=10, max_size=app["pool_threads"])
    app["service_db_pool"] = await \
        asyncpg.create_pool(dsn=app["dsn"],
                            loop=app["loop"],
                            min_size=1, max_size=1)

async def close_db_pool(app):
    await app["db_pool"].close()



def get_adjusted_block_time(h, request):
    return request.app["block_map_time"][h]


async def load_block_map(app):
    # init db pool
    app["log"].info("Loading block timestamp map ... ")
    app["block_map_time"] = dict()
    app["day_map_block"] = dict()
    app["time_map_block"] = dict()
    l = list()
    async with app["db_pool"].acquire() as conn:
        stmt = await conn.prepare("SELECT adjusted_timestamp, height , header  "
                                  "FROM blocks order by height asc;")
        rows = await stmt.fetch()
    for row in rows:
        t = unpack("<L", row["header"][68:72])[0]
        if app["blocks_data"]:
            l.append(t)
            if len(l) > 11:
                l.pop(0)
            median_time = median(l)
        else:
            median_time = None
        app["block_map_time"][row["height"]] = (row["adjusted_timestamp"], t, median_time)
        # app["block_map_time"][row["height"]] = row["adjusted_timestamp"]
        app["day_map_block"][ceil(row["adjusted_timestamp"] / 86400)] = row["height"]
        app["time_map_block"][row["adjusted_timestamp"]] = row["height"]
        app["last_block"] = row["height"]
    app["log"].info("Loaded time map for %s blocks" % app["last_block"])
    app["loop"].create_task(block_map_update_task(app))


async def block_map_update(app):
        i = app["last_block"]  - 11
        if i < 0:
            i = 0
        l = [app["block_map_time"][q][1] for q in range(app["last_block"], i)]
        async with app["db_pool"].acquire() as conn:
            stmt = await conn.prepare("SELECT adjusted_timestamp, height, header  "
                                      "FROM blocks WHERE height > $1 "
                                      "ORDER BY height asc;")
            rows = await stmt.fetch(app["last_block"])
        for row in rows:
            t = unpack("<L", row["header"][68:72])[0]
            if app["blocks_data"]:
                l.append(t)
                if len(l) > 11:
                    l.pop(0)
                median_time = median(l)
            else:
                median_time = None
            app["block_map_time"][row["height"]] = (row["adjusted_timestamp"], t, median_time)
            app["day_map_block"][ceil(row["adjusted_timestamp"] / 86400)] = row["height"]
            app["time_map_block"][row["adjusted_timestamp"]] = row["height"]
            app["last_block"] = row["height"]
            app["log"].warning("New block: %s" % row["height"])



async def block_map_update_task(app):
    app["log"].info("block_map_update started")
    while True:
        try:
            await block_map_update(app)
            await asyncio.sleep(0.5)
        except Exception as err:
            app["log"].error("block_map_update: "+str(err))
            await asyncio.sleep(5)

async def load_block_cache(app):
    while 1:
        await asyncio.sleep(10)
        try:
            t = time.time()
            r = await last_n_blocks(2016, app["db_pool"], False, False)

            app["log"].warning("cache blocks received  %s  %s " % (len(r["data"]),
                                                                   time.time() - t))
            app["last_blocks"] = list()
            for b in r["data"][::-1]:
                app["last_blocks"].append(b)
        except asyncio.CancelledError:
            app["log"].warning("Task load block cache stopped")
            raise
        except Exception as err:
            print(err)

