import asyncio
import asyncpg
from aiohttp import web
from pybtc import *
from utils import *
from pybtc import c_int_to_int as ci2i
from pybtc import c_int_len as cil
from pybtc import int_to_bytes as i2b
from pybtc import bytes_to_int as b2i
from pybtc import rh2s
import time
import json

async def block_by_pointer(pointer, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if pointer == 'last':
            stmt = await conn.prepare("SELECT height,"
                                      "       hash,"
                                      "       header,"
                                      "       adjusted_timestamp "
 
                                      "FROM blocks  ORDER BY height desc LIMIT 1;")
            row = await stmt.fetchrow()
        else:
            if type(pointer) == bytes:
                stmt = await conn.prepare("SELECT height,"
                                          "       hash,"
                                          "       header,"
                                          "       adjusted_timestamp "
                                          "FROM blocks  WHERE hash = $1 LIMIT 1;")
                row = await stmt.fetchrow(pointer)

            elif type(pointer) == int:
                stmt = await conn.prepare("SELECT height,"
                                          "       hash,"
                                          "       header,"
                                          "       adjusted_timestamp "
                                          "FROM blocks  WHERE height = $1 LIMIT 1;")
                row = await stmt.fetchrow(pointer)

            # next_block_hash = await conn.fetchval("SELECT hash from blocks WHERE height = $1 LIMIT 1;",
            #                                       row["height"])

    if row is None:
        raise APIException(NOT_FOUND, "block not found", status=404)

    block = dict()
    block["height"] = row["height"]
    block["hash"] = rh2s(row["hash"])
    block["header"] = row["header"].hex()
    block["adjustedTimestamp"] = row["adjusted_timestamp"]


    # block["previousBlockHash"] = None
    # if block["height"]:
    #     block["previousBlockHash"] = row["previous_block_hash"].hex()

    # if next_block_hash is not None:
    #     block["nextBlockHash"] = next_block_hash.hex()
    # else:
    #     block["nextBlockHash"] = None

    resp = {"data": block,
            "time": round(time.time() - q, 4)}
    return resp


async def block_headers(pointer, count, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if isinstance(pointer, bytes):
            stmt = await conn.prepare("SELECT height "
                                      "FROM blocks  WHERE hash = $1 LIMIT 1;")
            pointer = await stmt.fetchval(pointer)
            if pointer is None:
                raise APIException(NOT_FOUND, "block not found", status=404)
        rows = await conn.fetch("SELECT header "
                                  "FROM blocks  WHERE height > $1 ORDER BY height ASC LIMIT $2;",
                                  pointer, count)

    resp = {"data": [row["header"] for row in rows],
            "time": round(time.time() - q, 4)}
    return resp

async def block_utxo(pointer, limit, page, order, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if isinstance(pointer, bytes):
            stmt = await conn.prepare("SELECT height "
                                      "FROM blocks  WHERE hash = $1 LIMIT 1;")
            pointer = await stmt.fetchval(pointer)
            if pointer is None:
                raise APIException(NOT_FOUND, "block not found", status=404)
        rows = await conn.fetch("SELECT outpoint, pointer, address, amount "
                                "FROM connector_utxo  WHERE pointer >= $1 AND pointer < $2 "
                                "ORDER BY pointer %s LIMIT $3 OFFSET $4;" % order,
                                pointer << 39, (pointer + 1) << 39, limit + 1,  limit * (page - 1))
    utxo = list()
    for row in rows[:limit]:
        address = row["address"]
        address_type = SCRIPT_N_TYPES[address[0]]
        if address[0] in (0, 1, 2, 5, 6):
            script_hash = True if address[0] in (1,6) else False
            witness = 0 if address[0] in (1,6) else None
            address = hash_to_address(address[1:], testnet=app["testnet"],
                                      script_hash = script_hash, witness_version = witness)

        utxo.append({"txId": rh2s(row["outpoint"][:32]),
                     "vOut": bytes_to_int(row["outpoint"][32:]),
                     "txIndex": (row["pointer"] >> 20) & 1048575,
                     "amount": row["amount"],
                     "address": address,
                     "type": address_type})
    last_page = False if len(rows) > limit else True
    resp = {"data": utxo,
            "time": round(time.time() - q, 4),
            "lastPage": last_page}
    return resp

async def block_transactions(pointer, limit, page, order, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if isinstance(pointer, bytes):
            stmt = await conn.prepare("SELECT height "
                                      "FROM blocks  WHERE hash = $1 LIMIT 1;")
            pointer = await stmt.fetchval(pointer)
            if pointer is None:
                raise APIException(NOT_FOUND, "block not found", status=404)
        rows = await conn.fetch("SELECT tx_id "
                                "FROM transaction  WHERE pointer >= $1 AND pointer < $2 "
                                "ORDER BY pointer %s LIMIT $3 OFFSET $4;" % order,
                                pointer << 39, (pointer + 1) << 39, limit + 1,  limit * (page - 1))
    transactions = list()
    for row in rows:
        transactions.append(rh2s(row["tx_id"]))
    last_page = False if len(rows) > limit else True
    resp = {"data": transactions,
            "time": round(time.time() - q, 4),
            "lastPage": last_page}
    return resp



async def tx_by_pointer(pointer, app):
    q = time.time()
    testnet = app["testnet"]
    valid = True
    merkle_proof = None
    block_height = None
    block_index = None

    async with app["db_pool"].acquire() as conn:
        if isinstance(pointer, bytes):
            row = await conn.fetchrow("SELECT tx_id, timestamp "
                                         "FROM unconfirmed_transaction "
                                         "WHERE tx_id = $1 LIMIT 1;", pointer)
            if row is None:
                row = await conn.fetchrow("SELECT tx_id, timestamp, pointer "
                                          "FROM transaction "
                                          "WHERE tx_id = $1 LIMIT 1;", pointer)
                if row is None:
                    raise APIException(NOT_FOUND, "transaction not found", status=404)

                block_height = row["pointer"] >> 39
                block_index = (row["pointer"] >> 20) & 1048575
        else:
            row = await conn.fetchrow("SELECT tx_id, timestamp, pointer "
                                      "FROM transaction "
                                      "WHERE pointer = $1 LIMIT 1;", pointer)
            if row is None:
                raise APIException(NOT_FOUND, "transaction not found", status=404)
            block_height = row["pointer"] >> 39
            block_index = (row["pointer"] >> 20) & 1048575

    tx = {"txId": rh2s(row["tx_id"]),
          "blockHeight": block_height,
          "blockIndex": block_index,
          "timestamp": row["timestamp"]}

    return {"data": tx,
            "time": round(time.time() - q, 4)}


async def address_state(address, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        uamount = await conn.fetchval("SELECT sum(amount) "
                                      "FROM connector_unconfirmed_utxo "
                                      "WHERE address = $1;", address)
        camount = await conn.fetchval("SELECT  sum(amount)  "
                                      "FROM connector_utxo "
                                      "WHERE address = $1;", address)

    if uamount is None: uamount = 0
    if camount is None: camount = 0

    return {"data": {"amount": {"confirmed": int(camount),
                                "unconfirmed": int(uamount)}},
            "time": round(time.time() - q, 4)}


async def address_confirmed_utxo(address, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT  outpoint, amount, pointer  "
                                      "FROM connector_utxo "
                                      "WHERE address = $1 order by pointer;", address)
    utxo = []
    for row in rows:
        utxo.append({"txId": rh2s(row["outpoint"][:32]),
                     "vOut": bytes_to_int(row["outpoint"][32:]),
                     "block": row["pointer"] >> 39,
                     "txIndex": (row["pointer"] - (row["pointer"] >> 39)) >> 20,
                     "amount": row["amount"]})

    return {"data": utxo,
            "time": round(time.time() - q, 4)}





def get_adjusted_block_time(h, request):
    return request.app["block_map_time"][h]

async def init_db_pool(app):
    # init db pool
    app["loop"] = asyncio.get_event_loop()
    app["log"].info("Init db pool ... ")
    app["db_pool"] = await \
        asyncpg.create_pool(dsn=app["dsn"],
                            loop=app["loop"],
                            min_size=10, max_size=app["pool_threads"])
    app["service_db_pool"] = await \
        asyncpg.create_pool(dsn=app["dsn"],
                            loop=app["loop"],
                            min_size=1, max_size=1)

async def load_block_cache(app):
    while 1:
        await asyncio.sleep(10)
        try:
            t = time.time()
            r = await blocks_last_n(2016, app["db_pool"], False, False)

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



async def load_block_map(app):
    # init db pool
    app["log"].info("Loading block timestamp map ... ")
    app["block_map_time"] = dict()
    app["day_map_block"] = dict()
    app["time_map_block"] = dict()
    async with app["db_pool"].acquire() as conn:
        stmt = await conn.prepare("SELECT adjusted_timestamp, height   "
                                  "FROM blocks order by height desc;")
        rows = await stmt.fetch()
        for row in rows:
            app["block_map_time"][row["height"]] = row["adjusted_timestamp"]
            app["day_map_block"][ceil(row["adjusted_timestamp"] / 86400)] = row["height"]
            app["time_map_block"][row["adjusted_timestamp"]] = row["height"]
            app["last_block"] = row["height"]
    app["loop"].create_task(block_map_update(app))
    # app["loop"].create_task(load_block_cache(app))




async def block_map_update(app):
    app["log"].info("block_map_update started")
    return
    while True:
        try:
            async with app["db_pool"].acquire() as conn:
                stmt = await conn.prepare("SELECT adjusted_timestamp, height   "
                                          "FROM blocks "
                                          "WHERE height < $1 "
                                          "ORDER BY height DESC;")
                rows = await stmt.fetch(2147483647 - app["last_block"])
            for row in rows:
                app["block_map_time"][row["height"]] = row["adjusted_timestamp"]
                app["day_map_block"][ceil(row["adjusted_timestamp"] / 86400)] = row["height"]
                app["time_map_block"][row["adjusted_timestamp"]] = row["height"]
                app["last_block"] = row["height"]
                app["log"].info("New block: %s" % row["height"])
            await asyncio.sleep(1)
        except Exception as err:
            app["log"].error("block_map_update: "+str(err))
            await asyncio.sleep(5)


async def close_db_pool(app):
    await app["db_pool"].close()