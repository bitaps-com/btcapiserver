from pybtc import *
from utils import APIException
from utils import NOT_FOUND
from .service import block_map_update
from pybtc import rh2s, SCRIPT_N_TYPES
import time
import base64
import json


async def  last_n_blocks(n, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT height,"
                                      "       hash,"
                                      "       header,"
                                      "       adjusted_timestamp "
                                      "FROM blocks  ORDER BY height desc LIMIT $1;", n)

    if rows is None:
        raise APIException(NOT_FOUND, "blocks not found", status=404)
    r = []
    for row in rows:
        block = dict()
        block["height"] = row["height"]
        block["hash"] = rh2s(row["hash"])
        block["header"] = base64.b64encode(row["header"]).decode()
        block["adjustedTimestamp"] = row["adjusted_timestamp"]
        r.append(block)

    resp = {"data": r,
            "time": round(time.time() - q, 4)}
    return resp

async def blocks_daily(day, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT height,"
                                      "       hash,"
                                      "       header,"
                                      "       adjusted_timestamp "
                                      "FROM blocks  "
                                "WHERE adjusted_timestamp >= $1 and "
                                "      adjusted_timestamp < $2  "
                                "ORDER BY height desc;", day, day + 86400)

    if rows is None:
        raise APIException(NOT_FOUND, "blocks not found", status=404)
    r = []
    for row in rows:
        block = dict()
        block["height"] = row["height"]
        block["hash"] = rh2s(row["hash"])
        block["header"] = base64.b64encode(row["header"]).decode()
        block["adjustedTimestamp"] = row["adjusted_timestamp"]
        r.append(block)

    resp = {"data": r,
            "time": round(time.time() - q, 4)}
    return resp

async def blocks_last_n_hours(n, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT height,"
                                      "       hash,"
                                      "       header,"
                                      "       adjusted_timestamp "
                                      "FROM blocks  "
                                "WHERE adjusted_timestamp >= $1 "
                                "ORDER BY height desc;", int(time.time()) - n * 60 * 60 )
    if rows is None:
        raise APIException(NOT_FOUND, "blocks not found", status=404)
    r = []
    for row in rows:
        block = dict()
        block["height"] = row["height"]
        block["hash"] = rh2s(row["hash"])
        block["header"] = base64.b64encode(row["header"]).decode()
        block["adjustedTimestamp"] = row["adjusted_timestamp"]
        r.append(block)

    resp = {"data": r,
            "time": round(time.time() - q, 4)}
    return resp

async def  data_last_n_blocks(n, app):
    pool = app["db_pool"]
    qt = time.time()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT height,"
                                  "       hash,"
                                  "       miner,"
                                  "       timestamp_received,"
                                  "       data,"
                                  "       header,"
                                  "       adjusted_timestamp "
                              "FROM blocks  ORDER BY height desc LIMIT $1;", n)

        nx_map = dict()
        cb_pointers = []
        for row in rows:
            cb_pointers.append(row["height"] << 39)
            nx_map[row["height"] - 1] = rh2s(row["hash"])


        cb_pointers = [row["height"] << 39 for row in rows]

        cb_rows = await conn.fetch("SELECT pointer, raw_transaction  "
                                "FROM transaction  WHERE pointer = ANY($1);",  cb_pointers)
        cb_map = dict()
        for cb in cb_rows:
            cb_map[cb["pointer"] >> 39] = Transaction(cb["raw_transaction"], format="raw")



    if rows is None:
        raise APIException(NOT_FOUND, "blocks not found", status=404)
    r = []

    for row in rows:
        block = dict()
        block["height"] = row["height"]
        if block["height"] not in app["block_map_time"]:
            print(1)
            await block_map_update(app)
            if block["height"] not in app["block_map_time"]:
                print(block["height"], app["last_block"])
                raise Exception("internal error")
        block["hash"] = rh2s(row["hash"])
        block["header"] = base64.b64encode(row["header"]).decode()
        d = json.loads(row["data"])
        for k in  d:
            block[k] = d[k]
        if row["miner"]:
            block["miner"] = json.loads(row["miner"])
        else:
            block["miner"] = None
        block["medianBlockTime"] = app["block_map_time"][block["height"]][2]
        block["blockTime"] = app["block_map_time"][block["height"]][1]
        block["receivedTimestamp"] = row["timestamp_received"]
        block["adjustedTimestamp"] = row["adjusted_timestamp"]
        block["bitsHex"] = block["bits"]
        block["bits"] = bytes_to_int(bytes_from_hex(block["bits"]))
        block["nonceHex"] = block["nonce"].to_bytes(4, byteorder="big").hex()
        block["versionHex"] = int_to_bytes(block["version"]).hex()
        block["difficulty"] = block["targetDifficulty"]
        q = int.from_bytes(s2rh(block["hash"]), byteorder="little")
        block["blockDifficulty"] = target_to_difficulty(q)
        del block["targetDifficulty"]
        try:
            block["nextBlockHash"] = nx_map[block["height"]]
        except:
            block["nextBlockHash"] = None

        # get coinbase transaction

        tx = cb_map[block["height"]]

        block["estimatedBlockReward"] = 50 * 100000000 >> block["height"] // 840000
        block["blockReward"] = tx["amount"]
        if tx["amount"] > block["estimatedBlockReward"]:
            block["blockReward"] = block["estimatedBlockReward"]
            block["blockFeeReward"] = tx["amount"] - block["estimatedBlockReward"]
        else:
            block["blockReward"] = tx["amount"]
            block["blockFeeReward"] = 0
        block["confirmations"] = app["last_block"] -  block["height"] + 1
        block["transactionsCount"] = var_int_to_int(row["header"][80:])
        block["coinbase"] = tx["vIn"][0]["scriptSig"].hex()
        r.append(block)

    resp = {"data": r,
            "time": round(time.time() - qt, 4)}
    return resp

async def  blocks_data_last_n_hours(n, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT height,"
                                  "       hash,"
                                  "       miner,"
                                  "       timestamp_received,"
                                  "       data,"
                                  "       header,"
                                  "       adjusted_timestamp "
                              "FROM blocks  "
                                "WHERE adjusted_timestamp >= $1 "
                                "ORDER BY height desc;", int(time.time()) - n * 60 * 60 )

        nx_map = dict()
        cb_pointers = []
        for row in rows:
            cb_pointers.append(row["height"] << 39)
            nx_map[row["height"] - 1] = rh2s(row["hash"])


        cb_pointers = [row["height"] << 39 for row in rows]

        cb_rows = await conn.fetch("SELECT pointer, raw_transaction  "
                                "FROM transaction  WHERE pointer = ANY($1);",  cb_pointers)
        cb_map = dict()
        for cb in cb_rows:
            cb_map[cb["pointer"] >> 39] = Transaction(cb["raw_transaction"], format="raw")



    if rows is None:
        raise APIException(NOT_FOUND, "blocks not found", status=404)
    r = []

    for row in rows:
        block = dict()
        block["height"] = row["height"]
        if block["height"] > app["last_block"]:
            await block_map_update(app)
            if block["height"] > app["last_block"]:
                raise Exception("internal error")
        block["hash"] = rh2s(row["hash"])
        block["header"] = base64.b64encode(row["header"]).decode()
        d = json.loads(row["data"])
        for k in  d:
            block[k] = d[k]
        if row["miner"]:
            block["miner"] = json.loads(row["miner"])
        else:
            block["miner"] = None
        block["medianBlockTime"] = app["block_map_time"][block["height"]][2]
        block["blockTime"] = app["block_map_time"][block["height"]][1]
        block["receivedTimestamp"] = row["timestamp_received"]
        block["adjustedTimestamp"] = row["adjusted_timestamp"]
        block["bitsHex"] = block["bits"]
        block["bits"] = bytes_to_int(bytes_from_hex(block["bits"]))
        block["nonceHex"] = block["nonce"].to_bytes(4, byteorder="big").hex()
        block["versionHex"] = int_to_bytes(block["version"]).hex()
        block["difficulty"] = block["targetDifficulty"]
        q = int.from_bytes(s2rh(block["hash"]), byteorder="little")
        block["blockDifficulty"] = target_to_difficulty(q)
        del block["targetDifficulty"]
        try:
            block["nextBlockHash"] = nx_map[block["height"]]
        except:
            block["nextBlockHash"] = None

        # get coinbase transaction

        tx = cb_map[block["height"]]

        block["estimatedBlockReward"] = 50 * 100000000 >> block["height"] // 840000
        block["blockReward"] = tx["amount"]
        if tx["amount"] > block["estimatedBlockReward"]:
            block["blockReward"] = block["estimatedBlockReward"]
            block["blockFeeReward"] = tx["amount"] - block["estimatedBlockReward"]
        else:
            block["blockReward"] = tx["amount"]
            block["blockFeeReward"] = 0
        block["confirmations"] = app["last_block"] -  block["height"] + 1
        block["transactionsCount"] = var_int_to_int(row["header"][80:])
        block["coinbase"] = tx["vIn"][0]["scriptSig"].hex()
        r.append(block)

    resp = {"data": r,
            "time": round(time.time() - q, 4)}
    return resp

async def  data_blocks_daily(day, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT height,"
                                          "       hash,"
                                          "       miner,"
                                          "       timestamp_received,"
                                          "       data,"
                                          "       header,"
                                          "       adjusted_timestamp "
                                      "FROM blocks  "
                                "WHERE adjusted_timestamp >= $1 and "
                                "      adjusted_timestamp < $2  "
                                "ORDER BY height desc;", day, day + 86400)
        nx_map = dict()
        cb_pointers = []
        for row in rows:
            cb_pointers.append(row["height"] << 39)
            nx_map[row["height"] - 1] = rh2s(row["hash"])
        h = (cb_pointers[0] >> 39) + 1

        if cb_pointers:
            next_block = await conn.fetchval("SELECT hash "
                                          "FROM blocks  "
                                    "WHERE height = $1 LIMIT 1;", h)
            if next_block is not None:
                next_block = rh2s(next_block)
            nx_map[h - 1] = next_block
        else:
            nx_map[h - 1] = None


        cb_pointers = [row["height"] << 39 for row in rows]

        cb_rows = await conn.fetch("SELECT pointer, raw_transaction  "
                                "FROM transaction  WHERE pointer = ANY($1);",  cb_pointers)
        cb_map = dict()
        for cb in cb_rows:
            cb_map[cb["pointer"] >> 39] = Transaction(cb["raw_transaction"], format="raw")



    if rows is None:
        raise APIException(NOT_FOUND, "blocks not found", status=404)
    r = []
    for row in rows:
        block = dict()
        block["height"] = row["height"]
        if block["height"] > app["last_block"]:
            await block_map_update(app)
            if block["height"] > app["last_block"]:
                raise Exception("internal error")
        block["hash"] = rh2s(row["hash"])
        block["header"] = base64.b64encode(row["header"]).decode()
        d = json.loads(row["data"])
        for k in  d:
            block[k] = d[k]
        if row["miner"]:
            block["miner"] = json.loads(row["miner"])
        else:
            block["miner"] = None
        block["medianBlockTime"] = app["block_map_time"][block["height"]][2]
        block["blockTime"] = app["block_map_time"][block["height"]][1]
        block["receivedTimestamp"] = row["timestamp_received"]
        block["adjustedTimestamp"] = row["adjusted_timestamp"]
        block["bitsHex"] = block["bits"]
        block["bits"] = bytes_to_int(bytes_from_hex(block["bits"]))
        block["nonceHex"] = block["nonce"].to_bytes(4, byteorder="big").hex()
        block["versionHex"] = int_to_bytes(block["version"]).hex()
        block["difficulty"] = block["targetDifficulty"]
        q = int.from_bytes(s2rh(block["hash"]), byteorder="little")
        block["blockDifficulty"] = target_to_difficulty(q)
        del block["targetDifficulty"]
        try:
            block["nextBlockHash"] = nx_map[block["height"]]
        except:
            block["nextBlockHash"] = None

        # get coinbase transaction

        tx = cb_map[block["height"]]

        block["estimatedBlockReward"] = 50 * 100000000 >> block["height"] // 840000
        block["blockReward"] = tx["amount"]
        if tx["amount"] > block["estimatedBlockReward"]:
            block["blockReward"] = block["estimatedBlockReward"]
            block["blockFeeReward"] = tx["amount"] - block["estimatedBlockReward"]
        else:
            block["blockReward"] = tx["amount"]
            block["blockFeeReward"] = 0
        block["confirmations"] = app["last_block"] -  block["height"] + 1
        block["transactionsCount"] = var_int_to_int(row["header"][80:])
        block["coinbase"] = tx["vIn"][0]["scriptSig"].hex()
        r.append(block)

    resp = {"data": r,
            "time": round(time.time() - q, 4)}
    return resp
