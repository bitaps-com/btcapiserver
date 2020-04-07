import asyncio
import asyncpg
from pybtc import *
from utils import *
from pybtc import rh2s, SCRIPT_N_TYPES
import time
import base64
import aiojsonrpc
from statistics import median




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

    if row is None:
        raise APIException(NOT_FOUND, "block not found", status=404)

    block = dict()
    block["height"] = row["height"]
    block["hash"] = rh2s(row["hash"])
    block["header"] = base64.b64encode(row["header"]).decode()
    block["adjustedTimestamp"] = row["adjusted_timestamp"]

    resp = {"data": block,
            "time": round(time.time() - q, 4)}
    return resp

async def block_data_by_pointer(pointer, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if pointer == 'last':
            stmt = await conn.prepare("SELECT height,"
                                      "       hash,"
                                      "       miner,"
                                      "       timestamp_received,"
                                      "       data,"
                                      "       header,"
                                      "       adjusted_timestamp "
                                      "FROM blocks  ORDER BY height desc LIMIT 1;")
            row = await stmt.fetchrow()
        else:
            if type(pointer) == bytes:
                stmt = await conn.prepare("SELECT height,"
                                          "       hash,"
                                          "       miner,"
                                          "       timestamp_received,"
                                          "       data,"
                                          "       header,"
                                          "       adjusted_timestamp "
                                          "FROM blocks  WHERE hash = $1 LIMIT 1;")
                row = await stmt.fetchrow(pointer)

            elif type(pointer) == int:
                stmt = await conn.prepare("SELECT height,"
                                          "       hash,"
                                          "       miner,"
                                          "       timestamp_received,"
                                          "       data,"
                                          "       header,"
                                          "       adjusted_timestamp "
                                          "FROM blocks  WHERE height = $1 LIMIT 1;")
                row = await stmt.fetchrow(pointer)

        if row is None:
            raise APIException(NOT_FOUND, "block not found", status=404)

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

        block["miner"] = json.loads(row["miner"])
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
        next = await conn.fetchval("SELECT "
                                  "       hash "
                                  "FROM blocks WHERE height = $1;", block["height"] + 1)


        if next is not None:
            block["nextBlockHash"] = rh2s(next)
        else:
            block["nextBlockHash"] = None

        # get coinbase transaction
        cb = await conn.fetchval("SELECT raw_transaction  "
                                "FROM transaction  WHERE pointer = $1  LIMIT 1;",  block["height"] << 39)
        tx = Transaction(cb, format="raw")
        block["estimatedBlockReward"] = 50 * 100000000 >> block["height"] // 210000
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




    resp = {"data": block,
            "time": round(time.time() - q, 4)}
    return resp

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


async def  data_last_n_blocks(n, app):
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
        block["difficulty"] = round(block["targetDifficulty"], 2)
        q = int.from_bytes(s2rh(block["hash"]), byteorder="little")
        block["blockDifficulty"] = target_to_difficulty(q)
        del block["targetDifficulty"]
        try:
            block["nextBlockHash"] = nx_map[block["height"]]
        except:
            block["nextBlockHash"] = None

        # get coinbase transaction

        tx = cb_map[block["height"]]

        block["estimatedBlockReward"] = 50 * 100000000 >> block["height"] // 210000
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
        block["difficulty"] = round(block["targetDifficulty"], 2)
        q = int.from_bytes(s2rh(block["hash"]), byteorder="little")
        block["blockDifficulty"] = target_to_difficulty(q)
        del block["targetDifficulty"]
        try:
            block["nextBlockHash"] = nx_map[block["height"]]
        except:
            block["nextBlockHash"] = None

        # get coinbase transaction

        tx = cb_map[block["height"]]

        block["estimatedBlockReward"] = 50 * 100000000 >> block["height"] // 210000
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
        block["difficulty"] = round(block["targetDifficulty"], 2)
        q = int.from_bytes(s2rh(block["hash"]), byteorder="little")
        block["blockDifficulty"] = target_to_difficulty(q)
        del block["targetDifficulty"]
        try:
            block["nextBlockHash"] = nx_map[block["height"]]
        except:
            block["nextBlockHash"] = None

        # get coinbase transaction

        tx = cb_map[block["height"]]

        block["estimatedBlockReward"] = 50 * 100000000 >> block["height"] // 210000
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
                                  "FROM blocks  WHERE height >= $1 ORDER BY height ASC LIMIT $2;",
                                  pointer, count + 1)
    if not rows:
        raise APIException(NOT_FOUND, "block not found", status=404)
    resp = {"data": [base64.b64encode(row["header"]).decode() for row in rows[1:]],
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
        if address[0] in (0, 1, 5, 6):
            script_hash = True if address[0] in (1,6) else False
            witness = 0 if address[0] in (1,6) else None
            address = hash_to_address(address[1:], testnet=app["testnet"],
                                      script_hash = script_hash, witness_version = witness)
            script = address_to_script(address, hex=1)
        elif address[0] == 2:
            script = address[1:].hex()
            address = script_to_address(address[1:], testnet=app["testnet"])
        else:
            script = address[1:].hex()
            address = None

        utxo.append({"txId": rh2s(row["outpoint"][:32]),
                     "vOut": bytes_to_int(row["outpoint"][32:]),
                     "txIndex": (row["pointer"] >> 20) & 1048575,
                     "amount": row["amount"],
                     "address": address,
                     "script": script,
                     "type": address_type})
    last_page = False if len(rows) > limit else True
    resp = {"data": utxo,
            "time": round(time.time() - q, 4),
            "lastPage": last_page}

    return resp

async def block_transaction_id_list(pointer, limit, page, order, app):
    pool = app["db_pool"]
    q = time.time()

    async with pool.acquire() as conn:
        if isinstance(pointer, bytes):
            stmt = await conn.prepare("SELECT height FROM blocks  WHERE hash = $1 LIMIT 1;")
            pointer = await stmt.fetchval(pointer)
            if pointer is None:
                raise APIException(NOT_FOUND, "block not found", status=404)

    if app["block_transaction_id_list"].has_key(pointer):
        transactions = app["block_transaction_id_list"][pointer]
    else:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT tx_id "
                                    "FROM transaction  WHERE pointer >= $1 AND pointer < $2 "
                                    "ORDER BY pointer %s LIMIT $3 OFFSET $4;" % order,
                                    pointer << 39, (pointer + 1) << 39, limit + 1, limit * (page - 1))
        transactions = [rh2s(t["tx_id"]) for t in rows]
        app["block_transaction_id_list"][pointer] = transactions
    resp = {"data": transactions,
            "time": round(time.time() - q, 4)}
    return resp

async def block_transactions(pointer, option_raw_tx, limit, page, order, mode, app):
    pool = app["db_pool"]
    q = time.time()

    async with pool.acquire() as conn:
        if isinstance(pointer, bytes):
            stmt = await conn.prepare("SELECT height, hash, header, adjusted_timestamp "
                                      "FROM blocks  WHERE hash = $1 LIMIT 1;")
            block_row = await stmt.fetchrow(pointer)
            if block_row is None:
                raise APIException(NOT_FOUND, "block not found", status=404)
            pointer = block_row["height"]
            block_height = block_row["height"]
        else:
            block_height = pointer
            stmt = await conn.prepare("SELECT height, hash, header, adjusted_timestamp "
                                      "FROM blocks  WHERE height = $1 LIMIT 1;")
            block_row = await stmt.fetchrow(pointer)
        if block_row is None:
            raise APIException(NOT_FOUND, "block not found", status=404)


        if 1==9 and app["block_transactions"].has_key(block_row["hash"]):
            transactions = app["block_transactions"][block_row["hash"]]
        else:
            if app["merkle_proof"]:
                rows = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer, merkle_proof  "
                                        "FROM transaction  WHERE pointer >= $1 AND pointer < $2 "
                                        "ORDER BY pointer %s LIMIT $3 OFFSET $4;" % order,
                                        pointer << 39, (pointer + 1) << 39, limit + 1, limit * (page - 1))
            else:
                rows = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer  "
                                        "FROM transaction  WHERE pointer >= $1 AND pointer < $2 "
                                        "ORDER BY pointer %s LIMIT $3 OFFSET $4;" % order,
                                        pointer << 39, (pointer + 1) << 39, limit + 1, limit * (page - 1))
            block_time = unpack("<L", block_row["header"][68: 68 + 4])[0]

            transactions = list()
            for row in rows:
                tx = Transaction(row["raw_transaction"], format="decoded",
                                 testnet=app["testnet"], keep_raw_tx=option_raw_tx)
                tx["blockIndex"] = (row["pointer"] >> 20) & 524287
                tx["blockTime"] = block_time
                tx["timestamp"] = row["timestamp"]
                tx["confirmations"] = app["last_block"] - block_height + 1
                if app["merkle_proof"]:
                    tx["merkleProof"] = base64.b64encode(row["merkle_proof"]).decode()

                del tx["blockHash"]
                del tx["blockTime"]
                del tx["format"]
                del tx["testnet"]
                del tx["time"]
                del tx["fee"]
                if not option_raw_tx:
                    del tx["rawTx"]
                if app["transaction_history"]:
                    tx["inputsAmount"] = 0
                if mode == "brief":
                    tx["outputAddresses"] = 0
                    tx["inputAddresses"] = 0
                    for z in tx["vOut"]:
                        if "address" in tx["vOut"][z]:
                            tx["outputAddresses"] += 1
                transactions.append(tx)
            app["block_transactions"][block_row["hash"]] = transactions
        if app["transaction_history"]:
            # get information about spent input coins
            rows = await conn.fetch("SELECT stxo.pointer,"
                                    "       stxo.s_pointer,"
                                    "       transaction_map.address, "
                                    "       transaction_map.amount  "
                                    "FROM stxo "
                                    "JOIN transaction_map "
                                    "ON transaction_map.pointer = stxo.pointer "
                                    "WHERE stxo.s_pointer >= $1 and  stxo.s_pointer < $2 order by stxo.s_pointer asc;",
                                    (block_height << 39) + ((limit * (page - 1)) << 20) ,
                                    ((block_height ) << 39) + ((limit * (page )) << 20))

            for r in rows:
                s = r["s_pointer"]
                i = (s - ((s >> 19) << 19))
                m = ((s - ((s >> 39) << 39)) >> 20) - limit * (page -1)
                transactions[m]["inputsAmount"] += r["amount"]

                if mode == "verbose":

                    transactions[m]["vIn"][i]["type"] = SCRIPT_N_TYPES[r["address"][0]]
                    transactions[m]["vIn"][i]["amount"] = r["amount"]
                    transactions[m]["vIn"][i]["blockHeight"] = r["pointer"] >> 39
                    transactions[m]["vIn"][i]["confirmations"] = app["last_block"] - (r["pointer"] >> 39) + 1


                    if r["address"][0] in (0, 1, 5, 6):
                        script_hash = True if r["address"][0] in (1, 6) else False
                        witness_version = None if r["address"][0] < 5 else 0
                        transactions[m]["vIn"][i]["address"] = hash_to_address(r["address"][1:],
                                                                               testnet=app["testnet"],
                                                                               script_hash=script_hash,
                                                                               witness_version=witness_version)
                        transactions[m]["vIn"][i]["scriptPubKey"] = address_to_script(
                            transactions[m]["vIn"][i]["address"],
                            hex=1)
                    elif r["address"][0] == 2:
                        transactions[m]["vIn"][i]["address"] = script_to_address(r["address"][1:],
                                                                                 testnet=app["testnet"])
                        transactions[m]["vIn"][i]["scriptPubKey"] = r["address"][1:].hex()
                    else:
                        transactions[m]["vIn"][i]["scriptPubKey"] = r["address"][1:].hex()
                    transactions[m]["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(
                        transactions[m]["vIn"][i]["scriptPubKey"])
                    transactions[m]["vIn"][i]["scriptPubKeyAsm"] = decode_script(
                        transactions[m]["vIn"][i]["scriptPubKey"], 1)
                else:
                    if r["address"][0] in (0, 1, 2, 5, 6):
                        if mode == "brief":
                            transactions[m]["inputAddresses"] += 1


            for m in range(len(transactions)):
                transactions[m]["fee"] = transactions[m]["inputsAmount"] - transactions[m]["amount"]
                transactions[m]["outputsAmount"] = transactions[m]["amount"]
                if mode != "verbose":
                    transactions[m]["inputs"] = len(transactions[m]["vIn"])
                    transactions[m]["outputs"] = len(transactions[m]["vOut"])
                    del transactions[m]["vIn"]
                    del transactions[m]["vOut"]
            transactions[0]["fee"] = 0
            # get information about spent output coins
            if mode == "verbose":
                # get information about spent output coins
                rows = await conn.fetch("SELECT   outpoint,"
                                        "         input_index,"
                                          "       tx_id "
                                          "FROM connector_unconfirmed_stxo "
                                          "WHERE out_tx_id = ANY($1);", [s2rh(t["txId"]) for t in transactions])
                out_map = dict()
                for v in rows:
                    i = bytes_to_int(v["outpoint"][32:])
                    try:
                        out_map[(rh2s(v["outpoint"][:32]), i)].append({"txId": rh2s(v["tx_id"]), "vIn": v["input_index"]})
                    except:
                        out_map[(rh2s(v["outpoint"][:32]), i)] = [{"txId": rh2s(v["tx_id"]), "vIn": v["input_index"]}]

                rows = await conn.fetch("SELECT stxo.pointer,"
                                        "       stxo.s_pointer,"
                                        "       transaction.tx_id  "
                                        "FROM stxo "
                                        "JOIN transaction "
                                        "ON transaction.pointer = (stxo.s_pointer >> 18)<<18 "
                                        "WHERE stxo.pointer >= $1 and "
                                        "stxo.pointer < $2  order by stxo.pointer ;",
                                        (block_height << 39) + ((limit * (page - 1) ) << 20),
                                        ((block_height) << 39) + ((limit * page ) << 20))

                p_out_map = dict()
                for v in rows:
                    p_out_map[v["pointer"]] = [{"txId": rh2s(v["tx_id"]),
                                                   "vIn": v["s_pointer"] & 0b111111111111111111}]
                for t in range(len(transactions)):
                    o_pointer = (block_height << 39) + (transactions[t]["blockIndex"] << 20) + (1 << 19)
                    for i in transactions[t]["vOut"]:
                        try:
                            transactions[t]["vOut"][i]["spent"] = p_out_map[o_pointer + i]
                        except:
                            try:
                                transactions[t]["vOut"][i]["spent"] = out_map[(transactions[t]["txId"], int(i))]
                            except:
                                transactions[t]["vOut"][i]["spent"] = []


    resp = {"data": transactions,
            "time": round(time.time() - q, 4)}
    return resp


async def block_filters_headers(filter_type, start_height, stop_hash, log, app):
    pool = app["db_pool"]
    q = time.time()
    n = 0
    type_list = []
    while filter_type:
        if filter_type & 1:
            type_list.append(2 ** n)
        n += 1
        filter_type = filter_type >> 1

    async with pool.acquire() as conn:
        stmt = await conn.prepare("SELECT height "
                                  "FROM blocks  WHERE hash = $1 LIMIT 1;")
        stop_height = await stmt.fetchval(stop_hash)
        if stop_height - start_height > 1999:
            raise APIException(PARAMETER_ERROR, "only <=2000 headers per request", status=400)

        if stop_height is None:
            raise APIException(NOT_FOUND, "stop hash block not found", status=404)

        rows = await conn.fetch("SELECT type, hash, height "
                                "FROM block_filter  WHERE height >= $1 and height <= $2 "
                                "and type = ANY($3) "
                                "ORDER BY height;",
                                start_height, stop_height, type_list)

        h = dict()
        result = []
        for row in rows:
            filter_type |= row["type"]
            try:
                h[row["height"]][row["type"]] = row["hash"]
            except:
                h[row["height"]] = {row["type"]: row["hash"]}

        for i in range(start_height, stop_height + 1):
            try:
                result.append(merkle_root([h[i][j] for j in sorted(h[i].keys())]))
            except:
                result.append("00" * 32)

        assert len(result) <= stop_height - start_height + 1

    if not rows:
        raise APIException(NOT_FOUND, "blocks not found", status=404)
    resp = {"data": {"type":filter_type,
                     "headers": result},
            "time": round(time.time() - q, 4)}
    return resp

async def block_filters_batch_headers(filter_type, start_height, stop_hash, log, app):
    pool = app["db_pool"]
    q = time.time()
    n = 0
    type_list = []
    while filter_type:
        if filter_type & 1:
            type_list.append(2 ** n)
        n += 1
        filter_type = filter_type >> 1

    async with pool.acquire() as conn:
        stmt = await conn.prepare("SELECT height "
                                  "FROM blocks  WHERE hash = $1 LIMIT 1;")
        stop_height = await stmt.fetchval(stop_hash)
        l = (start_height // 1000)
        if start_height % 1000 > 0:
            l += 1
        l = l * 1000
        b = [l * 1000]
        while l < stop_height:
            l += 1000
            b.append(l)

        if stop_height is None:
            raise APIException(NOT_FOUND, "stop hash block not found", status=404)

        rows = await conn.fetch("SELECT type, hash, height "
                                "FROM block_filter  WHERE height = ANY($1) "
                                "and type = ANY($2) "
                                "ORDER BY height;",
                                b, type_list)

        h = dict()
        result = []
        for row in rows:
            filter_type |= row["type"]
            try:
                h[row["height"]][row["type"]] = row["hash"]
            except:
                h[row["height"]] = {row["type"]: row["hash"]}

        for i in sorted(b):
            try:
                result.append(merkle_root([h[i][j] for j in sorted(h[i].keys())]))
            except:
                result.append("00" * 32)

        assert len(result) <= stop_height - start_height + 1

    if not rows:
        raise APIException(NOT_FOUND, "blocks not found", status=404)
    resp = {"data": {"type":filter_type,
                     "headers": result},
            "time": round(time.time() - q, 4)}
    return resp

async def block_filters(filter_type, start_height, stop_hash, log, app):
    pool = app["db_pool"]
    q = time.time()
    n = 0
    type_list = []
    while filter_type:
        if filter_type & 1:
            type_list.append(2 ** n)
        n += 1
        filter_type = filter_type >> 1

    async with pool.acquire() as conn:
        stmt = await conn.prepare("SELECT height "
                                  "FROM blocks  WHERE hash = $1 LIMIT 1;")
        stop_height = await stmt.fetchval(stop_hash)
        if stop_height - start_height > 999:
            raise APIException(PARAMETER_ERROR, "only <=1000 filters per request", status=400)

        if stop_height is None:
            raise APIException(NOT_FOUND, "stop hash block not found", status=404)

        rows = await conn.fetch("SELECT type, filter, height "
                                "FROM block_filter  WHERE height >= $1 and height <= $2 "
                                "and type = ANY($3) "
                                "ORDER BY height;",
                                start_height, stop_height, type_list)

        h = dict()
        result = []
        for row in rows:
            filter_type |= row["type"]
            try:
                h[row["height"]][row["type"]] = base64.b64encode(row["filter"]).decode()
            except:
                h[row["height"]] = {row["type"]: base64.b64encode(row["filter"]).decode()}

        for i in range(start_height, stop_height + 1):
            try:
                result.append(h[i])
            except:
                result.append(dict())

        assert len(result) <= stop_height - start_height + 1

    if not rows:
        raise APIException(NOT_FOUND, "blocks not found", status=404)
    resp = {"data": {"type":filter_type,
                     "filters": result},
            "time": round(time.time() - q, 4)}
    return resp


async def tx_by_pointer_opt_tx(pointer, option_raw_tx, app):
    q = time.time()
    block_height = None
    block_index = None
    block_hash = None

    async with app["db_pool"].acquire() as conn:
        if isinstance(pointer, bytes):
            row = await conn.fetchrow("SELECT tx_id, raw_transaction, timestamp "
                                         "FROM unconfirmed_transaction "
                                         "WHERE tx_id = $1 LIMIT 1;", pointer)
            if row is None:
                if app["merkle_proof"]:
                    row = await conn.fetchrow("SELECT tx_id, raw_transaction,  timestamp, pointer, "
                                              "merkle_proof "
                                              "FROM transaction  "
                                              "WHERE tx_id = $1 LIMIT 1;", pointer)
                else:
                    row = await conn.fetchrow("SELECT tx_id, raw_transaction,  timestamp, pointer "
                                              "FROM transaction  "
                                              "WHERE tx_id = $1 LIMIT 1;", pointer)
                if row is None:
                    raise APIException(NOT_FOUND, "transaction not found", status=404)

                block_height = row["pointer"] >> 39
                block_index = (row["pointer"] >> 20) & 524287
                block_row = await conn.fetchrow("SELECT hash, header, adjusted_timestamp "
                                                "FROM blocks  "
                                                "WHERE height = $1 LIMIT 1;", block_height)
                adjusted_timestamp = block_row["adjusted_timestamp"]
                block_time = unpack("<L", block_row["header"][68: 68 + 4])[0]
                block_hash = block_row["hash"]

        else:
            if app["merkle_proof"]:
                row = await conn.fetchrow("SELECT tx_id, raw_transaction, timestamp, pointer, "
                                          "merkle_proof "
                                          "FROM transaction "
                                          "WHERE pointer = $1 LIMIT 1;", pointer)
            else:
                row = await conn.fetchrow("SELECT tx_id, raw_transaction, timestamp, pointer "
                                          "FROM transaction "
                                          "WHERE pointer = $1 LIMIT 1;", pointer)
            if row is None:
                raise APIException(NOT_FOUND, "transaction not found", status=404)
            block_height = row["pointer"] >> 39
            block_index = (row["pointer"] >> 20) & 524287
            block_row = await conn.fetchrow("SELECT hash, header, adjusted_timestamp "
                                             "FROM blocks  "
                                             "WHERE height = $1 LIMIT 1;", block_height)
            adjusted_timestamp = block_row["adjusted_timestamp"]
            block_time = unpack("<L", block_row["header"][68: 68+4])[0]
            block_hash = block_row["hash"]


        tx = Transaction(row["raw_transaction"], format="decoded", testnet=app["testnet"], keep_raw_tx=option_raw_tx)

        if app["transaction_history"]:
            for i in tx["vOut"]:
                tx["vOut"][i]['spent'] = []

            # get information about spent input coins
            if block_height is not None:
                s_pointers = [(block_height<<39)+(block_index<<20)+i for i in tx["vIn"]]


                rows = await conn.fetch("SELECT stxo.pointer,"
                                        "       stxo.s_pointer,"
                                        "       transaction_map.address, "
                                        "       transaction_map.amount  "
                                        "FROM stxo "
                                        "JOIN transaction_map "
                                        "ON transaction_map.pointer = stxo.pointer "
                                   
                                        "WHERE stxo.s_pointer = ANY($1);", s_pointers)


                tx["inputsAmount"] = 0
                if not tx["coinbase"]:
                    assert len(rows) == len(s_pointers)

                for r in rows:
                    s = r["s_pointer"]
                    i = (s - ((s>>19)<<19))

                    tx["vIn"][i]["type"] = SCRIPT_N_TYPES[r["address"][0]]
                    tx["vIn"][i]["amount"] = r["amount"]
                    tx["inputsAmount"] += r["amount"]
                    tx["vIn"][i]["blockHeight"] = r["pointer"] >> 39
                    tx["vIn"][i]["confirmations"] = app["last_block"] - (r["pointer"] >> 39) + 1
                    if r["address"][0] in (0,1,2,5,6):
                        script_hash = True if r["address"][0] in (1, 6) else False
                        witness_version = None if r["address"][0] < 5 else 0
                        tx["vIn"][i]["address"] = hash_to_address(r["address"][1:],
                                                                  testnet=app["testnet"],
                                                                  script_hash = script_hash,
                                                                  witness_version = witness_version)
                        tx["vIn"][i]["scriptPubKey"] = address_to_script(tx["vIn"][i]["address"], hex=1)
                    elif r["address"][0] == 2:
                        tx["vIn"][i]["address"] = script_to_address(r["address"][1:], testnet=app["testnet"])
                        tx["vIn"][i]["scriptPubKey"] = r["address"][1:].hex()
                    else:
                        tx["vIn"][i]["scriptPubKey"] = r["address"][1:].hex()
                    tx["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(tx["vIn"][i]["scriptPubKey"])
                    tx["vIn"][i]["scriptPubKeyAsm"] = decode_script(tx["vIn"][i]["scriptPubKey"], 1)
            else:
                rows = await conn.fetch("SELECT   outpoint,"
                                        "         input_index,"
                                        "       out_tx_id, "
                                        "       tx_id,"
                                        "       connector_unconfirmed_stxo.address, "
                                        "       connector_unconfirmed_stxo.amount "
                                        "    "
                                        "FROM connector_unconfirmed_stxo "
                                        "WHERE tx_id =  $1;", s2rh(tx["txId"]))
                c_rows = await conn.fetch("""
                                           SELECT pointer, tx_id FROM transaction 
                                           WHERE tx_id = ANY($1);
                                           """, [s2rh(tx["vIn"][q]["txId"]) for q in tx["vIn"]])
                tx_id_map_pointer = dict()
                for r in c_rows:
                    tx_id_map_pointer[rh2s(r["tx_id"])] = r["pointer"]
                tx["inputsAmount"] = 0

                if not tx["coinbase"]:
                    assert len(rows) == len(tx["vIn"])

                for r in rows:
                    i = r["input_index"]

                    tx["vIn"][i]["type"] = SCRIPT_N_TYPES[r["address"][0]]
                    tx["vIn"][i]["amount"] = r["amount"]
                    tx["inputsAmount"] += r["amount"]
                    try:
                        p = tx_id_map_pointer[tx["vIn"][i]["txId"]]
                        tx["vIn"][i]["blockHeight"] = p >> 39
                        tx["vIn"][i]["confirmations"] = app["last_block"] - (p >> 39) + 1
                    except:
                        tx["vIn"][i]["blockHeight"] = None
                        tx["vIn"][i]["confirmations"] = None
                    if r["address"][0] in (0, 1, 2, 5, 6):
                        script_hash = True if r["address"][0] in (1, 6) else False
                        witness_version = None if r["address"][0] < 5 else 0
                        try:
                            if r["address"][0] == 2:
                                ad = b"\x02" + parse_script(r["address"][1:])["addressHash"]
                            else:
                                ad = r["address"]
                            tx["vIn"][i]["address"] = hash_to_address(ad[1:],
                                                                      testnet=app["testnet"],
                                                                      script_hash=script_hash,
                                                                      witness_version=witness_version)
                            tx["vIn"][i]["scriptPubKey"] = address_to_script(tx["vIn"][i]["address"], hex=1)
                        except:
                            print("???", r["address"].hex())
                            raise
                    elif r["address"][0] == 2:
                        tx["vIn"][i]["address"] = script_to_address(r["address"][1:], testnet=app["testnet"])
                        tx["vIn"][i]["scriptPubKey"] = r["address"][1:].hex()
                    else:
                        tx["vIn"][i]["scriptPubKey"] = r["address"][1:].hex()
                    tx["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(tx["vIn"][i]["scriptPubKey"])
                    tx["vIn"][i]["scriptPubKeyAsm"] = decode_script(tx["vIn"][i]["scriptPubKey"], 1)

            # get information about spent output coins
            rows = await conn.fetch("SELECT   outpoint,"
                                    "         input_index,"
                                      "       tx_id "
                                      "FROM connector_unconfirmed_stxo "
                                      "WHERE out_tx_id = $1;", s2rh(tx["txId"]))
            for r in rows:
                i = bytes_to_int(r["outpoint"][32:])
                tx["vOut"][i]['spent'].append({"txId": rh2s(r["tx_id"]), "vIn": r["input_index"]})

            if block_height is not None:
                pointers = [(block_height << 39) +(block_index << 20) + (1 << 19) + i for i in tx["vOut"]]
                rows = await conn.fetch("SELECT stxo.pointer,"
                                        "       stxo.s_pointer,"
                                        "       transaction.tx_id  "
                                        "FROM stxo "
                                        "JOIN transaction "
                                        "ON transaction.pointer = (stxo.s_pointer >> 18)<<18 "
                                        "WHERE stxo.pointer = ANY($1);", pointers)
                for r in rows:
                    i =  r["pointer"] & 0b111111111111111111
                    tx["vOut"][i]['spent'].append({"txId": rh2s(r["tx_id"]),
                                                   "vIn": r["s_pointer"] & 0b111111111111111111})
        if not tx["coinbase"]:
            tx["fee"] = tx["inputsAmount"] - tx["amount"]
        else:
            tx["fee"] = 0

        tx["outputsAmount"] = tx["amount"]




    tx["blockHeight"] = block_height
    tx["blockIndex"] = block_index
    if block_hash is not None:
        tx["blockHash"] = rh2s(block_hash)
        tx["adjustedTimestamp"] = adjusted_timestamp
        tx["blockTime"] = block_time
    else:
        tx["blockHash"] = None
        tx["adjustedTimestamp"] = None
        tx["blockTime"] = None
    tx["time"] = row["timestamp"]
    if block_height:
        tx["confirmations"] =  app["last_block"] - block_height + 1
        if app["merkle_proof"]:
            tx["merkleProof"] = base64.b64encode(row["merkle_proof"]).decode()


    del tx["format"]
    del tx["testnet"]
    if not app["transaction_history"]:
        del tx["fee"]
    if not option_raw_tx:
        del tx["rawTx"]
    else:
        tx["rawTx"] = base64.b64encode(bytes_from_hex(tx["rawTx"])).decode()

    return {"data": tx,
            "time": round(time.time() - q, 4)}

async def tx_by_pointers_opt_tx(pointers, hashes, option_raw_tx, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        h_rows, uh_rows, p_row = [], [], []
        if hashes:
            if app["merkle_proof"]:
                h_rows = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer, merkle_proof,"
                                          "       blocks.hash, header, adjusted_timestamp "
                                          "FROM transaction "
                                          "JOIN blocks ON "
                                          "blocks.height = pointer >> 39 "
                                          "WHERE tx_id = ANY($1);", hashes)
            else:
                h_rows = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer, "
                                          "       blocks.hash, header, adjusted_timestamp "
                                          "FROM transaction "
                                          "JOIN blocks ON "
                                          "blocks.height = pointer >> 39 "
                                          "WHERE tx_id = ANY($1);", hashes)
        if len(h_rows) < len(hashes):
            uh_rows = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp "
                                         "FROM unconfirmed_transaction "
                                         "WHERE tx_id = ANY($1);", hashes)
        if pointers:
            if app["merkle_proof"]:
                p_row = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer, merkle_proof,  "
                                          "       blocks.hash, header, adjusted_timestamp "
                                          "FROM transaction "
                                          " JOIN blocks ON "
                                          "blocks.height = pointer >> 39 "
                                          "WHERE pointer = ANY($1);", pointers)
            else:
                p_row = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer, merkle_proof,  "
                                          "       blocks.hash, header, adjusted_timestamp "
                                          "FROM transaction "
                                          "JOIN blocks ON "
                                          "blocks.height = pointer >> 39 "
                                          "WHERE pointer = ANY($1);", pointers)


        r = dict()
        txs = dict()
        s_pointers = []
        o_pointers = []
        tx_id_list = []
        for row in h_rows:
            block_height = row["pointer"] >> 39
            block_index = (row["pointer"] >> 20) & 524287
            block_time = unpack("<L", row["header"][68: 68 + 4])[0]
            tx = Transaction(row["raw_transaction"],format="decoded",testnet=app["testnet"],keep_raw_tx=option_raw_tx)
            tx["blockHeight"] = block_height
            tx["blockIndex"] = block_index
            tx["blockHash"] = rh2s(row["hash"])
            tx["adjustedTimestamp"] = row["adjusted_timestamp"]
            tx["blockTime"] = block_time
            tx["timestamp"] = row["timestamp"]
            tx["confirmations"] = app["last_block"] - block_height + 1
            if app["merkle_proof"]: tx["merkleProof"] =  base64.b64encode(row["merkle_proof"]).decode()
            del tx["format"]
            del tx["testnet"]
            del tx["fee"]
            del tx["rawTx"]
            r[tx["txId"]] = tx
            if app["transaction_history"]:
                [s_pointers.append((block_height << 39) + (block_index << 20) + i) for i in tx["vIn"]]
                [o_pointers.append((block_height << 39) + (block_index << 20) + (1 << 19) + i) for i in tx["vIn"]]
                tx_id_list.append(s2rh(tx["txId"]))

        us_pointers = []
        us_pointers_inputs = []

        for row in uh_rows:
            block_height = None
            block_index = None
            block_time = None
            tx = Transaction(row["raw_transaction"],format="decoded",testnet=app["testnet"],keep_raw_tx=option_raw_tx)
            tx["blockHeight"] = block_height
            tx["blockIndex"] = block_index
            tx["blockHash"] = None
            tx["adjustedTimestamp"] = None
            tx["blockTime"] = block_time
            tx["time"] = row["timestamp"]
            tx["confirmations"] = 0
            del tx["format"]
            del tx["testnet"]
            del tx["fee"]
            if not option_raw_tx: del tx["rawTx"]
            r[tx["txId"]] = tx
            if app["transaction_history"]:
                us_pointers.append(s2rh(tx["txId"]))
                tx_id_list.append(s2rh(tx["txId"]))
                [us_pointers_inputs.append(s2rh(tx["vIn"][v]["txId"])) for v in tx["vIn"]]


        for row in p_row:
            block_height = row["pointer"] >> 39
            block_index = (row["pointer"] >> 20) & 524287
            block_time = unpack("<L", row["header"][68: 68 + 4])[0]
            tx = Transaction(row["raw_transaction"],format="decoded",testnet=app["testnet"],keep_raw_tx=option_raw_tx)
            tx["blockHeight"] = block_height
            tx["blockIndex"] = block_index
            tx["blockHash"] = rh2s(row["hash"])
            tx["adjustedTimestamp"] = row["adjusted_timestamp"]
            tx["blockTime"] = block_time
            tx["timestamp"] = row["timestamp"]
            tx["confirmations"] = app["last_block"] - block_height + 1
            if app["merkle_proof"]: tx["merkleProof"] = base64.b64encode(row["merkle_proof"]).decode()
            del tx["format"]
            del tx["testnet"]
            del tx["fee"]
            if not option_raw_tx: del tx["rawTx"]
            pointer = row["pointer"]
            r["%s:%s" % (pointer >> 39, (pointer >> 20) & 524287)] = tx
            if app["transaction_history"]:
                [s_pointers.append((block_height << 39) + (block_index << 20) + i) for i in tx["vIn"]]
                tx_id_list.append(s2rh(tx["txId"]))
                [o_pointers.append((block_height << 39) + (block_index << 20) + (1 << 19) + i) for i in tx["vIn"]]

        if app["transaction_history"]:
            # get information about spent input coins

            if s_pointers:
                rows = await conn.fetch("SELECT stxo.pointer,"
                                        "       stxo.s_pointer,"
                                        "       transaction_map.address, "
                                        "       transaction_map.amount  "
                                        "FROM stxo "
                                        "JOIN transaction_map "
                                        "ON transaction_map.pointer = stxo.pointer "

                                        "WHERE stxo.s_pointer = ANY($1);", s_pointers)
                s_pointers_map = dict()
                for v in rows:
                    s_pointers_map[v["s_pointer"]] = v


                for t in r:
                    if r[t]["blockHeight"] is None:
                        continue

                    s_pointer = (r[t]["blockHeight"] << 39) + (r[t]["blockIndex"] << 20)
                    r[t]["inputsAmount"] = 0
                    for i in r[t]["vIn"]:
                        if r[t]["coinbase"]:
                            continue
                        d = s_pointers_map[s_pointer + i]
                        r[t]["vIn"][i]["type"] = SCRIPT_N_TYPES[d["address"][0]]
                        r[t]["vIn"][i]["amount"] = d["amount"]
                        r[t]["inputsAmount"] += d["amount"]
                        r[t]["vIn"][i]["blockHeight"] = d["pointer"] >> 39
                        r[t]["vIn"][i]["confirmations"] = app["last_block"] - (d["pointer"] >> 39) + 1
                        if d["address"][0] in (0,1,2,5,6):
                            script_hash = True if d["address"][0] in (1, 6) else False
                            witness_version = None if d["address"][0] < 5 else 0
                            r[t]["vIn"][i]["address"] = hash_to_address(d["address"][1:],
                                                                        testnet=app["testnet"],
                                                                        script_hash = script_hash,
                                                                         witness_version = witness_version)
                            r[t]["vIn"][i]["scriptPubKey"] = address_to_script(r[t]["vIn"][i]["address"], hex=1)
                        elif r["address"][0] == 2:
                            r[t]["vIn"][i]["address"] = script_to_address(d["address"][1:], testnet=app["testnet"])
                            r[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                        else:
                            r[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                        r[t]["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(r[t]["vIn"][i]["scriptPubKey"])
                        r[t]["vIn"][i]["scriptPubKeyAsm"] = decode_script(r[t]["vIn"][i]["scriptPubKey"], 1)

            if us_pointers:
                rows = await conn.fetch("SELECT   outpoint,"
                                        "         input_index,"
                                        "       out_tx_id, "
                                        "       tx_id,"
                                        "       connector_unconfirmed_stxo.address, "
                                        "       connector_unconfirmed_stxo.amount "
                                        "    "
                                        "FROM connector_unconfirmed_stxo "
                                        "WHERE tx_id =  ANY($1);", us_pointers)
                c_rows = await conn.fetch("""
                                           SELECT pointer, tx_id FROM transaction 
                                           WHERE tx_id = ANY($1);
                                           """, us_pointers_inputs)
                tx_id_map_pointer = dict()

                for v in c_rows:
                    tx_id_map_pointer[rh2s(v["tx_id"])] = v["pointer"]


                us_pointers = dict()
                for v in rows:
                    us_pointers[(rh2s(v["tx_id"]), v["input_index"])] = v


                for t in r:
                    if r[t]["blockHeight"] is not None:
                        continue

                    r[t]["inputsAmount"] = 0
                    for i in r[t]["vIn"]:
                        d = us_pointers[(r[t]["txId"], i)]
                        r[t]["vIn"][i]["type"] = SCRIPT_N_TYPES[d["address"][0]]
                        r[t]["vIn"][i]["amount"] = d["amount"]
                        r[t]["inputsAmount"] += d["amount"]
                        try:
                            pointer = tx_id_map_pointer[r[t]["vIn"][i]["txId"]]
                            r[t]["vIn"][i]["blockHeight"] = pointer >> 39
                            r[t]["vIn"][i]["confirmations"] = app["last_block"] - (pointer >> 39) + 1
                        except:
                            r[t]["vIn"][i]["blockHeight"] = None
                            r[t]["vIn"][i]["confirmations"] = None

                        if d["address"][0] in (0,1,2,5,6):
                            script_hash = True if d["address"][0] in (1, 6) else False
                            witness_version = None if d["address"][0] < 5 else 0
                            try:
                                if d["address"][0] == 2:
                                    ad = b"\x02" + parse_script(d["address"][1:])["addressHash"]
                                else:
                                    ad = d["address"]
                                r[t]["vIn"][i]["address"] = hash_to_address(ad[1:],
                                                                            testnet=app["testnet"],
                                                                            script_hash = script_hash,
                                                                             witness_version = witness_version)

                                r[t]["vIn"][i]["scriptPubKey"] = address_to_script(r[t]["vIn"][i]["address"], hex=1)
                            except:
                                print(r[t]["txId"])
                                print("??", d["address"].hex())
                                raise

                        elif r["address"][0] == 2:
                            r[t]["vIn"][i]["address"] = script_to_address(d["address"][1:], testnet=app["testnet"])
                            r[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                        else:
                            r[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                        r[t]["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(r[t]["vIn"][i]["scriptPubKey"])
                        r[t]["vIn"][i]["scriptPubKeyAsm"] = decode_script(r[t]["vIn"][i]["scriptPubKey"], 1)

            # get information about spent output coins
            rows = await conn.fetch("SELECT   outpoint,"
                                    "         input_index,"
                                      "       tx_id "
                                      "FROM connector_unconfirmed_stxo "
                                      "WHERE out_tx_id = ANY($1);", tx_id_list)
            out_map = dict()
            for v in rows:
                i = bytes_to_int(v["outpoint"][32:])
                try:
                    out_map[(rh2s(v["outpoint"][:32]), i)].append({"txId": rh2s(v["tx_id"]), "vIn": v["input_index"]})
                except:
                    out_map[(rh2s(v["outpoint"][:32]), i)] = [{"txId": rh2s(v["tx_id"]), "vIn": v["input_index"]}]


            rows = await conn.fetch("SELECT stxo.pointer,"
                                    "       stxo.s_pointer,"
                                    "       transaction.tx_id  "
                                    "FROM stxo "
                                    "JOIN transaction "
                                    "ON transaction.pointer = (stxo.s_pointer >> 18)<<18 "
                                    "WHERE stxo.pointer = ANY($1);", o_pointers)
            p_out_map = dict()
            for v in rows:
                p_out_map[v["pointer"]] = [{"txId": rh2s(v["tx_id"]),
                                               "vIn": v["s_pointer"] & 0b111111111111111111}]

            for t in r:
                if r[t]["blockHeight"] is not None:
                    o_pointer = (r[t]["blockHeight"] << 39) + (r[t]["blockIndex"] << 20) + (1 << 19)
                    for i in r[t]["vOut"]:
                        try:
                            r[t]["vOut"][i]["spent"] = p_out_map[o_pointer + i]
                        except:
                            try:
                                r[t]["vOut"][i]["spent"] = out_map[(r[t]["txId"], int(i))]
                            except:
                                r[t]["vOut"][i]["spent"] = []
                else:
                    for i in r[t]["vOut"]:
                        try:
                            r[t]["vOut"][i]["spent"] = out_map[(r[t]["txId"], int(i))]
                        except:
                            r[t]["vOut"][i]["spent"] = []



        for pointer in pointers:
            key = "%s:%s" % (pointer >> 39, (pointer >> 20) & 524287)
            try:
                txs[key] = r[key]
            except:
                txs[key] = None

        for h in hashes:
            h = rh2s(h)
            try:
                txs[h] = r[h]
            except:
                txs[h] = None

    return {"data": txs,
            "time": round(time.time() - q, 4)}


async def tx_hash_by_pointer(pointer, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        row = await conn.fetchrow("SELECT tx_id "
                                  "FROM transaction "
                                  "WHERE pointer = $1 LIMIT 1;", pointer)
        if row is None:
            raise APIException(NOT_FOUND, "transaction not found", status=404)


    return {"data": rh2s(row["tx_id"]),
            "time": round(time.time() - q, 4)}

async def tx_hash_by_pointers(pointers, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT pointer, tx_id "
                                  "FROM transaction "
                                  "WHERE pointer = ANY($1);", pointers)

    d = dict()
    r = dict()
    for row in rows:
        d[row["pointer"]] = row["tx_id"]
    for pointer in pointers:
        try:
            key= "%s:%s" % (pointer >> 39, (pointer >> 20) & 1048575)
            r[key] = rh2s(d[pointer])
        except:
            r[key] = None

    return {"data": r,
            "time": round(time.time() - q, 4)}


async def tx_merkle_proof_by_pointer(pointer, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        if isinstance(pointer, bytes):
            row = await conn.fetchrow("SELECT merkle_proof, pointer "
                                      "FROM transaction "
                                      "WHERE tx_id = $1 LIMIT 1;", pointer)
            if row is None:
                raise APIException(NOT_FOUND, "transaction not found", status=404)

        else:
            row = await conn.fetchrow("SELECT merkle_proof, pointer "
                                      "FROM transaction "
                                      "WHERE pointer = $1 LIMIT 1;", pointer)
            if row is None:
                raise APIException(NOT_FOUND, "transaction not found", status=404)

    return {"data": {"blockHeight": row["pointer"] >> 39,
                     "blockIndex": (row["pointer"] >> 20) & 524287,
                     "merkleProof": base64.b64encode(row["merkle_proof"]).decode()},
            "time": round(time.time() - q, 4)}


async def calculate_tx_merkle_proof_by_pointer(pointer, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        if isinstance(pointer, bytes):
            row = await conn.fetchval("SELECT pointer "
                                      "FROM transaction "
                                      "WHERE tx_id = $1 LIMIT 1;", pointer)
            if row is None:
                raise APIException(NOT_FOUND, "transaction not found", status=404)

        else:
            row = await conn.fetchval("SELECT pointer "
                                      "FROM transaction "
                                      "WHERE pointer = $1 LIMIT 1;", pointer)
            if row is None:
                raise APIException(NOT_FOUND, "transaction not found", status=404)

        block_height = row >> 39
        block_index = (row >> 20) & 524287

        if not app["merkle_tree_cache"].has_key(block):
            rows = await conn.fetch("SELECT tx_id FROM transaction "
                                    "WHERE pointer >= $1 and pointer < $2 "
                                    "ORDER BY pointer ASC;",
                                    block_height << 39, (block_height + 1) << 39)
            block_header = await conn.fetchval("SELECT header FROM blocks "
                                               "WHERE height = $1 LIMIT 1;", block_height)
            mrt = block_header[36:36+32]
            hash_list = [row["tx_id"] for row in rows]
            mt = merkle_tree(hash_list)
            mr = merkle_root(hash_list, receive_hex=False, return_hex=False)
            assert mrt == mr
            app["merkle_tree_cache"][block_height] = (mt, mrt)

        mrt = app["merkle_tree_cache"][block_height][1]
        mpf = merkle_proof(app["merkle_tree_cache"][block_height][0], block_index, return_hex=False)
        assert merkle_root_from_proof(mpf, hash_list[block_index], block_index, return_hex=False) == mrt

        mpf = b''.join(mpf)
    return {"data": {"blockHeight": block_height,
                     "blockIndex": block_index,
                     "merkleProof": base64.b64encode(mpf).decode()},
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

    return {"data": {"balance": {"confirmed": int(camount),
                                 "unconfirmed": int(uamount)}},
            "time": round(time.time() - q, 4)}

async def address_state_extended(address, app):
    q = time.time()
    block_height = -1
    received_count = 0
    sent_count = 0
    received_amount = 0
    sent_amount = 0
    balance = 0
    frp = None
    fsp = None
    ltp = None
    type = SCRIPT_N_TYPES[address[0]]

    empty_result = {"balance": 0,
                     "receivedAmount": 0,
                     "receivedTxCount": 0,
                     "sentAmount": 0,
                     "sentTxCount": 0,
                     "firstReceivedTxPointer": None,
                     "firstSentTxPointer": None,
                     "lastTxPointer": None,
                     "outputsReceivedCount": 0,
                     "outputsSpentCount": 0,
                     "type": type}


    async with app["db_pool"].acquire() as conn:
        if address[0] == 2:

            script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                         "WHERE address = $1 LIMIT 1;", address[1:])
            if script is None:
                script = await conn.fetchval("SELECT script from connector_unconfirmed_p2pk_map "
                                             "WHERE address = $1 LIMIT 1;", address[1:])
            if script is None:
                return {"data": empty_result,
                        "time": round(time.time() - q, 4)}

            address = b"\x02" + script
        rows = await conn.fetch("SELECT pointer, amount FROM "
                                "transaction_map WHERE address = $1 and pointer > $2 "
                                "ORDER BY pointer ASC;",
                                address, block_height)
        if rows is None:
            return {"data": empty_result,
                    "time": round(time.time() - q, 4)}


    sent_tx_set = set()
    received_tx_set = set()
    for row in rows:
        if row["pointer"] & (1 << 19):
            received_count += 1
            received_amount += row["amount"]
            balance += row["amount"]
            if not frp:
                frp = row["pointer"]
            received_tx_set.add(row["pointer"] >> 20)
        else:
            sent_tx_set.add(row["pointer"] >> 20)
            sent_count += 1
            sent_amount += row["amount"]
            balance -= row["amount"]
            if not fsp:
                fsp = row["pointer"]
        ltp = row["pointer"]
    frp = "%s:%s" %  (frp >> 39, (frp >> 20) & 524287)
    if ltp:
        ltp = "%s:%s" %  (ltp >> 39, (ltp >> 20) & 524287)
    if fsp is not None:
        fsp = "%s:%s" %  (fsp >> 39, (fsp >> 20) & 524287)

    return {"data": {"balance": balance,
                     "receivedAmount": received_amount,
                     "receivedTxCount": len(received_tx_set),
                     "sentAmount": sent_amount,
                     "sentTxCount": len(sent_tx_set),
                     "firstReceivedTxPointer": frp,
                     "firstSentTxPointer": fsp,
                     "lastTxPointer": ltp,
                     "outputsReceivedCount": received_count,
                     "outputsSpentCount": sent_count,
                     "type": type
                     },
            "time": round(time.time() - q, 4)}


async def address_list_state(addresses, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        u_rows = await conn.fetch("SELECT address, amount "
                                      "FROM connector_unconfirmed_utxo "
                                      "WHERE address = ANY($1);", addresses.keys())
        c_rows = await conn.fetch("SELECT  address, amount , outpoint "
                                      "FROM connector_utxo "
                                      "WHERE address = ANY($1);", addresses.keys())

    r = dict()
    utxo = dict()

    for row in u_rows:
        try:
            r[addresses[row["address"]]]["unconfirmed"] += row["amount"]
        except:
            r[addresses[row["address"]]] = {"unconfirmed": row["amount"],
                                            "confirmed": 0}

    for row in c_rows:
        try:
            r[addresses[row["address"]]]["confirmed"] += row["amount"]
        except:
            r[addresses[row["address"]]] = {"confirmed": row["amount"],
                                            "uconfirmed": 0}
        utxo[row["outpoint"]] = (addresses[row["address"]], row["amount"])

    async with app["db_pool"].acquire() as conn:
        s_rows = await conn.fetch("SELECT  outpoint "
                                      "FROM connector_unconfirmed_stxo "
                                      "WHERE outpoint = ANY($1);", utxo.keys())
    for row in s_rows:
        r[utxo[row["outpoint"]][0]]["unconfirmed"] -= utxo[row["outpoint"]][1]

    for a in addresses:
        if addresses[a] not in r:
            r[addresses[a]] = {"confirmed": 0, "uconfirmed": 0}


    return {"data": r,
            "time": round(time.time() - q, 4)}



async def address_confirmed_utxo(address,  type, from_block, order, order_by, limit, page, app):
    q = time.time()
    utxo = []
    if from_block:
        from_block = " AND pointer >= " + (from_block << 39)
    else:
        from_block = ""

    if address[0] == 0 and type is None:
        a = [address]
        async with app["db_pool"].acquire() as conn:
            script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                         "WHERE address = $1 LIMIT 1;", address[1:])
            if script is not None:
                a.append(b"\x02" + script)
            print(a, from_block, limit, page)
            rows = await conn.fetch("SELECT  outpoint, amount, pointer, address  "
                                          "FROM connector_utxo "
                                          "WHERE address = ANY($1) %s "
                                    "order by  %s %s LIMIT $2 OFFSET $3;" % (from_block, order_by, order),
                                    a, limit, limit * (page - 1))
            for row in rows:
                utxo.append({"txId": rh2s(row["outpoint"][:32]),
                             "vOut": bytes_to_int(row["outpoint"][32:]),
                             "block": row["pointer"] >> 39,
                             "txIndex": (row["pointer"] - ((row["pointer"] >> 39) << 39)) >> 20,
                             "amount": row["amount"],
                             "type": SCRIPT_N_TYPES[address[0]]})

    else:
        async with app["db_pool"].acquire() as conn:
            if address[0] == 0:
                if type == 2:
                    script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                                 "WHERE address = $1 LIMIT 1;", address[1:])
                    if script is not None:
                        address = b"\x02" + script
                    else:
                        return {"data": utxo, "time": round(time.time() - q, 4)}

            rows = await conn.fetch("SELECT  outpoint, amount, pointer  "
                                          "FROM connector_utxo "
                                          "WHERE address = $1 %s "
                                    "order by  %s %s LIMIT $2 OFFSET $3;" % (from_block, order_by, order),
                                    address, limit, limit * (page - 1))

        for row in rows:
            utxo.append({"txId": rh2s(row["outpoint"][:32]),
                         "vOut": bytes_to_int(row["outpoint"][32:]),
                         "block": row["pointer"] >> 39,
                         "txIndex": (row["pointer"] - ((row["pointer"] >> 39) << 39)) >> 20,
                         "amount": row["amount"]})

    return {"data": utxo,
            "time": round(time.time() - q, 4)}


async def address_unconfirmed_utxo(address, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT  outpoint, amount  "
                                      "FROM connector_unconfirmed_utxo "
                                      "WHERE address = $1 order by pointer;", address)
    utxo = []
    for row in rows:
        utxo.append({"txId": rh2s(row["outpoint"][:32]),
                     "vOut": bytes_to_int(row["outpoint"][32:]),
                     "amount": row["amount"]})

    return {"data": utxo,
            "time": round(time.time() - q, 4)}




def get_adjusted_block_time(h, request):
    return request.app["block_map_time"][h]

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


async def close_db_pool(app):
    await app["db_pool"].close()