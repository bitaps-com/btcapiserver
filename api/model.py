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
            rows = await conn.fetch("SELECT pointer,"
                                    "       s_pointer,"
                                    "       address, "
                                    "       amount  "
                                    "FROM stxo "
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
    mempool_rank = None
    invalid_tx = False
    invalidation_timestamp = None

    last_dep = dict()
    conflict_outpoints = deque()

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
                    row = await conn.fetchrow("SELECT tx_id, raw_transaction,  timestamp, invalidation_timestamp "
                                              "FROM invalid_transaction  "
                                              "WHERE tx_id = $1 LIMIT 1;", pointer)
                    if row is None:
                        raise APIException(NOT_FOUND, "transaction not found", status=404)

                    invalid_tx = True
                    invalidation_timestamp = row["invalidation_timestamp"]

                if not invalid_tx:
                    block_height = row["pointer"] >> 39
                    block_index = (row["pointer"] >> 20) & 524287
                    block_row = await conn.fetchrow("SELECT hash, header, adjusted_timestamp "
                                                    "FROM blocks  "
                                                    "WHERE height = $1 LIMIT 1;", block_height)
                    adjusted_timestamp = block_row["adjusted_timestamp"]
                    block_time = unpack("<L", block_row["header"][68: 68 + 4])[0]
                    block_hash = block_row["hash"]
            else:

                mempool_rank = await conn.fetchval("""SELECT ranks.rank FROM 
                                                      (SELECT tx_id, rank() OVER(ORDER BY feerate DESC) as rank 
                                                      FROM unconfirmed_transaction) ranks 
                                                      WHERE tx_id = $1 LIMIT 1""", pointer)


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

                rows = await conn.fetch("SELECT pointer,"
                                        "       s_pointer,"
                                        "       address, "
                                        "       amount  "
                                        "FROM   stxo "
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
                if invalid_tx:
                    rows = await conn.fetch("SELECT outpoint,"
                                            "       input_index,"
                                            "       out_tx_id, "
                                            "       tx_id,"
                                            "       invalid_stxo.address, "
                                            "       invalid_stxo.amount "
                                            "    "
                                            "FROM invalid_stxo "
                                            "WHERE tx_id =  $1;", s2rh(tx["txId"]))
                else:
                    rows = await conn.fetch("SELECT outpoint,"
                                            "       input_index,"
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
                    if invalid_tx:
                        op = b"%s%s" % (s2rh(tx["vIn"][i]["txId"]), int_to_bytes(tx["vIn"][i]["vOut"]))
                        conflict_outpoints.append(op)
                        last_dep[op[:32]] = (tx["vIn"][i]["txId"], tx["vIn"][i]["vOut"])

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
            if invalid_tx:
                rows = await conn.fetch("SELECT   outpoint,"
                                        "         input_index,"
                                          "       tx_id "
                                          "FROM invalid_stxo "
                                          "WHERE out_tx_id = $1;", s2rh(tx["txId"]))
            else:
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

    if mempool_rank is not None:
        tx["mempoolRank"] = mempool_rank
    tx["valid"] = not invalid_tx
    tx["feeRate"] = round(tx["fee"] / tx["vSize"])

    if invalid_tx:
        m_conflict = []
        i_conflict_chain = []
        dep_chain = [[tx["txId"]]]
        conflict_outpoints_chain = [conflict_outpoints]

        # find out mainnet competitors and invalid tx chains
        while last_dep:
            async with app["db_pool"].acquire() as conn:
                rows = await conn.fetch("SELECT distinct tx_id, pointer FROM transaction where tx_id = ANY($1)", last_dep.keys())
            if len(rows) >= len(last_dep):
                break

            for row in rows:
                try:
                    del last_dep[row["tx_id"]]
                except:
                    pass
            dep_chain.append([rh2s(k) for k in last_dep.keys()])

            # go deeper
            async with app["db_pool"].acquire() as conn:
                rows = await conn.fetch("SELECT invalid_stxo.out_tx_id,"
                                        "        outpoint "
                                         "from invalid_stxo "
                                         "WHERE tx_id = ANY($1)", last_dep.keys())
            last_dep = dict()
            conflict_outpoints = deque()
            for row in rows:
                last_dep[row["out_tx_id"]] = (rh2s(row["outpoint"][:32]), bytes_to_int(row["outpoint"][32:]))
                conflict_outpoints.append(row["outpoint"])
            conflict_outpoints_chain.append(conflict_outpoints)

        pointer = rows[0]["pointer"]
        o_pointers = set()
        pointer_map = dict()
        for t in last_dep:
            pointer_map[pointer +(1<<19) +last_dep[t][1]] = last_dep[t]
            o_pointers.add(pointer +(1<<19) + last_dep[t][1])

        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT transaction.tx_id, stxo.s_pointer, stxo.pointer  "
                                    "FROM stxo "
                                    "JOIN transaction on transaction.pointer = (stxo.s_pointer >> 18)<<18 "
                                    "WHERE stxo.pointer = ANY($1);", o_pointers)

        for row in rows:
            m_conflict.append({
                               "doublespend": {"txId": pointer_map[row["pointer"]][0],
                                               "vOut": pointer_map[row["pointer"]][1],
                                               "block": row["pointer"] >> 39},
                               "competitor": {"txId": rh2s(row["tx_id"]),
                                              "vIn":row["s_pointer"] &  524287,
                                              "block": row["s_pointer"] >> 39}
                               })
        chain = dep_chain


        atx = set()
        atx.add(s2rh(tx["txId"]))
        # check invalid transactions competitors
        for conflict_outpoints in conflict_outpoints_chain:
            [atx.add(t[:32]) for t in conflict_outpoints]
            async with app["db_pool"].acquire() as conn:
                irows = await conn.fetch("SELECT invalid_stxo.tx_id,"
                                         "       invalid_stxo.out_tx_id, "
                                         "       invalid_stxo.input_index, "
                                         "       invalid_stxo.outpoint, "
                                         "       invalid_transaction.size, "
                                         "       invalid_transaction.b_size, "
                                         "       invalid_transaction.feerate, "
                                         "       invalid_transaction.fee,"
                                         "       invalid_transaction.rbf, "
                                         "       invalid_transaction.segwit "
                                         "from invalid_stxo "
                                         "JOIN invalid_transaction "
                                         "ON invalid_transaction.tx_id = invalid_stxo.tx_id "
                                         "WHERE outpoint = ANY($1)", conflict_outpoints)

            i_conflict = []
            for row in irows:
                h = rh2s(row["tx_id"])
                if row["tx_id"] not in atx:
                    i_conflict.append({"doublespend": {"txId": rh2s(row["outpoint"][:32]),
                                                       "vOut": bytes_to_int(row["outpoint"][32:])},
                                       "competitor": {"txId": h,
                                                      "vIn":row["input_index"],
                                                      "size": row["size"],
                                                      "bSize": row["b_size"],
                                                      "feeRate": round(row["feerate"], 2),
                                                      "fee": row["fee"],
                                                      "rbf": bool(row["rbf"]),
                                                      "segwit": bool(row["segwit"])
                                                      }})
            i_conflict_chain.append(i_conflict)






        del tx["blockHash"]
        del tx["confirmations"]
        del tx["blockTime"]
        del tx["blockIndex"]
        del tx["blockHeight"]
        del tx["adjustedTimestamp"]
        if "flag" in tx:
            del tx["flag"]

        tx["conflict"] = {"blockchain": m_conflict, "invalidTxCompetitors": i_conflict_chain, "invalidTxChain": chain,
                          "invalidationTimestamp": invalidation_timestamp}

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
                rows = await conn.fetch("SELECT pointer,"
                                        "       s_pointer,"
                                        "       address, "
                                        "       amount  "
                                        "FROM stxo "
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
                                        "       address, "
                                        "       amount "
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


async def mempool_transactions(limit, page, order, from_timestamp, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        count = await conn.fetchval("SELECT count(tx_id) FROM unconfirmed_transaction "
                                    "WHERE timestamp > $1;", from_timestamp)
        pages = math.ceil(count / limit)

        rows = await conn.fetch("SELECT   "
                                "        raw_transaction,"
                                "        tx_id,  "
                                "        timestamp  "
                                "FROM unconfirmed_transaction "
                                "WHERE timestamp > $1 ORDER BY timestamp %s LIMIT $2 OFFSET $3;" % order,
                                from_timestamp, limit,  limit * (page - 1))
    tx_list = []
    for row in rows:
        tx = Transaction(row["raw_transaction"], testnet=app["testnet"])
        tx_list.append({"txId": tx["txId"], "data": tx["data"], "amount": tx["amount"], "timestamp": row["timestamp"]})


    return {"data": {"page": page,
                     "limit": limit,
                     "pages": pages,
                     "fromTimestamp":from_timestamp,
                     "list": tx_list},
            "time": round(time.time() - q, 4)}

async def invalid_transactions(limit, page, order, from_timestamp, app):
    qq = time.time()
    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT   "
                                "        raw_transaction,"
                                "        tx_id,  "
                                "        timestamp,"
                                "        invalidation_timestamp,"
                                "        amount,"
                                "        segwit,"
                                "        rbf,"
                                "        fee,"
                                "         feerate "
                                "FROM invalid_transaction "
                                "WHERE invalidation_timestamp > $1 "
                                "ORDER BY invalidation_timestamp %s LIMIT $2 OFFSET $3;" % order,
                                from_timestamp, limit,  limit * (page - 1))
    tx_list = []
    tx_set = set()
    last_dep = dict()
    list_map = dict()
    for row in rows:
        tx = Transaction(row["raw_transaction"], testnet=app["testnet"])
        tx["conflict"] = {"blockchain": [], "invalid": [],
                          "invalidationTimestamp": row["invalidation_timestamp"]}
        tx["fee"] = row["fee"]
        tx["feeRate"] = round(row["feerate"], 2)
        tx["rbf"] = bool(row["feerate"])
        tx["segwit"] = bool(row["feerate"])
        if "flag" in tx:
            del tx["flag"]
        tx_list.append(tx)

        list_map[row["tx_id"]] = set()
        for q in tx["vIn"]:
            tx_set.add(s2rh(tx["vIn"][q]["txId"]))
            last_dep[s2rh(tx["vIn"][q]["txId"])] =  tx["vIn"][q]["vOut"]
            list_map[row["tx_id"]].add(tx["vIn"][q]["txId"])


    # find out mainnet competitiors and invalid chains

    while last_dep:
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT distinct tx_id, pointer FROM transaction where tx_id = ANY($1)",
                                    last_dep.keys())
        if len(rows) >= len(last_dep):
            break

        for row in rows:
            try:
                del last_dep[row["tx_id"]]
            except:
                pass
        dep_chain.append([rh2s(k) for k in last_dep.keys()])

        # go deeper
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT invalid_stxo.out_tx_id,"
                                    "        outpoint "
                                    "from invalid_stxo "
                                    "WHERE tx_id = ANY($1)", last_dep.keys())
        last_dep = dict()
        conflict_outpoints = deque()
        for row in rows:
            last_dep[row["out_tx_id"]] = (rh2s(row["outpoint"][:32]), bytes_to_int(row["outpoint"][32:]))
            conflict_outpoints.append(row["outpoint"])
        conflict_outpoints_chain.append(conflict_outpoints)

    async with app["db_pool"].acquire() as conn:
        c_rows = await conn.fetch("""
                                   SELECT pointer, tx_id FROM transaction
                                   WHERE tx_id = ANY($1);
                                   """, tx_set)
    tx_id_map_pointer = dict()
    for r in c_rows:
        tx_id_map_pointer[rh2s(r["tx_id"])] = r["pointer"]


    out_pointers = dict()
    i_pointers = dict()

    for i in range(len(tx_list)):
        for q in tx_list[i]["vIn"]:
            try:
                pointer = tx_id_map_pointer[tx_list[i]["vIn"][q]["txId"]]
                out_pointers[pointer + (1<<19) +tx_list[i]["vIn"][q]["vOut"]] = (i, q)

            except Exception as err:

                print(err)
                i_pointers[b"%s%s" % (s2rh(tx_list[i]["vIn"][q]["txId"]),
                                      int_to_bytes(tx_list[i]["vIn"][q]["vOut"]))] = (i, s2rh(tx_list[i]["txId"]))
        del tx_list[i]["vIn"]
        del tx_list[i]["vOut"]
        del tx_list[i]["format"]
        del tx_list[i]["testnet"]
        del tx_list[i]["blockTime"]
        del tx_list[i]["rawTx"]
        del tx_list[i]["blockHash"]
        del tx_list[i]["confirmations"]
        del tx_list[i]["blockIndex"]
        tx_list[i]["valid"] = False

    async with app["db_pool"].acquire() as conn:
        if out_pointers:
            rows = await conn.fetch("SELECT transaction.tx_id, stxo.s_pointer, stxo.pointer  "
                                    "FROM stxo "
                                    "JOIN transaction on transaction.pointer = (stxo.s_pointer >> 18)<<18 "
                                    "WHERE stxo.pointer = ANY($1);", out_pointers.keys())
        else:
            rows = []

        if i_pointers:
            irows = await conn.fetch("SELECT tx_id, input_index, outpoint from invalid_stxo "
                                     "WHERE outpoint = ANY($1)", i_pointers.keys())
        else:
            irows = []


    for row in rows:
        i, t = out_pointers[row["pointer"]]
        tx_list[i]["conflict"]["blockchain"].append({"txId":rh2s(row["tx_id"]),
                                                     "vIn": row["s_pointer"] & 524287,
                                                     "block": row["s_pointer"] >> 39})
    for row in irows:
        i, t = i_pointers[row["outpoint"]]
        if t == row["tx_id"]:
            continue
        tx_list[i]["conflict"]["invalid"].append({"txId":rh2s(row["tx_id"]),
                                                     "vIn": row["input_index"]})






    return {"data": {"page": page,
                     "limit": limit,
                     "fromTimestamp":from_timestamp,
                     "list": tx_list},
            "time": round(time.time() - qq, 4)}


async def mempool_doublespend(limit, page, order, from_timestamp, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        count = await conn.fetchval("SELECT count(tx_id) FROM mempool_dbs "
                                    "WHERE timestamp > $1;", from_timestamp)

        pages = math.ceil(count / limit)

        rows = await conn.fetch("SELECT   "
                                "        mempool_dbs.tx_id,"
                                "        unconfirmed_transaction.amount,  "
                                "        unconfirmed_transaction.size,  "
                                "        unconfirmed_transaction.b_size,  "
                                "        unconfirmed_transaction.fee,  "
                                "        unconfirmed_transaction.rbf,  "
                                "        unconfirmed_transaction.segwit,  "
                                "        mempool_dbs.timestamp  "
                                "FROM mempool_dbs "
                                "JOIN unconfirmed_transaction "
                                "ON unconfirmed_transaction.tx_id = mempool_dbs.tx_id "
                                "WHERE mempool_dbs.timestamp > $1 "
                                "ORDER BY mempool_dbs.timestamp %s LIMIT $2 OFFSET $3;" % order,
                                from_timestamp, limit,  limit * (page - 1))
    tx_list = []
    for row in rows:
        tx_list.append({"txId": rh2s(row["tx_id"]),
                        "amount": row["amount"],
                        "fee": row["fee"],
                        "size": row["size"],
                        "bSize": row["b_size"],
                        "rbf": bool(row["rbf"]),
                        "segwit": bool(row["b_size"]),
                        "timestamp": row["timestamp"]})


    return {"data": {"page": page,
                     "limit": limit,
                     "pages": pages,
                     "count": count,
                     "fromTimestamp":from_timestamp,
                     "list": tx_list},
            "time": round(time.time() - q, 4)}

async def mempool_doublespend_childs(limit, page, order, from_timestamp, app):
    q = time.time()
    async with app["db_pool"].acquire() as conn:
        count = await conn.fetchval("SELECT count(tx_id) FROM mempool_dbs_childs "
                                    "WHERE timestamp > $1;", from_timestamp)
        pages = math.ceil(count / limit)

        rows = await conn.fetch("SELECT   "
                                "        mempool_dbs_childs.tx_id,"
                                "        unconfirmed_transaction.amount,  "
                                "        unconfirmed_transaction.size,  "
                                "        unconfirmed_transaction.b_size,  "
                                "        unconfirmed_transaction.fee,  "
                                "        unconfirmed_transaction.rbf,  "
                                "        unconfirmed_transaction.segwit,  "
                                "        mempool_dbs_childs.timestamp  "
                                "FROM mempool_dbs_childs "
                                "JOIN unconfirmed_transaction "
                                "ON unconfirmed_transaction.tx_id = mempool_dbs_childs.tx_id "
                                "WHERE mempool_dbs_childs.timestamp > $1 "
                                "ORDER BY mempool_dbs_childs.timestamp %s LIMIT $2 OFFSET $3;" % order,
                                from_timestamp, limit,  limit * (page - 1))
    tx_list = []
    for row in rows:
        tx_list.append({"txId": rh2s(row["tx_id"]),
                        "amount": row["amount"],
                        "fee": row["fee"],
                        "size": row["size"],
                        "bSize": row["b_size"],
                        "rbf": bool(row["rbf"]),
                        "segwit": bool(row["b_size"]),
                        "timestamp": row["timestamp"]})


    return {"data": {"page": page,
                     "limit": limit,
                     "pages": pages,
                     "fromTimestamp":from_timestamp,
                     "list": tx_list},
            "time": round(time.time() - q, 4)}


async def mempool_state(app):
    q = time.time()


    async with app["db_pool"].acquire() as conn:
        row = await conn.fetchrow("SELECT inputs, outputs, transactions from mempool_analytica  "
                               " order by minute desc limit 1")
        inputs = json.loads(row["inputs"])
        outputs = json.loads(row["outputs"])
        transactions = json.loads(row["transactions"])

    return {"data": {"inputs": inputs,
                     "outputs": outputs,
                     "transactions": transactions},
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



async def address_state(address,  type, app):
    q = time.time()

    if address[0] == 0 and type is None:
        a = [address]
        async with app["db_pool"].acquire() as conn:
            script = await conn.fetchval("SELECT script from connector_unconfirmed_p2pk_map "
                                         "WHERE address = $1 LIMIT 1;", address[1:])
            if script is None:
                script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                             "WHERE address = $1 LIMIT 1;", address[1:])
            if script is not None:
                a.append(b"\x02" + script)
            uamount = await conn.fetchval("SELECT  sum(amount) FROM connector_unconfirmed_utxo "
                                          "WHERE address = ANY($1);", a)
            camount = await conn.fetchval("SELECT  sum(amount) FROM connector_utxo "
                                          "WHERE address = ANY($1);", a)
    else:
        async with app["db_pool"].acquire() as conn:
            if address[0] == 0:
                if type == 2:
                    script = await conn.fetchval("SELECT script from connector_unconfirmed_p2pk_map "
                                                 "WHERE address = $1 LIMIT 1;", address[1:])
                    if script is None:
                        script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                                     "WHERE address = $1 LIMIT 1;", address[1:])
                    if script is not None:
                        address = b"\x02" + script
                    else:
                        return {"data":{"confirmed": 0,
                                        "unconfirmed": 0},
                                "time": round(time.time() - q, 4)}

            uamount = await conn.fetchval("SELECT  sum(amount) FROM connector_unconfirmed_utxo "
                                          "WHERE address = $1;", address)
            camount = await conn.fetchval("SELECT  sum(amount) FROM connector_utxo "
                                          "WHERE address = $1;", address)

    if uamount is None: uamount = 0
    if camount is None: camount = 0
    return {"data": {"confirmed": int(camount),
                     "unconfirmed": int(uamount)},
            "time": round(time.time() - q, 4)}


async def address_transactions(address,  type, limit, page, order, mode, from_block, app):
    q = time.time()
    pages = 0

    if address[0] == 0 and type is None:
        a = [address]
        async with app["db_pool"].acquire() as conn:
            script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                         "WHERE address = $1 LIMIT 1;", address[1:])
            if script is not None:
                a.append(b"\x02" + script)

    else:
        async with app["db_pool"].acquire() as conn:
            if address[0] == 0:
                if type == 2:
                    script = await conn.fetchval("SELECT script from connector_unconfirmed_p2pk_map "
                                                 "WHERE address = $1 LIMIT 1;", address[1:])
                    if script is None:
                        script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                                     "WHERE address = $1 LIMIT 1;", address[1:])
                    if script is not None:
                        address = b"\x02" + script
                    else:
                        return {"data":{"confirmed": 0,
                                        "unconfirmed": 0},
                                "time": round(time.time() - q, 4)}
            a = [address]

    if from_block:
        from_block_str = " and transaction_map.pointer >= %s" % (from_block << 39)
    else:
        from_block_str = ""

    async with app["db_pool"].acquire() as conn:
        count = await conn.fetchval("SELECT count(pointer) FROM transaction_map "
                                    "WHERE address = ANY($1) %s;" % from_block_str, a)
        pages = math.ceil(count / limit)

        rows = await conn.fetch("SELECT  transaction.pointer,"
                                "        transaction.raw_transaction,"
                                "        transaction.tx_id,  "
                                "        transaction.timestamp  "
                                "FROM transaction_map "
                                "JOIN transaction on transaction.pointer = transaction_map.pointer "
                                "WHERE address = ANY($1) %s  "
                                "order by  transaction_map.pointer %s "
                                "LIMIT $2 OFFSET $3;" % (from_block_str, order) ,
                                a, limit,  limit * (page - 1))

    target_scripts = []



    tx_list = []
    s_pointers = []
    tx_id_set = set()
    o_pointers = []

    for row in rows:
        tx_id_set.add(row["tx_id"])
        tx = Transaction(row["raw_transaction"], testnet=app["testnet"])
        tx["blockHeight"] = row["pointer"] >> 39
        tx["blockIndex"] = (row["pointer"] >> 20) & 524287
        tx["timestamp"] = row["timestamp"]
        tx["confirmations"] = app["last_block"] - tx["blockHeight"] + 1
        tx["rbf"] = False
        try:
            tx["blockTime"] = app["block_map_time"][tx["blockHeight"]][1]
        except:
            pass
        tx_pointer = (tx["blockHeight"] << 39) + (tx["blockIndex"] << 20)

        for i in tx["vIn"]:
            s_pointers.append(row["pointer"] + i)
        for i in tx["vOut"]:
            o_pointers.append(tx_pointer + (1 << 19) + i)

        tx_list.append(tx)
        ts = dict()
        for d in a:
            if d[0] in (0, 1, 5, 6):
                ts[hash_to_script(d[1:], d[0], hex=True)] = 0
            else:
                ts[d[1:].hex()] = 0
        target_scripts.append(ts)


    async with app["db_pool"].acquire() as conn:
        stxo = await conn.fetch("SELECT s_pointer, pointer, amount, address FROM stxo "
                                "WHERE stxo.s_pointer = ANY($1);", s_pointers)
    stxo_map = {}


    for row in stxo:
        stxo_map[row["s_pointer"]] = (row["address"], row["amount"], row["pointer"])


    for i in range(len(tx_list)):
        tx_list[i]["inputsAmount"] = 0
        tx_list[i]["inputAddressCount"] = 0
        tx_list[i]["outAddressCount"] = 0
        tx_list[i]["inputsCount"] = len(tx_list[i]["vIn"])
        tx_list[i]["outsCount"] = len(tx_list[i]["vOut"])
        tx_pointer = (tx_list[i]["blockHeight"] << 39) + (tx_list[i]["blockIndex"] << 20)
        if not tx_list[i]["coinbase"]:
            for k in tx_list[i]["vIn"]:
                d = stxo_map[tx_pointer + k]
                tx_list[i]["vIn"][k]["type"] = SCRIPT_N_TYPES[d[0][0]]
                tx_list[i]["vIn"][k]["amount"] = d[1]
                tx_list[i]["inputsAmount"] += d[1]
                pointer = d[2]
                tx_list[i]["vIn"][k]["blockHeight"] = pointer >> 39
                tx_list[i]["vIn"][k]["confirmations"] = app["last_block"] - (pointer >> 39) + 1

                if d[0][0] in (0, 1, 2, 5, 6):
                    script_hash = True if d[0][0] in (1, 6) else False
                    witness_version = None if d[0][0] < 5 else 0
                    try:
                        if d[0][0] == 2:
                            ad = b"\x02" + parse_script(d[0][1:])["addressHash"]
                        else:
                            ad = d[0]
                        tx_list[i]["vIn"][k]["address"] = hash_to_address(ad[1:],
                                                                    testnet=app["testnet"],
                                                                    script_hash=script_hash,
                                                                    witness_version=witness_version)

                        tx_list[i]["vIn"][k]["scriptPubKey"] = address_to_script(tx_list[i]["vIn"][k]["address"], hex=1)
                    except:
                        print(tx_list[i]["txId"])
                        print("??", d[0].hex())
                        raise
                    tx_list[i]["inputAddressCount"] += 1
                else:
                    tx_list[i]["vIn"][k]["scriptPubKey"] = d[0][1:].hex()

                tx_list[i]["vIn"][k]["scriptPubKeyOpcodes"] = decode_script(tx_list[i]["vIn"][k]["scriptPubKey"])
                tx_list[i]["vIn"][k]["scriptPubKeyAsm"] = decode_script(tx_list[i]["vIn"][k]["scriptPubKey"], 1)
                for ti in target_scripts[i]:
                    if ti == tx_list[i]["vIn"][k]["scriptPubKey"]:
                        target_scripts[i][ti] -= tx_list[i]["vIn"][k]["amount"]

                if tx_list[i]["vIn"][k]["sequence"] < 0xfffffffe:
                    tx_list[i]["vIn"][k]["rbf"] = True


        if not tx_list[i]["coinbase"]:
            tx_list[i]["fee"] = tx_list[i]["inputsAmount"] - tx_list[i]["amount"]
        else:
            tx_list[i]["fee"] = 0

        tx_list[i]["outputsAmount"] = tx_list[i]["amount"]

    # get information about spent output coins
    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT   outpoint,"
                                "         input_index,"
                                "       tx_id "
                                "FROM connector_unconfirmed_stxo "
                                "WHERE out_tx_id = ANY($1);", tx_id_set)
    out_map = dict()
    for v in rows:
        i = bytes_to_int(v["outpoint"][32:])
        try:
            out_map[(rh2s(v["outpoint"][:32]), i)].append({"txId": rh2s(v["tx_id"]), "vIn": v["input_index"]})
        except:
            out_map[(rh2s(v["outpoint"][:32]), i)] = [{"txId": rh2s(v["tx_id"]), "vIn": v["input_index"]}]

    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT stxo.pointer,"
                                "       stxo.s_pointer,"
                                "       transaction.tx_id,  "
                                "       transaction.timestamp  "
                                "FROM stxo "
                                "JOIN transaction "
                                "ON transaction.pointer = (stxo.s_pointer >> 18)<<18 "
                                "WHERE stxo.pointer = ANY($1);", o_pointers)
        p_out_map = dict()
        for v in rows:
            p_out_map[v["pointer"]] = [{"txId": rh2s(v["tx_id"]),
                                           "vIn": v["s_pointer"] & 0b111111111111111111}]

    for t in range(len(tx_list)):
        if tx_list[t]["blockHeight"] is not None:
            o_pointer = (tx_list[t]["blockHeight"] << 39) + (tx_list[t]["blockIndex"] << 20) + (1 << 19)
            for i in tx_list[t]["vOut"]:
                try:
                    tx_list[t]["vOut"][i]["spent"] = p_out_map[o_pointer + i]
                except:
                    try:
                        tx_list[t]["vOut"][i]["spent"] = out_map[(tx_list[t]["txId"], int(i))]
                    except:
                        tx_list[t]["vOut"][i]["spent"] = []
                for ti in target_scripts[t]:
                    if ti == tx_list[t]["vOut"][i]["scriptPubKey"]:
                        target_scripts[t][ti] += tx_list[t]["vOut"][i]["value"]
                if "address" in  tx_list[t]["vOut"][i]:
                    tx_list[t]["outAddressCount"] += 1

            address_amount = 0
            for ti in target_scripts[t]:
                address_amount += target_scripts[t][ti]


            tx_list[t]["amount"] = address_amount


        if mode != "verbose":
            del tx_list[t]["vIn"]
            del tx_list[t]["vOut"]
        del tx_list[t]["format"]
        del tx_list[t]["testnet"]
        del tx_list[t]["rawTx"]
        del tx_list[t]["hash"]
        del tx_list[t]["blockHash"]
        del tx_list[t]["time"]

        try:
            del tx_list[t]["flag"]
        except:
            pass

    return {"data": {"page": page,
                     "limit": limit,
                     "pages": pages,
                     "fromBlock":from_block,
                     "list": tx_list},
            "time": round(time.time() - q, 4)}

async def address_unconfirmed_transactions(address,  type, limit, page, order, mode, app):
    q = time.time()

    if address[0] == 0 and type is None:
        a = [address]
        async with app["db_pool"].acquire() as conn:
            script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                         "WHERE address = $1 LIMIT 1;", address[1:])
            if script is None:
                script = await conn.fetchval("SELECT script from connector_unconfirmed_p2pk_map "
                                             "WHERE address = $1 LIMIT 1;", address[1:])
            if script is not None:
                a.append(b"\x02" + script)

    else:
        async with app["db_pool"].acquire() as conn:
            if address[0] == 0:
                if type == 2:
                    script = await conn.fetchval("SELECT script from connector_unconfirmed_p2pk_map "
                                                 "WHERE address = $1 LIMIT 1;", address[1:])
                    if script is None:
                        script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                                     "WHERE address = $1 LIMIT 1;", address[1:])
                    if script is not None:
                        address = b"\x02" + script
                    else:
                        return {"data":{"confirmed": 0,
                                        "unconfirmed": 0},
                                "time": round(time.time() - q, 4)}
            a = [address]


    async with app["db_pool"].acquire() as conn:
        count = await conn.fetchval("SELECT count(tx_id) FROM unconfirmed_transaction_map "
                                    "WHERE address = ANY($1);", a)
        pages = math.ceil(count / limit)

        rows = await conn.fetch("SELECT  "
                                "        unconfirmed_transaction.raw_transaction,"
                                "        unconfirmed_transaction.tx_id,  "
                                "        unconfirmed_transaction.timestamp  "
                                "FROM unconfirmed_transaction_map "
                                "JOIN unconfirmed_transaction "
                                "on unconfirmed_transaction.tx_id = unconfirmed_transaction_map.tx_id "
                                "WHERE unconfirmed_transaction_map.address = ANY($1)    "
                                "order by  unconfirmed_transaction.timestamp %s "
                                "LIMIT $2 OFFSET $3;" %   order,
                                a, limit,  limit * (page - 1))
        t = set()
        mempool_rank_map = dict()
        for row in rows:
            t.add(row["tx_id"])
        if t:
            ranks = await conn.fetch("""SELECT ranks.rank, ranks.tx_id FROM 
                                                  (SELECT tx_id, rank() OVER(ORDER BY feerate DESC) as rank 
                                                  FROM unconfirmed_transaction) ranks 
                                                  WHERE tx_id = ANY($1) LIMIT $2""", t, len(t))
            for r in ranks:
                mempool_rank_map[r["tx_id"]] = r["rank"]


    target_scripts = []



    tx_list = []
    tx_id_set = set()

    for row in rows:
        tx_id_set.add(row["tx_id"])
        tx = Transaction(row["raw_transaction"], testnet=app["testnet"])
        tx["timestamp"] = row["timestamp"]
        tx["rbf"] = False
        tx_list.append(tx)
        try:
            tx["mempoolRank"] = mempool_rank_map[row["tx_id"]]
        except:
            pass
        ts = dict()
        for d in a:
            if d[0] in (0, 1, 5, 6):
                ts[hash_to_script(d[1:], d[0], hex=True)] = 0
            else:
                ts[d[1:].hex()] = 0
        target_scripts.append(ts)


    async with app["db_pool"].acquire() as conn:
        stxo = await conn.fetch("SELECT connector_unconfirmed_stxo.input_index,"
                                "       connector_unconfirmed_stxo.tx_id,"
                                "       connector_unconfirmed_stxo.amount,"
                                "       connector_unconfirmed_stxo.address,"
                                "       transaction.pointer "
                                "FROM connector_unconfirmed_stxo "
                                "LEFT OUTER JOIN transaction "
                                "ON connector_unconfirmed_stxo.out_tx_id = transaction.tx_id "
                                "WHERE connector_unconfirmed_stxo.tx_id = ANY($1);", tx_id_set)
    stxo_map = {}


    for row in stxo:
        stxo_map[(rh2s(row["tx_id"]), row["input_index"])] = (row["address"], row["amount"], row["pointer"])


    for i in range(len(tx_list)):
        tx_list[i]["inputsAmount"] = 0
        tx_list[i]["inputAddressCount"] = 0
        tx_list[i]["outAddressCount"] = 0
        tx_list[i]["inputsCount"] = len(tx_list[i]["vIn"])
        tx_list[i]["outsCount"] = len(tx_list[i]["vOut"])

        if not tx_list[i]["coinbase"]:
            for k in tx_list[i]["vIn"]:
                d = stxo_map[(tx_list[i]["txId"],  k)]
                tx_list[i]["vIn"][k]["type"] = SCRIPT_N_TYPES[d[0][0]]
                tx_list[i]["vIn"][k]["amount"] = d[1]
                tx_list[i]["inputsAmount"] += d[1]
                pointer = d[2]
                tx_list[i]["vIn"][k]["blockHeight"] = pointer >> 39
                tx_list[i]["vIn"][k]["confirmations"] = app["last_block"] - (pointer >> 39) + 1

                if d[0][0] in (0, 1, 2, 5, 6):
                    script_hash = True if d[0][0] in (1, 6) else False
                    witness_version = None if d[0][0] < 5 else 0
                    try:
                        if d[0][0] == 2:
                            ad = b"\x02" + parse_script(d[0][1:])["addressHash"]
                        else:
                            ad = d[0]
                        tx_list[i]["vIn"][k]["address"] = hash_to_address(ad[1:],
                                                                    testnet=app["testnet"],
                                                                    script_hash=script_hash,
                                                                    witness_version=witness_version)

                        tx_list[i]["vIn"][k]["scriptPubKey"] = address_to_script(tx_list[i]["vIn"][k]["address"], hex=1)
                    except:
                        print(tx_list[i]["txId"])
                        print("??", d[0].hex())
                        raise
                    tx_list[i]["inputAddressCount"] += 1
                else:
                    tx_list[i]["vIn"][k]["scriptPubKey"] = d[0][1:].hex()

                tx_list[i]["vIn"][k]["scriptPubKeyOpcodes"] = decode_script(tx_list[i]["vIn"][k]["scriptPubKey"])
                tx_list[i]["vIn"][k]["scriptPubKeyAsm"] = decode_script(tx_list[i]["vIn"][k]["scriptPubKey"], 1)
                for ti in target_scripts[i]:
                    if ti == tx_list[i]["vIn"][k]["scriptPubKey"]:
                        target_scripts[i][ti] -= tx_list[i]["vIn"][k]["amount"]

                if tx_list[i]["vIn"][k]["sequence"] < 0xfffffffe:
                    tx_list[i]["vIn"][k]["rbf"] = True


        if not tx_list[i]["coinbase"]:
            tx_list[i]["fee"] = tx_list[i]["inputsAmount"] - tx_list[i]["amount"]
        else:
            tx_list[i]["fee"] = 0

        tx_list[i]["outputsAmount"] = tx_list[i]["amount"]

    # get information about spent output coins
    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT   outpoint,"
                                "         input_index,"
                                "       tx_id "
                                "FROM connector_unconfirmed_stxo "
                                "WHERE out_tx_id = ANY($1);", tx_id_set)
    out_map = dict()
    for v in rows:
        i = bytes_to_int(v["outpoint"][32:])
        try:
            out_map[(rh2s(v["outpoint"][:32]), i)].append({"txId": rh2s(v["tx_id"]), "vIn": v["input_index"]})
        except:
            out_map[(rh2s(v["outpoint"][:32]), i)] = [{"txId": rh2s(v["tx_id"]), "vIn": v["input_index"]}]


    # todo get information about double spent coins
    # async with app["db_pool"].acquire() as conn:
    #     rows = await conn.fetch("SELECT   outpoint,"
    #                             "         input_index,"
    #                             "       tx_id "
    #                             "FROM connector_unconfirmed_stxo "
    #                             "WHERE out_tx_id = ANY($1);", tx_id_set)

    for t in range(len(tx_list)):
        for i in tx_list[t]["vOut"]:
            try:
                tx_list[t]["vOut"][i]["spent"] = out_map[(tx_list[t]["txId"], int(i))]
            except:
                tx_list[t]["vOut"][i]["spent"] = []

            for ti in target_scripts[t]:
                if ti == tx_list[t]["vOut"][i]["scriptPubKey"]:
                    target_scripts[t][ti] += tx_list[t]["vOut"][i]["value"]
            if "address" in  tx_list[t]["vOut"][i]:
                tx_list[t]["outAddressCount"] += 1

        address_amount = 0
        for ti in target_scripts[t]:
            address_amount += target_scripts[t][ti]


        tx_list[t]["amount"] = address_amount


        if mode != "verbose":
            del tx_list[t]["vIn"]
            del tx_list[t]["vOut"]
        del tx_list[t]["format"]
        del tx_list[t]["testnet"]
        del tx_list[t]["rawTx"]
        del tx_list[t]["hash"]
        del tx_list[t]["blockHash"]
        del tx_list[t]["time"]
        del tx_list[t]["confirmations"]
        del tx_list[t]["blockIndex"]

        try:
            del tx_list[t]["flag"]
        except:
            pass

    return {"data": {"page": page,
                     "limit": limit,
                     "pages": pages,
                     "list": tx_list},
            "time": round(time.time() - q, 4)}




async def address_state_extended(address, app):
    q = time.time()
    block_height = -1
    received_outs_count = 0
    spent_outs_count = 0
    pending_received_outs_count = 0
    pending_spent_outs_count = 0
    pending_received_amount = 0
    pending_sent_amount = 0
    pending_received_tx_count = 0
    pending_sent_tx_count = 0
    received_amount = 0
    sent_amount = 0
    received_tx_count = 0
    sent_tx_count = 0
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
                    "largestTxAmount": None,
                    "largestTxPointer": None,

                    "largestSpentTxAmount": None,
                    "largestSpentTxPointer": None,
                    "largestReceivedTxAmount": None,
                    "largestReceivedTxPointer": None,

                    "receivedOutsCount": 0,
                    "spentOutsCount": 0,
                    "pendingReceivedAmount": 0,
                    "pendingSentAmount": 0,
                    "pendingReceivedTxCount": 0,
                    "pendingSentTxCount": 0,
                    "pendingReceivedOutsCount": 0,
                    "pendingSpentOutsCount": 0,
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


        stxo = await conn.fetch("SELECT pointer, s_pointer, amount FROM "
                                "stxo WHERE address = $1 and s_pointer > $2 ", address, block_height)

        utxo = await conn.fetch("SELECT pointer, amount FROM "
                                "connector_utxo WHERE address = $1 and pointer > $2 ", address, block_height)

        ustxo = await conn.fetch("SELECT tx_id, amount FROM "
                                "connector_unconfirmed_stxo WHERE address = $1;", address)

        uutxo = await conn.fetch("SELECT out_tx_id as tx_id, amount FROM "
                                "connector_unconfirmed_utxo WHERE address = $1;", address)



        if not stxo and not utxo and not ustxo and not uutxo:
            return {"data": empty_result,
                    "time": round(time.time() - q, 4)}

    tx_map = dict()


    for row in stxo:
        spent_outs_count += 1
        received_outs_count += 1
        received_amount += row["amount"]
        sent_amount += row["amount"]

        if not frp:
            frp = row["pointer"]

        if not fsp:
            fsp = row["s_pointer"]

        try:
            tx_map[row["s_pointer"] >> 20] -= row["s_pointer"]
        except:
            tx_map[row["s_pointer"] >> 20] = 0 - row["s_pointer"]

        ltp = row["pointer"]


    for row in utxo:
        received_outs_count += 1
        received_amount += row["amount"]
        balance += row["amount"]

        if not frp:
            frp = row["pointer"]

        try:
            tx_map[row["pointer"] >> 20] += row["amount"]
        except:
            tx_map[row["pointer"] >> 20] = row["amount"]

        if ltp is None or ltp < row["pointer"]:
            ltp = row["pointer"]



    largest_spent_amount = 0
    largest_received_amount = 0
    largest_spent_pointer = None
    largest_received_pointer = None

    for k in tx_map:
        if tx_map[k] < 0:
            sent_tx_count += 1
            if largest_spent_amount > tx_map[k]:
                largest_spent_amount = tx_map[k]
                largest_spent_pointer = "%s:%s" %  (k >> 19, k  & 524287)
        else:
            received_tx_count += 1
            if largest_received_amount < tx_map[k]:
                largest_received_amount = tx_map[k]
                largest_received_pointer = "%s:%s" %  (k >> 19, k  & 524287)

    if largest_spent_amount is not None:
        largest_spent_amount = abs(largest_spent_amount)

    frp = "%s:%s" %  (frp >> 39, (frp >> 20) & 524287)

    if ltp:
        ltp = "%s:%s" %  (ltp >> 39, (ltp >> 20) & 524287)

    if fsp is not None:
        fsp = "%s:%s" %  (fsp >> 39, (fsp >> 20) & 524287)


    tx_map = dict()

    for row in ustxo:
        pending_received_outs_count += 1
        pending_received_amount += row["amount"]
        pending_spent_outs_count += 1
        pending_sent_amount += row["amount"]

        try:
            tx_map[row["tx_id"]] -= row["amount"]
        except:
            tx_map[row["tx_id"]] = 0 - row["amount"]

    for row in uutxo:
        pending_received_outs_count += 1
        pending_received_amount += row["amount"]

        try:
            tx_map[row["tx_id"]] -= row["amount"]
        except:
            tx_map[row["tx_id"]] = 0 - row["amount"]

    for k in tx_map:
        if tx_map[k] < 0:
            pending_sent_tx_count += 1
        else:
            pending_received_tx_count += 1


    return {"data": {"balance": balance,
                     "receivedAmount": received_amount,
                     "receivedTxCount": received_tx_count,
                     "sentAmount": sent_amount,
                     "sentTxCount": sent_tx_count,
                     "firstReceivedTxPointer": frp,
                     "firstSentTxPointer": fsp,
                     "lastTxPointer": ltp,

                     "largestSpentTxAmount": largest_spent_amount,
                     "largestSpentTxPointer": largest_spent_pointer,
                     "largestReceivedTxAmount": largest_received_amount,
                     "largestReceivedTxPointer": largest_received_pointer,


                     "receivedOutsCount": received_outs_count,
                     "spentOutsCount": spent_outs_count,
                     "pendingReceivedAmount": pending_received_amount,
                     "pendingSentAmount": pending_sent_amount,
                     "pendingReceivedTxCount": pending_received_tx_count,
                     "pendingSentTxCount": pending_sent_tx_count,
                     "pendingReceivedOutsCount": pending_received_outs_count,
                     "pendingSpentOutsCount": pending_spent_outs_count,
                     "type": type
                     },
            "time": round(time.time() - q, 4)}


async def address_list_state(addresses, type, app):
    q = time.time()
    pubkey_addresses = []
    pubkey_map = dict()
    a = set(addresses.keys())

    for address in addresses.keys():
        if address[0] == 0:
            pubkey_addresses.append(address[1:])

    async with app["db_pool"].acquire() as conn:
        if pubkey_addresses and (type is None or type == 2):
            urows = await conn.fetch("SELECT address, script from connector_unconfirmed_p2pk_map "
                                         "WHERE address = ANY($1);", pubkey_addresses)
            rows = await conn.fetch("SELECT script, address from connector_p2pk_map "
                                             "WHERE address = ANY($1);", pubkey_addresses)
            for row in urows:
                pubkey_map[b"\x02" + row["script"]] = b"\x00" + row["address"]
                a.add(b"\x02" + row["script"])
                if type == 2:
                    a.remove(b"\x00" + row["address"])

            for row in rows:
                pubkey_map[b"\x02" + row["script"]] = b"\x00" + row["address"]
                a.add(b"\x02" + row["script"])
                if type == 2:
                    a.remove(b"\x00" + row["address"])

        u_rows = await conn.fetch("SELECT address, amount "
                                      "FROM connector_unconfirmed_utxo "
                                      "WHERE address = ANY($1);", a)
        c_rows = await conn.fetch("SELECT  address, amount , outpoint "
                                      "FROM connector_utxo "
                                      "WHERE address = ANY($1);", a)

    r = dict()
    utxo = dict()

    for row in u_rows:
        try:
            a = addresses[row["address"]]
        except:
            a = addresses[pubkey_map[row["address"]]]

        try:
            r[a]["unconfirmed"] += row["amount"]
        except:
            r[a] = {"unconfirmed": row["amount"],
                                            "confirmed": 0}

    for row in c_rows:
        try:
            a = addresses[row["address"]]
        except:
            a = addresses[pubkey_map[row["address"]]]

        try:
            r[a]["confirmed"] += row["amount"]
        except:
            r[a] = {"confirmed": row["amount"],
                                            "uconfirmed": 0}
        utxo[row["outpoint"]] = (a, row["amount"])

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
        from_block = " AND pointer >= " + str(from_block << 39)
    else:
        from_block = ""

    if address[0] == 0 and type is None:
        a = [address]
        async with app["db_pool"].acquire() as conn:
            script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                         "WHERE address = $1 LIMIT 1;", address[1:])
            if script is not None:
                a.append(b"\x02" + script)
            rows = await conn.fetch("SELECT  outpoint, amount, pointer, address  "
                                          "FROM connector_utxo "
                                          "WHERE address = ANY($1) %s "
                                    "order by  %s %s LIMIT $2 OFFSET $3;" % (from_block, order_by, order),
                                    a, limit if limit else "ALL", limit * (page - 1))
            for row in rows:
                utxo.append({"txId": rh2s(row["outpoint"][:32]),
                             "vOut": bytes_to_int(row["outpoint"][32:]),
                             "block": row["pointer"] >> 39,
                             "txIndex": (row["pointer"] - ((row["pointer"] >> 39) << 39)) >> 20,
                             "amount": row["amount"],
                             "type": SCRIPT_N_TYPES[row["address"][0]]})

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


async def address_unconfirmed_utxo(address,  type, order, limit, page, app):
    q = time.time()
    utxo = []

    if address[0] == 0 and type is None:
        a = [address]
        async with app["db_pool"].acquire() as conn:
            script = await conn.fetchval("SELECT script from connector_unconfirmed_p2pk_map "
                                         "WHERE address = $1 LIMIT 1;", address[1:])
            if script is None:
                script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                             "WHERE address = $1 LIMIT 1;", address[1:])
            if script is not None:
                a.append(b"\x02" + script)
            rows = await conn.fetch("SELECT  outpoint, amount, address  "
                                          "FROM connector_unconfirmed_utxo "
                                    "WHERE address = ANY($1)  "
                                    "order by  amount %s LIMIT $2 OFFSET $3;" %  order,
                                    a, limit if limit else "ALL", limit * (page - 1))
            for row in rows:
                utxo.append({"txId": rh2s(row["outpoint"][:32]),
                             "vOut": bytes_to_int(row["outpoint"][32:]),
                             "amount": row["amount"],
                             "type": SCRIPT_N_TYPES[row["address"][0]]})

    else:
        async with app["db_pool"].acquire() as conn:
            if address[0] == 0:
                if type == 2:
                    script = await conn.fetchval("SELECT script from connector_unconfirmed_p2pk_map "
                                                 "WHERE address = $1 LIMIT 1;", address[1:])
                    if script is None:
                        script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                                     "WHERE address = $1 LIMIT 1;", address[1:])
                    if script is not None:
                        address = b"\x02" + script
                    else:
                        return {"data": utxo, "time": round(time.time() - q, 4)}


            rows = await conn.fetch("SELECT  outpoint, amount, address  "
                                          "FROM connector_unconfirmed_utxo "
                                    "WHERE address = $1 "
                                    "order by  amount %s LIMIT $2 OFFSET $3;" %  order,
                                    address, limit if limit else "ALL", limit * (page - 1))

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