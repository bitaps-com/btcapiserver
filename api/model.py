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
import base64

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
    block["header"] = row["header"].hex()
    block["adjustedTimestamp"] = row["adjusted_timestamp"]

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
                                  "FROM blocks  WHERE height >= $1 ORDER BY height ASC LIMIT $2;",
                                  pointer, count + 1)
    if not rows:
        raise APIException(NOT_FOUND, "block not found", status=404)
    resp = {"data": [row["header"].hex() for row in rows[1:]],
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

async def block_transactions_opt_tx(pointer, option_raw_tx, limit, page, order, app):
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
        if app["merkle_proof"]:
            rows = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer, merkle_proof  "
                                    "FROM transaction  WHERE pointer >= $1 AND pointer < $2 "
                                    "ORDER BY pointer %s LIMIT $3 OFFSET $4;" % order,
                                    pointer << 39, (pointer + 1) << 39, limit + 1, limit * (page - 1))
        else:
            rows = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer  "
                                    "FROM transaction  WHERE pointer >= $1 AND pointer < $2 "
                                    "ORDER BY pointer %s LIMIT $3 OFFSET $4;" % order,
                                    pointer << 39, (pointer + 1) << 39, limit + 1,  limit * (page - 1))
        block_time = unpack("<L", block_row["header"][68: 68 + 4])[0]
    transactions = list()
    for row in rows:
        tx = Transaction(row["raw_transaction"], format="decoded", testnet=app["testnet"], keep_raw_tx=option_raw_tx)
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
        else:
            tx["rawTx"] = base64.b64encode(bytes_from_hex(tx["rawTx"])).decode()
        if app["transaction_history"]:
            tx["inputsAmount"] = 0
        transactions.append(tx)
    last_page = False if len(rows) > limit else True

    if app["transaction_history"]:

        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT stxo.pointer,"
                                    "       stxo.s_pointer,"
                                    "       transaction_map.address, "
                                    "       transaction_map.amount  "
                                    "FROM stxo "
                                    "JOIN transaction_map "
                                    "ON transaction_map.pointer = stxo.pointer "

                                    "WHERE stxo.s_pointer >= $1 and "
                                    "stxo.s_pointer < $2 ;", block_height << 39, (block_height+1) << 39)

        print(rows)
        for r in rows:
            s = r["s_pointer"]
            i = (s - ((s>>19)<<19))
            m = (s - ((s>>39)<<39)) >> 20

            transactions[m]["vIn"][i]["type"] = SCRIPT_N_TYPES[r["address"][0]]
            transactions[m]["vIn"][i]["amount"] = r["amount"]
            transactions[m]["inputsAmount"] += r["amount"]
            transactions[m]["vIn"][i]["blockHeight"] = r["pointer"] >> 39
            transactions[m]["vIn"][i]["confirmations"] = app["last_block"] - (r["pointer"] >> 39) + 1
            if r["address"][0] in (0,1,2,5,6):
                script_hash = True if r["address"][0] in (1, 6) else False
                witness_version = None if r["address"][0] < 5 else 0
                transactions[m]["vIn"][i]["address"] = hash_to_address(r["address"][1:],
                                                                       testnet=app["testnet"],
                                                                       script_hash = script_hash,
                                                                       witness_version = witness_version)
                transactions[m]["vIn"][i]["scriptPubKey"] = address_to_script(transactions[m]["vIn"][i]["address"],
                                                                              hex=1)
            else:
                transactions[m]["vIn"][i]["scriptPubKey"] = r["address"][1:].hex()
            transactions[m]["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(transactions[m]["vIn"][i]["scriptPubKey"])
            transactions[m]["vIn"][i]["scriptPubKeyAsm"] = decode_script(transactions[m]["vIn"][i]["scriptPubKey"], 1)

        for m in range(len(transactions)):
            transactions[m]["fee"] = transactions[m]["inputsAmount"] - transactions[m]["amount"]
            transactions[m]["outputsAmount"] = transactions[m]["amount"]
        transactions[0]["fee"] = 0



    resp = {"data": {"height": block_height,
                     "hash": rh2s(block_row["hash"]),
                     "header": block_row["header"].hex(),
                     "adjustedTimestamp": block_row["adjusted_timestamp"],
                     "time": unpack("<L", block_row["header"][68: 68+4])[0],
                     "transactions":  transactions},
            "time": round(time.time() - q, 4),
            "lastPage": last_page}
    return resp

async def block_range_filter_headers(start_header, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if start_header:
            stmt = await conn.prepare("SELECT height "
                                      "FROM block_range_filters  "
                                      "WHERE hash = $1 LIMIT 1;")

            h = await stmt.fetchval(start_header)
            if h is None:
                raise APIException(NOT_FOUND, "from header not found", status=404)
        else:
            h=0
        rows = await conn.fetch("SELECT height,"
                                "       hash " 
                                "FROM block_range_filters  WHERE height >= $1 ORDER BY height ASC;", h)

        r = list()
        if h > 0:
            s_h = rows[0]["height"]
            for row in rows[1:]:
                r.append({"range": [s_h + 1, row["height"]], "header": row["hash"].hex()})
                s_h = row["height"]
        else:
            s_h = 0
            for row in rows:
                r.append({"range": [s_h + 1, row["height"]], "header": row["hash"].hex()})
                s_h = row["height"]

    if not rows:
        raise APIException(NOT_FOUND, "headers not found", status=404)
    resp = {"data": r,
            "time": round(time.time() - q, 4)}
    return resp

async def block_range_filter(pointer, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        stmt = await conn.prepare("SELECT filter "
                                  "FROM block_range_filters  "
                                  "WHERE hash = $1 LIMIT 1;")

        h = await stmt.fetchval(pointer)
        if h is None:
            raise APIException(NOT_FOUND, "filter not found", status=404)

    resp = {"data": base64.b64encode(h).decode(),
            "time": round(time.time() - q, 4)}
    return resp

async def block_filter_headers(pointer, count, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if isinstance(pointer, bytes):
            stmt = await conn.prepare("SELECT height "
                                      "FROM blocks  WHERE hash = $1 LIMIT 1;")
            pointer = await stmt.fetchval(pointer)

            if pointer is None:
                raise APIException(NOT_FOUND, "block not found", status=404)

        rows = await conn.fetch("SELECT hash "
                                  "FROM block_filters  WHERE height >= $1 ORDER BY height ASC LIMIT $2;",
                                  pointer, count + 1)
    if not rows:
        raise APIException(NOT_FOUND, "block not found", status=404)
    resp = {"data": [row["hash"].hex() for row in rows[1:]],
            "time": round(time.time() - q, 4)}
    return resp

async def block_filters(pointer, count, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if isinstance(pointer, bytes):
            stmt = await conn.prepare("SELECT height "
                                      "FROM blocks  WHERE hash = $1 LIMIT 1;")
            pointer = await stmt.fetchval(pointer)

            if pointer is None:
                raise APIException(NOT_FOUND, "block not found", status=404)

        rows = await conn.fetch("SELECT filter "
                                  "FROM block_filters  WHERE height >= $1 ORDER BY height ASC LIMIT $2;",
                                  pointer, count + 1)
    if not rows:
        raise APIException(NOT_FOUND, "block not found", status=404)

    resp = {"data": [base64.b64encode(row["filter"]).decode() for row in rows[1:]],
            "time": round(time.time() - q, 4)}
    return resp


async def block_filter(pointer, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        stmt = await conn.prepare("SELECT filter "
                                  "FROM block_filters  "
                                  "WHERE hash = $1 LIMIT 1;")

        h = await stmt.fetchval(pointer)
        if h is None:
            raise APIException(NOT_FOUND, "filter not found", status=404)

    resp = {"data": base64.b64encode(h).decode(),
            "time": round(time.time() - q, 4)}
    return resp


async def tx_by_pointer(pointer, app):
    q = time.time()
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
                block_index = (row["pointer"] >> 20) & 524287
        else:
            row = await conn.fetchrow("SELECT tx_id, timestamp, pointer "
                                      "FROM transaction "
                                      "WHERE pointer = $1 LIMIT 1;", pointer)
            if row is None:
                raise APIException(NOT_FOUND, "transaction not found", status=404)
            block_height = row["pointer"] >> 39
            block_index = (row["pointer"] >> 20) & 524287

    tx = {"txId": rh2s(row["tx_id"]),
          "blockHeight": block_height,
          "blockIndex": block_index,
          "timestamp": row["timestamp"]}

    return {"data": tx,
            "time": round(time.time() - q, 4)}

async def tx_by_pointers(pointers, hashes, app):
    q = time.time()

    async with app["db_pool"].acquire() as conn:
        h_rows, uh_rows, p_row = [], [], []
        if hashes:
            h_rows = await conn.fetch("SELECT tx_id, timestamp, pointer "
                                      "FROM transaction "
                                      "WHERE tx_id = ANY($1);", hashes)
        if len(h_rows) < len(hashes):
            uh_rows = await conn.fetch("SELECT tx_id, timestamp "
                                         "FROM unconfirmed_transaction "
                                         "WHERE tx_id = ANY($1);", hashes)

        if pointers:
            p_row = await conn.fetch("SELECT tx_id, timestamp, pointer "
                                      "FROM transaction "
                                      "WHERE pointer = ANY($1);", pointers)
        r = dict()
        txs = dict()
        for row in h_rows:
            block_height = row["pointer"] >> 39
            block_index = (row["pointer"] >> 20) & 524287
            r[row["tx_id"]] = {"txId": rh2s(row["tx_id"]),
                               "blockHeight": block_height,
                               "blockIndex": block_index,
                               "timestamp": row["timestamp"]}
        for row in uh_rows:
            block_height = None
            block_index = None
            r[row["tx_id"]] = {"txId": rh2s(row["tx_id"]),
                               "blockHeight": block_height,
                               "blockIndex": block_index,
                               "timestamp": row["timestamp"]}
        for row in p_row:
            block_height = row["pointer"] >> 39
            block_index = (row["pointer"] >> 20) & 524287
            r[row["pointer"]] = {"txId": rh2s(row["tx_id"]),
                                 "blockHeight": block_height,
                                 "blockIndex": block_index,
                                 "timestamp": row["timestamp"]}

        for pointer in pointers:
            try:
                key = "%s:%s" % (pointer >> 39, (pointer >> 20) & 524287)
                txs[key] = r[pointer]
            except: txs[key] = None
        for h in hashes:
            try: txs[rh2s(h)] = r[h]
            except: txs[rh2s(h)] = None

    return {"data": txs,
            "time": round(time.time() - q, 4)}


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
        s_pointers = [(block_height<<39)+(block_index<<20)+i for i in tx["vIn"]]

        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT stxo.pointer,"
                                    "       stxo.s_pointer,"
                                    "       transaction_map.address, "
                                    "       transaction_map.amount  "
                                    "FROM stxo "
                                    "JOIN transaction_map "
                                    "ON transaction_map.pointer = stxo.pointer "
                               
                                    "WHERE stxo.s_pointer = ANY($1);", s_pointers)

        assert len(rows) == len(s_pointers)

        tx["inputsAmount"] = 0

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
            else:
                tx["vIn"][i]["scriptPubKey"] = r["address"][1:].hex()
            tx["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(tx["vIn"][i]["scriptPubKey"])
            tx["vIn"][i]["scriptPubKeyAsm"] = decode_script(tx["vIn"][i]["scriptPubKey"], 1)


        tx["fee"] = tx["inputsAmount"] - tx["amount"]
        tx["outputsAmount"] = tx["amount"]





    tx["blockHeight"] = block_height
    tx["blockIndex"] = block_index
    tx["blockHash"] = rh2s(block_hash)
    tx["adjustedTimestamp"] = adjusted_timestamp
    tx["blockTime"] = block_time
    tx["timestamp"] = row["timestamp"]
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
            if not option_raw_tx: del tx["rawTx"]
            r[row["tx_id"]] = tx

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
            tx["timestamp"] = row["timestamp"]
            tx["confirmations"] = 0
            del tx["format"]
            del tx["testnet"]
            del tx["fee"]
            if not option_raw_tx: del tx["rawTx"]
            r[row["tx_id"]] = tx

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
            r[row["pointer"]] = tx

        for pointer in pointers:
            try:
                key = "%s:%s" % (pointer >> 39, (pointer >> 20) & 524287)
                txs[key] = r[pointer]
            except: txs[key] = None
        for h in hashes:
            try: txs[rh2s(h)] = r[h]
            except: txs[rh2s(h)] = None

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

    return {"data": {"block": row["pointer"] >> 39,
                     "index": (row["pointer"] >> 20) & 524287,
                     "merkleProof": base64.b64encode(row["merkle_proof"]).decode()},
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
        stmt = await conn.prepare("SELECT adjusted_timestamp, height , header  "
                                  "FROM blocks order by height asc;")
        rows = await stmt.fetch()
        for row in rows:
            app["block_map_time"][row["height"]] = (row["adjusted_timestamp"],
                                                    unpack("<L", row["header"][68: 68 + 4])[0])
            app["block_map_time"][row["height"]] = row["adjusted_timestamp"]
            app["day_map_block"][ceil(row["adjusted_timestamp"] / 86400)] = row["height"]
            app["time_map_block"][row["adjusted_timestamp"]] = row["height"]
            app["last_block"] = row["height"]
    app["log"].info("Loaded time map for %s blocks" % app["last_block"])
    app["loop"].create_task(block_map_update(app))




async def block_map_update(app):
    app["log"].info("block_map_update started")
    while True:
        try:
            async with app["db_pool"].acquire() as conn:
                stmt = await conn.prepare("SELECT adjusted_timestamp, height, header  "
                                          "FROM blocks WHERE height > $1 "
                                          "ORDER BY height asc;")
                rows = await stmt.fetch(app["last_block"])
            for row in rows:
                app["block_map_time"][row["height"]] = (row["adjusted_timestamp"],
                                                        unpack("<L", row["header"][68: 68 + 4])[0])
                app["day_map_block"][ceil(row["adjusted_timestamp"] / 86400)] = row["height"]
                app["time_map_block"][row["adjusted_timestamp"]] = row["height"]
                app["last_block"] = row["height"]
                app["log"].warning("New block: %s" % row["height"])
            await asyncio.sleep(1)
        except Exception as err:
            app["log"].error("block_map_update: "+str(err))
            await asyncio.sleep(5)


async def close_db_pool(app):
    await app["db_pool"].close()