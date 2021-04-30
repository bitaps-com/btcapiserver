from pybtc import *
from utils import APIException
from utils import NOT_FOUND
from .service import block_map_update
from pybtc import rh2s, SCRIPT_N_TYPES
import time
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
    block["header"] = base64.b64encode(row["header"]).decode()
    block["adjustedTimestamp"] = row["adjusted_timestamp"]

    resp = {"data": block,
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

async def block_data_by_pointer(pointer, stat, app):
    pool = app["db_pool"]
    qt = time.time()
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
        if block["height"] not in app["block_map_time"]:
            await block_map_update(app)
            if block["height"] not in app["block_map_time"]:

                raise Exception("internal error")
        block["hash"] = rh2s(row["hash"])
        block["header"] = base64.b64encode(row["header"]).decode()
        d = json.loads(row["data"])
        for k in  d:
            block[k] = d[k]
        try:
            block["miner"] = json.loads(row["miner"])
        except:
            block["miner"] = None
        print(block["height"] not in app["block_map_time"])
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

    if stat and app["blockchain_analytica"]:
        async with pool.acquire() as conn:
            stat = await conn.fetchval("SELECT block FROM block_stat WHERE height = $1 LIMIT 1;", row["height"])
        if stat is not None:
            block["statistics"] = json.loads(stat)
        else:
            block["statistics"] = None


    resp = {"data": block,
            "time": round(time.time() - qt, 4)}
    return resp

async def block_transactions(pointer, option_raw_tx, limit, page, order, mode, app):
    pool = app["db_pool"]
    qt = time.time()

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

        count = var_int_to_int(block_row["header"][80:])
        pages = math.ceil(count / limit)
        if app["merkle_proof"]:
            rows = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer, merkle_proof  "
                                    "FROM transaction  WHERE pointer >= $1 AND pointer < $2 "
                                    "ORDER BY pointer %s LIMIT $3 OFFSET $4;" % order,
                                    pointer << 39, (pointer + 1) << 39, limit, limit * (page - 1))
        else:
            rows = await conn.fetch("SELECT tx_id, raw_transaction,  timestamp, pointer  "
                                    "FROM transaction  WHERE pointer >= $1 AND pointer < $2 "
                                    "ORDER BY pointer %s LIMIT $3 OFFSET $4;" % order,
                                    pointer << 39, (pointer + 1) << 39, limit, limit * (page - 1))
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
                if transactions[m]["coinbase"]:
                    transactions[m]["fee"] = 0
                else:
                    transactions[m]["fee"] = transactions[m]["inputsAmount"] - transactions[m]["amount"]
                transactions[m]["outputsAmount"] = transactions[m]["amount"]
                if mode != "verbose":
                    transactions[m]["inputs"] = len(transactions[m]["vIn"])
                    transactions[m]["outputs"] = len(transactions[m]["vOut"])
                    del transactions[m]["vIn"]
                    del transactions[m]["vOut"]
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


    resp = {"data": {"list": transactions,
                     "page": page,
                     "pages": pages,
                     "total": count,
                     "limit": limit},
            "time": round(time.time() - qt, 4)}
    return resp

async def block_transaction_id_list(pointer, limit, page, order, app):
    pool = app["db_pool"]
    qt = time.time()

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
                                    pointer << 39, (pointer + 1) << 39, limit, limit * (page - 1))
        transactions = [rh2s(t["tx_id"]) for t in rows]
        app["block_transaction_id_list"][pointer] = transactions
    resp = {"data": transactions,
            "time": round(time.time() - qt, 4)}
    return resp


async def blockchain_state(pointer, app):
    pool = app["db_pool"]
    qt = time.time()
    async with pool.acquire() as conn:
        if pointer == 'last':
            stmt = await conn.prepare("SELECT blockchian "
                                      "FROM blockchian_stat  ORDER BY height desc LIMIT 1;")
            row = await stmt.fetchrow()
        else:
            if type(pointer) == bytes:
                stmt = await conn.prepare("SELECT blockchian "
                                          "FROM blockchian_stat  WHERE hash = $1 LIMIT 1;")
                row = await stmt.fetchrow(pointer)

            elif type(pointer) == int:
                stmt = await conn.prepare("SELECT blockchian "
                                          "FROM blockchian_stat  WHERE height = $1 LIMIT 1;")
                row = await stmt.fetchrow(pointer)

        if row is None:
            raise APIException(NOT_FOUND, "block not found", status=404)

    resp = {"data": json.loads(row["blockchian"]),
            "time": round(time.time() - qt, 4)}
    return resp
