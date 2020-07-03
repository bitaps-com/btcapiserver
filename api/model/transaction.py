from pyltc import *
from utils import APIException
from utils import NOT_FOUND
from pyltc import rh2s, SCRIPT_N_TYPES
import time
import base64
import json

async def tx_by_pointer_opt_tx(pointer, option_raw_tx, app):
    q = time.time()
    block_height = None
    block_index = None
    block_hash = None
    mempool_rank = None
    invalid_tx = False
    invalidation_timestamp = None
    mempool_conflict = False
    last_dep = dict()
    conflict_outpoints = deque()

    async with app["db_pool"].acquire() as conn:
        if isinstance(pointer, bytes):
            row = await conn.fetchrow("SELECT tx_id "
                                         "FROM mempool_dbs "
                                         "WHERE tx_id = $1 LIMIT 1;", pointer)
            if row:
                mempool_conflict = True

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
                    if r["address"][0] in (0,1,5,6):
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
                    if invalid_tx or mempool_conflict:
                        op = b"%s%s" % (s2rh(tx["vIn"][i]["txId"]), int_to_bytes(tx["vIn"][i]["vOut"]))
                        conflict_outpoints.append(op)
                        try:
                            last_dep[op[:32]].add(tx["vIn"][i]["vOut"])
                        except:
                            last_dep[op[:32]] = {tx["vIn"][i]["vOut"]}

                    if r["address"][0] in (0, 1, 5, 6):
                        script_hash = True if r["address"][0] in (1, 6) else False
                        witness_version = None if r["address"][0] < 5 else 0
                        try:
                            tx["vIn"][i]["address"] = hash_to_address(r["address"][1:],
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
    tx["feeRate"] = round(tx["fee"] / tx["vSize"], 2)

    if invalid_tx or mempool_conflict:
        m_conflict = []
        i_conflict_chain = []
        dep_chain = [[tx["txId"]]]
        conflict_outpoints_chain = [conflict_outpoints]

        # find out mainnet competitors and invalid tx chains
        while last_dep:
            async with app["db_pool"].acquire() as conn:
                rows = await conn.fetch("SELECT  tx_id, pointer FROM transaction where tx_id = ANY($1)", last_dep.keys())
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
                if invalid_tx:
                    rows = await conn.fetch("SELECT out_tx_id,"
                                            "       outpoint "
                                             "from invalid_stxo "
                                             "WHERE tx_id = ANY($1)", last_dep.keys())
                else:
                    rows = await conn.fetch("SELECT out_tx_id,"
                                            "       outpoint "
                                             "from connector_unconfirmed_stxo "
                                             "WHERE tx_id = ANY($1)", last_dep.keys())
            last_dep = dict()
            conflict_outpoints = deque()
            for row in rows:
                v_out = bytes_to_int(row["outpoint"][32:])
                try:
                    last_dep[row["out_tx_id"]].add(v_out)
                except:
                    last_dep[row["out_tx_id"]] = {v_out}

                conflict_outpoints.append(row["outpoint"])
            conflict_outpoints_chain.append(conflict_outpoints)


        o_pointers = set()
        pointer_map = dict()

        for row in rows:
            pointer = row["pointer"]
            t = row["tx_id"]
            for v_out in last_dep[t]:
                pointer_map[pointer +(1<<19) + v_out] = [rh2s(t), v_out]
                o_pointers.add(pointer +(1<<19) + v_out)
                if mempool_conflict:
                    m_conflict.append({
                        "outpoint": {"txId": rh2s(t),
                                        "vOut": v_out,
                                        "block": pointer >> 39}})

        if invalid_tx:
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

            if invalid_tx:
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
            else:
                async with app["db_pool"].acquire() as conn:
                    irows = await conn.fetch("SELECT connector_unconfirmed_stxo.tx_id,"
                                             "       connector_unconfirmed_stxo.out_tx_id, "
                                             "       connector_unconfirmed_stxo.input_index, "
                                             "       connector_unconfirmed_stxo.outpoint, "
                                             "       unconfirmed_transaction.size, "
                                             "       unconfirmed_transaction.b_size, "
                                             "       unconfirmed_transaction.feerate, "
                                             "       unconfirmed_transaction.fee,"
                                             "       unconfirmed_transaction.rbf, "
                                             "       unconfirmed_transaction.segwit "
                                             "from connector_unconfirmed_stxo "
                                             "JOIN unconfirmed_transaction "
                                             "ON unconfirmed_transaction.tx_id = connector_unconfirmed_stxo.tx_id "
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

        if invalid_tx:
            del tx["blockHash"]
            del tx["confirmations"]
            del tx["blockTime"]
            del tx["blockIndex"]
            del tx["blockHeight"]
            del tx["adjustedTimestamp"]
        if "flag" in tx:
            del tx["flag"]
        if invalid_tx:
            tx["conflict"] = {"blockchain": m_conflict,
                              "invalidTxCompetitors": i_conflict_chain,
                              "invalidTxChain": chain,
                              "invalidationTimestamp": invalidation_timestamp}
        else:
            tx["conflict"] = {"blockchain": m_conflict,
                              "txCompetitors": i_conflict_chain,
                              "unconfirmedChain": chain}

    return {"data": tx,
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
