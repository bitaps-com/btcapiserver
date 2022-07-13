from pybtc import *
from pybtc import rh2s, SCRIPT_N_TYPES
import time
import base64


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
                        if d["address"][0] in (0,1,2,5,6, 9):
                            script_hash = True if d["address"][0] in (1, 6) else False
                            witness_version = None if d["address"][0] < 5 else 0
                            if r["address"][0] == 9:
                                witness_version = 1
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

                        if d["address"][0] in (0,1,2,5,6,9):
                            script_hash = True if d["address"][0] in (1, 6) else False
                            witness_version = None if d["address"][0] < 5 else 0
                            if r["address"][0] == 9:
                                witness_version = 1
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


