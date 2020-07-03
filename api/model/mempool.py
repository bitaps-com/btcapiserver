from pybtc import *
from utils import *
from pybtc import rh2s
import time


async def mempool_transactions(limit, page, order, from_timestamp, mode, app):
    qt = time.time()
    async with app["db_pool"].acquire() as conn:
        async with conn.transaction():
            count = await conn.fetchval("SELECT count(tx_id) FROM unconfirmed_transaction "
                                        "WHERE timestamp > $1;", from_timestamp)
            pages = math.ceil(count / limit)

            rows = await conn.fetch("SELECT   "
                                    "        unconfirmed_transaction.tx_id,"
                                    "        mempool_dbs.child,"
                                    "        unconfirmed_transaction.amount,  "
                                    "        unconfirmed_transaction.size,  "
                                    "        unconfirmed_transaction.b_size,  "
                                    "        unconfirmed_transaction.fee,  "
                                    "        unconfirmed_transaction.feerate,  "
                                    "        unconfirmed_transaction.rbf,  "
                                    "        unconfirmed_transaction.segwit,  "
                                    "        unconfirmed_transaction.timestamp,  "
                                    "        unconfirmed_transaction.raw_transaction "
                                    "FROM unconfirmed_transaction "
                                    "LEFT OUTER JOIN mempool_dbs "
                                    "ON unconfirmed_transaction.tx_id = mempool_dbs.tx_id "
                                    "WHERE unconfirmed_transaction.timestamp > $1 "
                                    "ORDER BY unconfirmed_transaction.timestamp %s LIMIT $2 OFFSET $3;" % order,
                                    from_timestamp, limit,  limit * (page - 1))

            h = [r["tx_id"] for r in rows]

            m_rows = await conn.fetch("""SELECT ranks.tx_id, ranks.rank FROM 
                                      (SELECT tx_id, rank() OVER(ORDER BY feerate DESC) as rank 
                                      FROM unconfirmed_transaction) ranks 
                                      WHERE tx_id = ANY($1) LIMIT $2""", h, len(h))
        rank_map = dict()
    for row in m_rows:
        rank_map[row["tx_id"]] = row["rank"]

    tx_list = []
    dep_chain = dict()
    conflict_outpoints_chain = dict()
    conflict_outpoints_accum = set()
    last_dep = dict()
    last_dep_map = dict()
    conflict_outpoint_map = dict()
    us_pointers = deque()
    us_pointers_inputs = deque()

    for row in rows:
        tx = Transaction(row["raw_transaction"], testnet=app["testnet"])
        tx["fee"] = row["fee"]
        tx["time"] = row["timestamp"]
        tx["feeRate"] = round(row["feerate"], 2)
        tx["rbf"] = bool(row["rbf"])
        tx["segwit"] = bool(row["segwit"])
        tx["mempoolRank"] = rank_map[row["tx_id"]]
        if "flag" in tx:
            del tx["flag"]

        if row["child"] is not None:
            tx["conflict"] = {"blockchain": [],
                              "txCompetitors": [],
                              "unconfirmedChain": [],
                              "child": bool(row["child"])}
            dep_chain[tx["txId"]] = [{s2rh(tx["txId"])}, set()]
        else:
            tx["conflict"] = None


        tx_list.append(tx)

        conflict_outpoint = deque()


        for q in tx["vIn"]:
            op = b"%s%s" % (s2rh(tx["vIn"][q]["txId"]), int_to_bytes(tx["vIn"][q]["vOut"]))
            if tx["conflict"]:
                try:
                    last_dep[op[:32]].add(tx["vIn"][q]["vOut"])
                except:
                    last_dep[op[:32]] = {tx["vIn"][q]["vOut"]}
                dep_chain[tx["txId"]][-1].add(op[:32])
                us_pointers_inputs.append(op[:32])
                try:
                    last_dep_map[op[:32]].add(tx["txId"])
                except:
                    last_dep_map[op[:32]] = {tx["txId"]}

                conflict_outpoint_map[op] = tx["txId"]
                conflict_outpoint.append(op)
                conflict_outpoints_accum.add(op)

        if tx["conflict"]:
            conflict_outpoints_chain[tx["txId"]] = [conflict_outpoint]

        us_pointers.append(s2rh(tx["txId"]))



    # get info about inputs
    if  mode=="verbose" and us_pointers:
        o_pointers = us_pointers
        async with app["db_pool"].acquire() as conn:
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

        # get information about spent output coins

        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT   outpoint,"
                                    "         input_index,"
                                    "       tx_id, "
                                    "       out_tx_id "
                                    "FROM connector_unconfirmed_stxo "
                                    "WHERE out_tx_id = ANY($1);", o_pointers)
        spent_outs_map = dict()
        for r in rows:
            t = rh2s(r["out_tx_id"])
            i = bytes_to_int(r["outpoint"][32:])
            try:
                spent_outs_map[t][i] = {"txId": rh2s(r["tx_id"]), "vIn": r["input_index"]}
            except:
                spent_outs_map[t] = {i: {"txId": rh2s(r["tx_id"]), "vIn": r["input_index"]}}

        for t in range(len(tx_list)):

            tx_list[t]["inputsAmount"] = 0
            for i in tx_list[t]["vIn"]:
                d = us_pointers[(tx_list[t]["txId"], i)]
                tx_list[t]["vIn"][i]["type"] = SCRIPT_N_TYPES[d["address"][0]]
                tx_list[t]["vIn"][i]["amount"] = d["amount"]
                tx_list[t]["inputsAmount"] += d["amount"]
                try:
                    pointer = tx_id_map_pointer[tx_list[t]["vIn"][i]["txId"]]
                    tx_list[t]["vIn"][i]["blockHeight"] = pointer >> 39
                    tx_list[t]["vIn"][i]["confirmations"] = app["last_block"] - (pointer >> 39) + 1
                except:
                    tx_list[t]["vIn"][i]["blockHeight"] = None
                    tx_list[t]["vIn"][i]["confirmations"] = None

                if d["address"][0] in (0, 1, 2, 5, 6):
                    script_hash = True if d["address"][0] in (1, 6) else False
                    witness_version = None if d["address"][0] < 5 else 0
                    try:
                        if d["address"][0] == 2:
                            ad = b"\x02" + parse_script(d["address"][1:])["addressHash"]
                        else:
                            ad = d["address"]
                        tx_list[t]["vIn"][i]["address"] = hash_to_address(ad[1:],
                                                                    testnet=app["testnet"],
                                                                    script_hash=script_hash,
                                                                    witness_version=witness_version)

                        tx_list[t]["vIn"][i]["scriptPubKey"] = address_to_script(tx_list[t]["vIn"][i]["address"], hex=1)
                    except:
                        print(tx_list[t]["txId"])
                        print("??", d["address"].hex())
                        raise

                elif tx_list["address"][0] == 2:
                    tx_list[t]["vIn"][i]["address"] = script_to_address(d["address"][1:], testnet=app["testnet"])
                    tx_list[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                else:
                    tx_list[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                tx_list[t]["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(tx_list[t]["vIn"][i]["scriptPubKey"])
                tx_list[t]["vIn"][i]["scriptPubKeyAsm"] = decode_script(tx_list[t]["vIn"][i]["scriptPubKey"], 1)


            for i in tx_list[t]["vOut"]:
                tx_list[t]["vOut"][i]['spent'] = []
            try:
                s_outs = spent_outs_map[tx_list[t]["txId"]]
                for s in s_outs:
                    tx_list[t]["vOut"][s]['spent'].append(s_outs[s])
            except:
                pass



    # find out mainnet competitors and invalid chains
    last_pointer_map = dict()
    while last_dep:
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT  tx_id, pointer FROM transaction where tx_id = ANY($1)",
                                    last_dep.keys())
        tmp_lp = dict()
        for row in rows:
            tl = last_dep_map[row["tx_id"]]
            for t in tl:
                try:
                    v_outs =  last_dep[row["tx_id"]]
                    dep_chain[t][-1].remove(row["tx_id"])
                    try:
                        tmp_lp[t][row["pointer"]] = {"txId": row["tx_id"], "vOuts": v_outs}
                    except:
                        tmp_lp[t] = {row["pointer"]: {"txId": row["tx_id"], "vOuts": v_outs}}

                except:
                    pass

        for t in tmp_lp:
            last_pointer_map[t] = tmp_lp[t]
            if not dep_chain[t][-1]:
                dep_chain[t].pop()

        # go deeper
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT connector_unconfirmed_stxo.tx_id,"
                                    "       connector_unconfirmed_stxo.out_tx_id,"
                                    "       outpoint "
                                    "from connector_unconfirmed_stxo "
                                    "WHERE tx_id = ANY($1)", last_dep.keys())
        last_dep = dict()
        new_last_dep_map = dict()
        new_dep_chain = dict()
        conflict_outpoints = deque()
        tmp_co = dict()
        for row in rows:
            tl = last_dep_map[row["tx_id"]]
            for t in tl:
                try:
                    new_last_dep_map[row["out_tx_id"]].add(t)
                except:
                    new_last_dep_map[row["out_tx_id"]] = {t}

                o = row["outpoint"][:32]
                i = bytes_to_int(row["outpoint"][32:])
                try:
                    new_dep_chain[t].add(o)
                except:
                    new_dep_chain[t] = {o}

                try:
                    last_dep[row["out_tx_id"]].add(i)
                except:
                    last_dep[row["out_tx_id"]] = {i}

                conflict_outpoints.append(row["outpoint"])
                conflict_outpoints_accum.add(row["outpoint"])
                try:
                    tmp_co[t].append(row["outpoint"])
                except:
                    tmp_co[t] = [row["outpoint"]]

        for t in tmp_co:
            conflict_outpoints_chain[t].append(tmp_co[t])
            dep_chain[t].append(new_dep_chain[t])
        last_dep_map = new_last_dep_map

    # check invalid transactions competitors
    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT connector_unconfirmed_stxo.tx_id,"
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
                                 "WHERE outpoint = ANY($1)", conflict_outpoints_accum)

    conflict_outpoints_map = dict()
    for row in rows:
        try:
            conflict_outpoints_map[row["outpoint"]].append(row)
        except:
            conflict_outpoints_map[row["outpoint"]] = [row]

    # convert last pointers to coins
    o_pointers = set()
    pointer_map_coin = dict()
    pointer_map_competitor = dict()

    for t in last_pointer_map:
        for pointer in last_pointer_map[t]:
            for v_out in last_pointer_map[t][pointer]["vOuts"]:
                pointer_map_coin[pointer + (1 << 19) + v_out] = {"txId": rh2s(last_pointer_map[t][pointer]["txId"]),
                                                                 "vOut": v_out,
                                                                 "block": pointer >> 39}
                o_pointers.add(pointer + (1 << 19) + v_out)
    if o_pointers:
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT transaction.tx_id, stxo.s_pointer, stxo.pointer  "
                                    "FROM stxo "
                                    "JOIN transaction on transaction.pointer = (stxo.s_pointer >> 18)<<18 "
                                    "WHERE stxo.pointer = ANY($1);", o_pointers)

        for row in rows:
            pointer_map_competitor[row["pointer"]] = {"txId": rh2s(row["tx_id"]),
                                                      "vIn":row["s_pointer"] &  524287,
                                                      "block": row["s_pointer"] >> 39}


    for i in range(len(tx_list)):
        if tx_list[i]["conflict"]:
            t = tx_list[i]["txId"]
            m_conflict = []
            if t not in last_pointer_map:
                continue
            for pointer in last_pointer_map[t]:
                for v_out in last_pointer_map[t][pointer]["vOuts"]:
                    c_pointer = pointer + (1 << 19) + v_out
                    dbs = pointer_map_coin[c_pointer]
                    m_conflict.append({"outpoint": dbs})

            tx_list[i]["conflict"]["blockchain"] = m_conflict
            tx_list[i]["conflict"]["unconfirmedChain"] = [[rh2s(y) for y in l] for l in dep_chain[t]]

            i_conflict = []
            atx = {s2rh(t)}
            for outpoints in conflict_outpoints_chain[t]:
                [atx.add(out[:32]) for out in outpoints]
                m = []
                for out in outpoints:
                    rows = conflict_outpoints_map[out]
                    for row in rows:
                        if row["tx_id"] not in atx:
                            m.append({"doublespend": {"txId": rh2s(row["outpoint"][:32]),
                                                                   "vOut": bytes_to_int(row["outpoint"][32:])},
                                                   "competitor": {"txId": rh2s(row["tx_id"]),
                                                                  "vIn":row["input_index"],
                                                                  "size": row["size"],
                                                                  "bSize": row["b_size"],
                                                                  "feeRate": round(row["feerate"], 2),
                                                                  "fee": row["fee"],
                                                                  "rbf": bool(row["rbf"]),
                                                                  "segwit": bool(row["segwit"])
                                                                  }})
                i_conflict.append(m)

            tx_list[i]["conflict"]["txCompetitors"] = i_conflict
        tx_list[i]["valid"] = True
        if mode == "brief":
            del tx_list[i]["vIn"]
            del tx_list[i]["vOut"]
        del tx_list[i]["format"]
        del tx_list[i]["testnet"]
        del tx_list[i]["blockTime"]
        del tx_list[i]["rawTx"]
        del tx_list[i]["blockHash"]
        del tx_list[i]["confirmations"]
        del tx_list[i]["blockIndex"]






    return {"data": {"page": page,
                     "limit": limit,
                     "pages": pages,
                     "count": count,
                     "fromTimestamp":from_timestamp,
                     "list": tx_list},
            "time": round(time.time() - qt, 4)}

async def invalid_transactions(limit, page, order, from_timestamp, mode, app):
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
    dep_chain = dict()
    conflict_outpoints_chain = dict()
    conflict_outpoints_accum = set()
    last_dep = dict()
    last_dep_map = dict()
    conflict_outpoint_map = dict()
    us_pointers = deque()
    us_pointers_inputs = deque()

    for row in rows:
        tx = Transaction(row["raw_transaction"], testnet=app["testnet"])
        tx["fee"] = row["fee"]
        tx["time"] = row["timestamp"]
        tx["feeRate"] = round(row["feerate"], 2)
        tx["rbf"] = bool(row["feerate"])
        tx["segwit"] = bool(row["feerate"])
        if "flag" in tx:
            del tx["flag"]
        tx["conflict"] = {"blockchain": [], "invalidTxCompetitors": [], "invalidTxChain": [],
                          "invalidationTimestamp": row["invalidation_timestamp"]}

        tx_list.append(tx)

        conflict_outpoint = deque()
        dep_chain[tx["txId"]] = [{s2rh(tx["txId"])}, set()]

        for q in tx["vIn"]:
            op = b"%s%s" % (s2rh(tx["vIn"][q]["txId"]), int_to_bytes(tx["vIn"][q]["vOut"]))
            try:
                last_dep[op[:32]].add(tx["vIn"][q]["vOut"])
            except:
                last_dep[op[:32]] = {tx["vIn"][q]["vOut"]}
            dep_chain[tx["txId"]][-1].add(op[:32])
            us_pointers_inputs.append(op[:32])
            try:
                last_dep_map[op[:32]].add(tx["txId"])
            except:
                last_dep_map[op[:32]] = {tx["txId"]}

            conflict_outpoint_map[op] = tx["txId"]
            conflict_outpoint.append(op)
            conflict_outpoints_accum.add(op)
        us_pointers.append(s2rh(tx["txId"]))
        conflict_outpoints_chain[tx["txId"]] = [conflict_outpoint]


    # get info about inputs
    if  mode=="verbose" and us_pointers:
        o_pointers = us_pointers
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT   outpoint,"
                                    "         input_index,"
                                    "       out_tx_id, "
                                    "       tx_id,"
                                    "       address, "
                                    "       amount "
                                    "    "
                                    "FROM invalid_stxo "
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

        # get information about spent output coins

        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT   outpoint,"
                                    "         input_index,"
                                    "       tx_id, "
                                    "       out_tx_id "
                                    "FROM invalid_stxo "
                                    "WHERE out_tx_id = ANY($1);", o_pointers)
        spent_outs_map = dict()
        for r in rows:
            t = rh2s(r["out_tx_id"])
            i = bytes_to_int(r["outpoint"][32:])
            try:
                spent_outs_map[t][i] = {"txId": rh2s(r["tx_id"]), "vIn": r["input_index"]}
            except:
                spent_outs_map[t] = {i: {"txId": rh2s(r["tx_id"]), "vIn": r["input_index"]}}

        for t in range(len(tx_list)):

            tx_list[t]["inputsAmount"] = 0
            for i in tx_list[t]["vIn"]:
                d = us_pointers[(tx_list[t]["txId"], i)]
                tx_list[t]["vIn"][i]["type"] = SCRIPT_N_TYPES[d["address"][0]]
                tx_list[t]["vIn"][i]["amount"] = d["amount"]
                tx_list[t]["inputsAmount"] += d["amount"]
                try:
                    pointer = tx_id_map_pointer[tx_list[t]["vIn"][i]["txId"]]
                    tx_list[t]["vIn"][i]["blockHeight"] = pointer >> 39
                    tx_list[t]["vIn"][i]["confirmations"] = app["last_block"] - (pointer >> 39) + 1
                except:
                    tx_list[t]["vIn"][i]["blockHeight"] = None
                    tx_list[t]["vIn"][i]["confirmations"] = None

                if d["address"][0] in (0, 1, 2, 5, 6):
                    script_hash = True if d["address"][0] in (1, 6) else False
                    witness_version = None if d["address"][0] < 5 else 0
                    try:
                        if d["address"][0] == 2:
                            ad = b"\x02" + parse_script(d["address"][1:])["addressHash"]
                        else:
                            ad = d["address"]
                        tx_list[t]["vIn"][i]["address"] = hash_to_address(ad[1:],
                                                                    testnet=app["testnet"],
                                                                    script_hash=script_hash,
                                                                    witness_version=witness_version)

                        tx_list[t]["vIn"][i]["scriptPubKey"] = address_to_script(tx_list[t]["vIn"][i]["address"], hex=1)
                    except:
                        print(tx_list[t]["txId"])
                        print("??", d["address"].hex())
                        raise

                elif tx_list["address"][0] == 2:
                    tx_list[t]["vIn"][i]["address"] = script_to_address(d["address"][1:], testnet=app["testnet"])
                    tx_list[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                else:
                    tx_list[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                tx_list[t]["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(tx_list[t]["vIn"][i]["scriptPubKey"])
                tx_list[t]["vIn"][i]["scriptPubKeyAsm"] = decode_script(tx_list[t]["vIn"][i]["scriptPubKey"], 1)


            for i in tx_list[t]["vOut"]:
                tx_list[t]["vOut"][i]['spent'] = []
            try:
                s_outs = spent_outs_map[tx_list[t]["txId"]]
                for s in s_outs:
                    tx_list[t]["vOut"][s]['spent'].append(s_outs[s])
            except:
                pass





    # find out mainnet competitiors and invalid chains
    last_pointer_map = dict()


    while last_dep:
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT  tx_id, pointer FROM transaction where tx_id = ANY($1)",
                                    last_dep.keys())
        tmp_lp = dict()
        for row in rows:
            tl = last_dep_map[row["tx_id"]]
            for t in tl:
                try:
                    v_outs =  last_dep[row["tx_id"]]
                    dep_chain[t][-1].remove(row["tx_id"])
                    try:
                        tmp_lp[t][row["pointer"]] = {"txId": row["tx_id"], "vOuts": v_outs}
                    except:
                        tmp_lp[t] = {row["pointer"]: {"txId": row["tx_id"], "vOuts": v_outs}}

                except:
                    pass

        for t in tmp_lp:
            last_pointer_map[t] = tmp_lp[t]
            if not dep_chain[t][-1]:
                dep_chain[t].pop()

        # go deeper
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT invalid_stxo.tx_id, invalid_stxo.out_tx_id,"
                                    "        outpoint "
                                    "from invalid_stxo "
                                    "WHERE tx_id = ANY($1)", last_dep.keys())
        last_dep = dict()
        new_last_dep_map = dict()
        new_dep_chain = dict()
        conflict_outpoints = deque()
        tmp_co = dict()
        for row in rows:
            tl = last_dep_map[row["tx_id"]]
            for t in tl:
                try:
                    new_last_dep_map[row["out_tx_id"]].add(t)
                except:
                    new_last_dep_map[row["out_tx_id"]] = {t}

                o = row["outpoint"][:32]
                i = bytes_to_int(row["outpoint"][32:])
                try:
                    new_dep_chain[t].add(o)
                except:
                    new_dep_chain[t] = {o}

                try:
                    last_dep[row["out_tx_id"]].add(i)
                except:
                    last_dep[row["out_tx_id"]] = {i}

                conflict_outpoints.append(row["outpoint"])
                conflict_outpoints_accum.add(row["outpoint"])
                try:
                    tmp_co[t].append(row["outpoint"])
                except:
                    tmp_co[t] = [row["outpoint"]]

        for t in tmp_co:
            conflict_outpoints_chain[t].append(tmp_co[t])
            dep_chain[t].append(new_dep_chain[t])
        last_dep_map = new_last_dep_map

    # check invalid transactions competitors
    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT invalid_stxo.tx_id,"
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
                                 "WHERE outpoint = ANY($1)", conflict_outpoints_accum)

    conflict_outpoints_map = dict()
    for row in rows:
        try:
            conflict_outpoints_map[row["outpoint"]].append(row)
        except:
            conflict_outpoints_map[row["outpoint"]] = [row]

    # convert last pointers to coins
    o_pointers = set()
    pointer_map_coin = dict()
    pointer_map_competitor = dict()

    for t in last_pointer_map:
        for pointer in last_pointer_map[t]:
            for v_out in last_pointer_map[t][pointer]["vOuts"]:
                pointer_map_coin[pointer + (1 << 19) + v_out] = {"txId": rh2s(last_pointer_map[t][pointer]["txId"]),
                                                                 "vOut": v_out,
                                                                 "block": pointer >> 39}
                o_pointers.add(pointer + (1 << 19) + v_out)
    if o_pointers:
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT transaction.tx_id, stxo.s_pointer, stxo.pointer  "
                                    "FROM stxo "
                                    "JOIN transaction on transaction.pointer = (stxo.s_pointer >> 18)<<18 "
                                    "WHERE stxo.pointer = ANY($1);", o_pointers)

        for row in rows:
            pointer_map_competitor[row["pointer"]] = {"txId": rh2s(row["tx_id"]),
                                                      "vIn":row["s_pointer"] &  524287,
                                                      "block": row["s_pointer"] >> 39}


    for i in range(len(tx_list)):
        t = tx_list[i]["txId"]
        m_conflict = []
        for pointer in last_pointer_map[t]:
            for v_out in last_pointer_map[t][pointer]["vOuts"]:
                c_pointer = pointer + (1 << 19) + v_out
                dbs = pointer_map_coin[c_pointer]
                cmpt = pointer_map_competitor[c_pointer]
                m_conflict.append({"doublespend": dbs, "competitor": cmpt})

        tx_list[i]["conflict"]["blockchain"] = m_conflict
        tx_list[i]["conflict"]["invalidTxChain"] = [[rh2s(y) for y in l] for l in dep_chain[t]]

        i_conflict = []
        atx = {s2rh(t)}
        for outpoints in conflict_outpoints_chain[t]:
            [atx.add(out[:32]) for out in outpoints]
            m = []
            for out in outpoints:
                rows = conflict_outpoints_map[out]
                for row in rows:
                    if row["tx_id"] not in atx:
                        m.append({"doublespend": {"txId": rh2s(row["outpoint"][:32]),
                                                               "vOut": bytes_to_int(row["outpoint"][32:])},
                                               "competitor": {"txId": rh2s(row["tx_id"]),
                                                              "vIn":row["input_index"],
                                                              "size": row["size"],
                                                              "bSize": row["b_size"],
                                                              "feeRate": round(row["feerate"], 2),
                                                              "fee": row["fee"],
                                                              "rbf": bool(row["rbf"]),
                                                              "segwit": bool(row["segwit"])
                                                              }})
            i_conflict.append(m)

        tx_list[i]["conflict"]["invalidTxCompetitors"] = i_conflict
        tx_list[i]["valid"] = False
        if mode=="brief":
            del tx_list[i]["vIn"]
            del tx_list[i]["vOut"]
        del tx_list[i]["format"]
        del tx_list[i]["testnet"]
        del tx_list[i]["blockTime"]
        del tx_list[i]["rawTx"]
        del tx_list[i]["blockHash"]
        del tx_list[i]["confirmations"]
        del tx_list[i]["blockIndex"]

    return {"data": {"page": page,
                     "limit": limit,
                     "fromTimestamp":from_timestamp,
                     "list": tx_list},
            "time": round(time.time() - qq, 4)}

async def mempool_doublespend(limit, page, order, from_timestamp, dbs_type, mode, app):
    qt = time.time()
    async with app["db_pool"].acquire() as conn:
        count = await conn.fetchval("SELECT count(tx_id) FROM mempool_dbs "
                                    "WHERE timestamp > $1;", from_timestamp)

        pages = math.ceil(count / limit)

        if dbs_type == None:
            rows = await conn.fetch("SELECT   "
                                    "        mempool_dbs.tx_id,"
                                    "        mempool_dbs.child,"
                                    "        unconfirmed_transaction.amount,  "
                                    "        unconfirmed_transaction.size,  "
                                    "        unconfirmed_transaction.b_size,  "
                                    "        unconfirmed_transaction.fee,  "
                                    "        unconfirmed_transaction.feerate,  "
                                    "        unconfirmed_transaction.rbf,  "
                                    "        unconfirmed_transaction.segwit,  "
                                    "        mempool_dbs.timestamp,  "
                                    "        unconfirmed_transaction.raw_transaction "
                                    "FROM mempool_dbs "
                                    "JOIN unconfirmed_transaction "
                                    "ON unconfirmed_transaction.tx_id = mempool_dbs.tx_id "
                                    "WHERE mempool_dbs.timestamp > $1 "
                                    "ORDER BY mempool_dbs.timestamp %s LIMIT $2 OFFSET $3;" % order,
                                    from_timestamp, limit,  limit * (page - 1))
        else:
            rows = await conn.fetch("SELECT   "
                                    "        mempool_dbs.tx_id,"
                                    "        mempool_dbs.child,"
                                    "        unconfirmed_transaction.amount,  "
                                    "        unconfirmed_transaction.size,  "
                                    "        unconfirmed_transaction.b_size,  "
                                    "        unconfirmed_transaction.fee,  "
                                    "        unconfirmed_transaction.feerate,  "
                                    "        unconfirmed_transaction.rbf,  "
                                    "        unconfirmed_transaction.segwit,  "
                                    "        mempool_dbs.timestamp,  "
                                    "        unconfirmed_transaction.raw_transaction "
                                    "FROM mempool_dbs "
                                    "JOIN unconfirmed_transaction "
                                    "ON unconfirmed_transaction.tx_id = mempool_dbs.tx_id "
                                    "WHERE child = $4 and mempool_dbs.timestamp > $1 "
                                    "ORDER BY mempool_dbs.timestamp %s LIMIT $2 OFFSET $3;" % order,
                                    from_timestamp, limit,  limit * (page - 1), dbs_type)


        h = [r["tx_id"] for r in rows]

        m_rows = await conn.fetch("""SELECT ranks.tx_id, ranks.rank FROM 
                                  (SELECT tx_id, rank() OVER(ORDER BY feerate DESC) as rank 
                                  FROM unconfirmed_transaction) ranks 
                                  WHERE tx_id = ANY($1) LIMIT $2""", h, len(h))
    rank_map = dict()
    for row in m_rows:
        rank_map[row["tx_id"]] = row["rank"]

    tx_list = []
    dep_chain = dict()
    conflict_outpoints_chain = dict()
    conflict_outpoints_accum = set()
    last_dep = dict()
    last_dep_map = dict()
    conflict_outpoint_map = dict()
    us_pointers = deque()
    us_pointers_inputs = deque()

    for row in rows:
        tx = Transaction(row["raw_transaction"], testnet=app["testnet"])
        tx["fee"] = row["fee"]
        tx["time"] = row["timestamp"]
        tx["feeRate"] = round(row["feerate"], 2)
        tx["rbf"] = bool(row["feerate"])
        tx["segwit"] = bool(row["feerate"])
        tx["mempoolRank"] = rank_map[row["tx_id"]]
        if "flag" in tx:
            del tx["flag"]
        tx["conflict"] = {"blockchain": [], "txCompetitors": [], "unconfirmedChain": [],
                          }

        tx_list.append(tx)

        conflict_outpoint = deque()
        dep_chain[tx["txId"]] = [{s2rh(tx["txId"])}, set()]

        for q in tx["vIn"]:
            op = b"%s%s" % (s2rh(tx["vIn"][q]["txId"]), int_to_bytes(tx["vIn"][q]["vOut"]))
            try:
                last_dep[op[:32]].add(tx["vIn"][q]["vOut"])
            except:
                last_dep[op[:32]] = {tx["vIn"][q]["vOut"]}
            dep_chain[tx["txId"]][-1].add(op[:32])
            us_pointers_inputs.append(op[:32])
            try:
                last_dep_map[op[:32]].add(tx["txId"])
            except:
                last_dep_map[op[:32]] = {tx["txId"]}

            conflict_outpoint_map[op] = tx["txId"]
            conflict_outpoint.append(op)
            conflict_outpoints_accum.add(op)

        conflict_outpoints_chain[tx["txId"]] = [conflict_outpoint]

        us_pointers.append(s2rh(tx["txId"]))



    # get info about inputs
    if  mode=="verbose" and us_pointers:
        o_pointers = us_pointers
        async with app["db_pool"].acquire() as conn:
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

        # get information about spent output coins

        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT   outpoint,"
                                    "         input_index,"
                                    "       tx_id, "
                                    "       out_tx_id "
                                    "FROM connector_unconfirmed_stxo "
                                    "WHERE out_tx_id = ANY($1);", o_pointers)
        spent_outs_map = dict()
        for r in rows:
            t = rh2s(r["out_tx_id"])
            i = bytes_to_int(r["outpoint"][32:])
            try:
                spent_outs_map[t][i] = {"txId": rh2s(r["tx_id"]), "vIn": r["input_index"]}
            except:
                spent_outs_map[t] = {i: {"txId": rh2s(r["tx_id"]), "vIn": r["input_index"]}}

        for t in range(len(tx_list)):

            tx_list[t]["inputsAmount"] = 0
            for i in tx_list[t]["vIn"]:
                d = us_pointers[(tx_list[t]["txId"], i)]
                tx_list[t]["vIn"][i]["type"] = SCRIPT_N_TYPES[d["address"][0]]
                tx_list[t]["vIn"][i]["amount"] = d["amount"]
                tx_list[t]["inputsAmount"] += d["amount"]
                try:
                    pointer = tx_id_map_pointer[tx_list[t]["vIn"][i]["txId"]]
                    tx_list[t]["vIn"][i]["blockHeight"] = pointer >> 39
                    tx_list[t]["vIn"][i]["confirmations"] = app["last_block"] - (pointer >> 39) + 1
                except:
                    tx_list[t]["vIn"][i]["blockHeight"] = None
                    tx_list[t]["vIn"][i]["confirmations"] = None

                if d["address"][0] in (0, 1, 2, 5, 6):
                    script_hash = True if d["address"][0] in (1, 6) else False
                    witness_version = None if d["address"][0] < 5 else 0
                    try:
                        if d["address"][0] == 2:
                            ad = b"\x02" + parse_script(d["address"][1:])["addressHash"]
                        else:
                            ad = d["address"]
                        tx_list[t]["vIn"][i]["address"] = hash_to_address(ad[1:],
                                                                    testnet=app["testnet"],
                                                                    script_hash=script_hash,
                                                                    witness_version=witness_version)

                        tx_list[t]["vIn"][i]["scriptPubKey"] = address_to_script(tx_list[t]["vIn"][i]["address"], hex=1)
                    except:
                        print(tx_list[t]["txId"])
                        print("??", d["address"].hex())
                        raise

                elif tx_list["address"][0] == 2:
                    tx_list[t]["vIn"][i]["address"] = script_to_address(d["address"][1:], testnet=app["testnet"])
                    tx_list[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                else:
                    tx_list[t]["vIn"][i]["scriptPubKey"] = d["address"][1:].hex()
                tx_list[t]["vIn"][i]["scriptPubKeyOpcodes"] = decode_script(tx_list[t]["vIn"][i]["scriptPubKey"])
                tx_list[t]["vIn"][i]["scriptPubKeyAsm"] = decode_script(tx_list[t]["vIn"][i]["scriptPubKey"], 1)


            for i in tx_list[t]["vOut"]:
                tx_list[t]["vOut"][i]['spent'] = []
            try:
                s_outs = spent_outs_map[tx_list[t]["txId"]]
                for s in s_outs:
                    tx_list[t]["vOut"][s]['spent'].append(s_outs[s])
            except:
                pass



    # find out mainnet competitors and invalid chains
    last_pointer_map = dict()
    while last_dep:
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT  tx_id, pointer FROM transaction where tx_id = ANY($1)",
                                    last_dep.keys())
        tmp_lp = dict()
        for row in rows:
            tl = last_dep_map[row["tx_id"]]
            for t in tl:
                try:
                    v_outs =  last_dep[row["tx_id"]]
                    dep_chain[t][-1].remove(row["tx_id"])
                    try:
                        tmp_lp[t][row["pointer"]] = {"txId": row["tx_id"], "vOuts": v_outs}
                    except:
                        tmp_lp[t] = {row["pointer"]: {"txId": row["tx_id"], "vOuts": v_outs}}

                except:
                    pass

        for t in tmp_lp:
            last_pointer_map[t] = tmp_lp[t]
            if not dep_chain[t][-1]:
                dep_chain[t].pop()

        # go deeper
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT connector_unconfirmed_stxo.tx_id,"
                                    "       connector_unconfirmed_stxo.out_tx_id,"
                                    "       outpoint "
                                    "from connector_unconfirmed_stxo "
                                    "WHERE tx_id = ANY($1)", last_dep.keys())
        last_dep = dict()
        new_last_dep_map = dict()
        new_dep_chain = dict()
        conflict_outpoints = deque()
        tmp_co = dict()
        for row in rows:
            tl = last_dep_map[row["tx_id"]]
            for t in tl:
                try:
                    new_last_dep_map[row["out_tx_id"]].add(t)
                except:
                    new_last_dep_map[row["out_tx_id"]] = {t}

                o = row["outpoint"][:32]
                i = bytes_to_int(row["outpoint"][32:])
                try:
                    new_dep_chain[t].add(o)
                except:
                    new_dep_chain[t] = {o}

                try:
                    last_dep[row["out_tx_id"]].add(i)
                except:
                    last_dep[row["out_tx_id"]] = {i}

                conflict_outpoints.append(row["outpoint"])
                conflict_outpoints_accum.add(row["outpoint"])
                try:
                    tmp_co[t].append(row["outpoint"])
                except:
                    tmp_co[t] = [row["outpoint"]]

        for t in tmp_co:
            conflict_outpoints_chain[t].append(tmp_co[t])
            dep_chain[t].append(new_dep_chain[t])
        last_dep_map = new_last_dep_map

    # check invalid transactions competitors
    async with app["db_pool"].acquire() as conn:
        rows = await conn.fetch("SELECT connector_unconfirmed_stxo.tx_id,"
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
                                 "WHERE outpoint = ANY($1)", conflict_outpoints_accum)

    conflict_outpoints_map = dict()
    for row in rows:
        try:
            conflict_outpoints_map[row["outpoint"]].append(row)
        except:
            conflict_outpoints_map[row["outpoint"]] = [row]

    # convert last pointers to coins
    o_pointers = set()
    pointer_map_coin = dict()
    pointer_map_competitor = dict()

    for t in last_pointer_map:
        for pointer in last_pointer_map[t]:
            for v_out in last_pointer_map[t][pointer]["vOuts"]:
                pointer_map_coin[pointer + (1 << 19) + v_out] = {"txId": rh2s(last_pointer_map[t][pointer]["txId"]),
                                                                 "vOut": v_out,
                                                                 "block": pointer >> 39}
                o_pointers.add(pointer + (1 << 19) + v_out)
    if o_pointers:
        async with app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT transaction.tx_id, stxo.s_pointer, stxo.pointer  "
                                    "FROM stxo "
                                    "JOIN transaction on transaction.pointer = (stxo.s_pointer >> 18)<<18 "
                                    "WHERE stxo.pointer = ANY($1);", o_pointers)

        for row in rows:
            pointer_map_competitor[row["pointer"]] = {"txId": rh2s(row["tx_id"]),
                                                      "vIn":row["s_pointer"] &  524287,
                                                      "block": row["s_pointer"] >> 39}


    for i in range(len(tx_list)):
        t = tx_list[i]["txId"]
        m_conflict = []
        if t not in last_pointer_map:
            continue
        for pointer in last_pointer_map[t]:
            for v_out in last_pointer_map[t][pointer]["vOuts"]:
                c_pointer = pointer + (1 << 19) + v_out
                dbs = pointer_map_coin[c_pointer]
                m_conflict.append({"outpoint": dbs})

        tx_list[i]["conflict"]["blockchain"] = m_conflict
        tx_list[i]["conflict"]["unconfirmedChain"] = [[rh2s(y) for y in l] for l in dep_chain[t]]

        i_conflict = []
        atx = {s2rh(t)}
        for outpoints in conflict_outpoints_chain[t]:
            [atx.add(out[:32]) for out in outpoints]
            m = []
            for out in outpoints:
                rows = conflict_outpoints_map[out]
                for row in rows:
                    if row["tx_id"] not in atx:
                        m.append({"doublespend": {"txId": rh2s(row["outpoint"][:32]),
                                                               "vOut": bytes_to_int(row["outpoint"][32:])},
                                               "competitor": {"txId": rh2s(row["tx_id"]),
                                                              "vIn":row["input_index"],
                                                              "size": row["size"],
                                                              "bSize": row["b_size"],
                                                              "feeRate": round(row["feerate"], 2),
                                                              "fee": row["fee"],
                                                              "rbf": bool(row["rbf"]),
                                                              "segwit": bool(row["segwit"])
                                                              }})
            i_conflict.append(m)

        tx_list[i]["conflict"]["txCompetitors"] = i_conflict
        tx_list[i]["valid"] = True
        if mode == "brief":
            del tx_list[i]["vIn"]
            del tx_list[i]["vOut"]
        del tx_list[i]["format"]
        del tx_list[i]["testnet"]
        del tx_list[i]["blockTime"]
        del tx_list[i]["rawTx"]
        del tx_list[i]["blockHash"]
        del tx_list[i]["confirmations"]
        del tx_list[i]["blockIndex"]






    return {"data": {"page": page,
                     "limit": limit,
                     "pages": pages,
                     "count": count,
                     "fromTimestamp":from_timestamp,
                     "list": tx_list},
            "time": round(time.time() - qt, 4)}

async def mempool_state(app):
    q = time.time()


    async with app["db_pool"].acquire() as conn:
        row = await conn.fetchrow("SELECT inputs, outputs, transactions from mempool_analytica  "
                               " order by minute desc limit 1")
        if row is not None:
            inputs = json.loads(row["inputs"])
            outputs = json.loads(row["outputs"])
            transactions = json.loads(row["transactions"])
        else:
            outputs = {"count": 0,
                       "amount": {"max": {"value": None,
                                          "txId": None},
                                  "min": {"value": None,
                                          "txId": None},
                                  "total": 0
                                  },
                       "typeMap": {},
                       "amountMap": {}}
            inputs = {"count": 0,
                      "amount": {"max": {"value": None,
                                         "txId": None},
                                 "min": {"value": None,
                                         "txId": None},
                                 "total": 0
                                 },
                      "typeMap": {},
                      "amountMap": {},
                      "ageMap": {}}

            transactions = {"fee": {"max": {"value": None,
                                            "txId": None},
                                    "min": {"value": None,
                                            "txId": None},
                                    "total": 0
                                    },
                            "size": {"max": {"value": None,
                                             "txId": None},
                                     "min": {"value": None,
                                             "txId": None},
                                     "total": 0
                                     },
                            "vSize": {"max": {"value": None,
                                              "txId": None},
                                      "min": {"value": None,
                                              "txId": None},
                                      "total": 0
                                      },
                            "amount": {"max": {"value": None,
                                               "txId": None},
                                       "min": {"value": None,
                                               "txId": None},
                                       "total": 0
                                       },
                            "feeRate": {"max": {"value": None,
                                                "txId": None},
                                        "min": {"value": None,
                                                "txId": None},
                                        "best": 1,
                                        "best4h": 1,
                                        "bestHourly": 1
                                        },
                            "segwitCount": 0,
                            "rbfCount": 0,
                            "doublespend": {"count": 0, "size": 0, "vSize": 0, "amount": 0},
                            "doublespendChilds": {"count": 0, "size": 0, "vSize": 0, "amount": 0},
                            "feeRateMap": {},
                            "count": 0}

    return {"data": {"inputs": inputs,
                     "outputs": outputs,
                     "transactions": transactions},
            "time": round(time.time() - q, 4)}


async def fee(app):
    q = time.time()


    async with app["db_pool"].acquire() as conn:
        row = await conn.fetchrow("SELECT transactions from mempool_analytica  "
                               " order by minute desc limit 1")
        fee = json.loads(row["transactions"])["feeRate"]
        del fee["min"]
        del fee["max"]

    return {"data": fee,
            "time": round(time.time() - q, 4)}
