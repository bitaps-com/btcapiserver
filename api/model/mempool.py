from pybtc import *
from utils import *
from pybtc import rh2s
import time


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

    dep_chain = dict()
    conflict_outpoints_chain = dict()

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

        conflict_outpoint = deque()
        for q in tx["vIn"]:
            tx_set.add(s2rh(tx["vIn"][q]["txId"]))
            op = b"%s%s" % (s2rh(tx["vIn"][q]["txId"]), int_to_bytes(tx["vIn"][q]["vOut"]))
            conflict_outpoint.append(op)
        dep_chain[tx["txId"]] = [[tx["txId"]]]
        conflict_outpoints_chain[tx["txId"]] = [conflict_outpoint]

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
