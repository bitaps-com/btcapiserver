from pyltc import *
from utils import *
from pyltc import rh2s, SCRIPT_N_TYPES
import time
import math

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
            outpoints = [row["outpoint"] for row in rows]
            urows = await conn.fetch("SELECT outpoint FROM connector_unconfirmed_stxo "
                                     "WHERE outpoint = ANY($1);", outpoints)
            outpoints = set([row["outpoint"] for row in urows])
            for row in rows:
                if row["outpoint"] not in outpoints:
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
            outpoints = [row["outpoint"] for row in rows]
            urows = await conn.fetch("SELECT outpoint FROM connector_unconfirmed_stxo "
                                     "WHERE outpoint = ANY($1);", outpoints)
            outpoints = set([row["outpoint"] for row in urows])
        for row in rows:
            if row["outpoint"] not in outpoints:
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

async def address_state_extended(address, app, block_height = -1):
    q = time.time()
    h = None
    balance = 0
    rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp = (0, 0, 0, None, None, None,
                                                           0, 0, 0, None, None, None)
    ltp = None
    type = SCRIPT_N_TYPES[address[0]]
    tx_map = dict()
    pdc, psa, pc, pra, psc, prc = 0, 0, 0, 0, 0, 0
    stxo, utxo, ustxo, uutxo = [], [], [], []

    async with app["db_pool"].acquire() as conn:

        if address[0] == 2:

            script = await conn.fetchval("SELECT script from connector_p2pk_map "
                                         "WHERE address = $1 LIMIT 1;", address[1:])
            if script is None:
                script = await conn.fetchval("SELECT script from connector_unconfirmed_p2pk_map "
                                             "WHERE address = $1 LIMIT 1;", address[1:])
            if script is not None:
                address = b"\x02" + script
            else:
                address = None


        if address is not None:
            if app["address_state"]:
                h = await conn.fetchval("SELECT height FROM  blockchian_address_stat  order by height desc LIMIT 1;")
                r = await conn.fetchrow("SELECT data  FROM  address WHERE address = $1 LIMIT 1;", address)
                if r is not None:
                    rc, ra, c, frp, lra, lrp, sc, sa, cd, fsp, lsa, lsp = deserialize_address_data(r["data"])

                    balance = ra - sa
                    if frp:
                        frp =frp << 20
                    if lrp:
                        lrp =lrp << 20
                    if fsp:
                        fsp =fsp << 20
                    if lsp:
                        lsp =lsp << 20
                    if lsp is not None and lrp is not None:
                        ltp = lsp if lsp > lrp else lrp
                    elif lsp is None:
                        if lrp is not None:
                            ltp = lrp
                    else:
                        if lrp is None:
                            ltp = lsp


            if h is not None:
                block_height = h

            stxo = await conn.fetch("SELECT pointer, s_pointer, amount FROM "
                                    "stxo WHERE address = $1 and s_pointer >= $2 ", address, (block_height + 1)<< 39)

            utxo = await conn.fetch("SELECT pointer, amount FROM "
                                    "connector_utxo WHERE address = $1 and pointer >= $2 ",
                                    address, (block_height +1 )<< 39)

            ustxo = await conn.fetch("SELECT tx_id, amount FROM "
                                    "connector_unconfirmed_stxo WHERE address = $1;", address)

            uutxo = await conn.fetch("SELECT out_tx_id as tx_id, amount FROM "
                                    "connector_unconfirmed_utxo WHERE address = $1;", address)

    if lsa is None:
        lsa = 0
    if lra is None:
        lra = 0

    for row in stxo:
        sh = row["s_pointer"] >> 39
        rh = row["pointer"] >> 39
        if sh > block_height:
            cd += 1
            sa += row["amount"]
            balance -= row["amount"]
            if not fsp:
                fsp = row["s_pointer"]
            try:
                tx_map[row["s_pointer"] >> 20] -= row["amount"]
            except:
                tx_map[row["s_pointer"] >> 20] = 0 - row["amount"]


        if rh > block_height:
            ra += row["amount"]
            balance += row["amount"]
            c += 1
            if not frp:
                frp = row["pointer"]
            try:
                tx_map[row["pointer"] >> 20] += row["amount"]
            except:
                tx_map[row["pointer"] >> 20] = row["amount"]


        if row["pointer"] > row["s_pointer"]:
            ltp = row["pointer"]
        else:
            ltp = row["s_pointer"]


    for row in utxo:
        c += 1
        ra += row["amount"]
        balance += row["amount"]
        if not frp:
            frp = row["pointer"]
        try:
            tx_map[row["pointer"] >> 20] += row["amount"]
        except:
            tx_map[row["pointer"] >> 20] = row["amount"]
        if ltp is None or ltp < row["pointer"]:
            ltp = row["pointer"]

    for k in tx_map:
        if tx_map[k] < 0:
            sc += 1
            if lsa > tx_map[k]:
                lsa = tx_map[k]
                lsp = k << 20
        else:
            rc += 1
            if lra < tx_map[k]:
                lra = tx_map[k]
                lrp = k << 20

    if lsa is not None:
         lsa = abs(lsa)
    if frp  is not None:
        frp = "%s:%s" %  (frp >> 39, (frp >> 20) & 524287)

    if ltp  is not None:
        ltp = "%s:%s" %  (ltp >> 39, (ltp >> 20) & 524287)

    if fsp is not None:
        fsp = "%s:%s" %  (fsp >> 39, (fsp >> 20) & 524287)

    if lsp is not None:
        lsp = "%s:%s" %  (lsp >> 39, (lsp >> 20) & 524287)
    if lrp is not None:
        lrp = "%s:%s" %  (lrp >> 39, (lrp >> 20) & 524287)

    tx_map = dict()

    for row in ustxo:
        pdc += 1
        psa += row["amount"]

        try:
            tx_map[row["tx_id"]] -= row["amount"]
        except:
            tx_map[row["tx_id"]] = 0 - row["amount"]

    for row in uutxo:
        pc += 1
        pra += row["amount"]

        try:
            tx_map[row["tx_id"]] += row["amount"]
        except:
            tx_map[row["tx_id"]] = row["amount"]

    for k in tx_map:
        if tx_map[k] < 0:
            psc += 1
        else:
            prc += 1

    return {"data": {"balance": balance,
                     "receivedAmount": ra,
                     "receivedTxCount": rc,
                     "sentAmount": sa,
                     "sentTxCount": sc,
                     "firstReceivedTxPointer": frp,
                     "firstSentTxPointer": fsp,
                     "lastTxPointer": ltp,

                     "largestSpentTxAmount": lsa,
                     "largestSpentTxPointer": lsp,
                     "largestReceivedTxAmount": lra,
                     "largestReceivedTxPointer": lrp,

                     "receivedOutsCount": c,
                     "spentOutsCount": cd,
                     "pendingReceivedAmount": pra,
                     "pendingSentAmount": psa,
                     "pendingReceivedTxCount": prc,
                     "pendingSentTxCount": psc,
                     "pendingReceivedOutsCount": pc,
                     "pendingSpentOutsCount": pdc,
                     "type": type
                     },
            "time": round(time.time() - q, 4)}

async def address_state_extended_in_pointer(address, pointer, app):
    q = time.time()
    received_outs_count = 0
    spent_outs_count = 0

    received_amount = 0
    sent_amount = 0
    received_tx_count = 0
    sent_tx_count = 0
    balance = 0
    frp = None
    fsp = None
    ltp = None


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
                    "spentOutsCount": 0}

    qw = time.time()
    async with app["db_pool"].acquire() as conn:
        txo = await conn.fetch("SELECT pointer, s_pointer, amount FROM "
                                "stxo WHERE address = ANY($1)", address)
        ctxo = await conn.fetch("SELECT pointer, amount FROM "
                                "connector_utxo WHERE address = ANY($1);", address)
    print(time.time()-qw)
    stxo = []
    ustxo = []
    utxo = []
    p = (pointer >> 20) << 20

    for t in txo:
        if t["s_pointer"] < p:
            stxo.append(t)
        if t["pointer"] < p:
            ustxo.append(t)

    for t in ctxo:
        if t["pointer"] < p:
            utxo.append(t)

    if not stxo and not utxo and not ustxo:
        return {"data": empty_result,
                "time": round(time.time() - q, 4)}

    tx_map = dict()

    for row in ustxo:
        received_outs_count += 1
        received_amount += row["amount"]

        if not frp:
            frp = row["pointer"]

        try:
            tx_map[row["pointer"] >> 20] += row["amount"]
        except:
            tx_map[row["pointer"] >> 20] = row["amount"]
        if ltp is None or ltp < row["pointer"] >> 20:
            ltp = row["pointer"] >> 20

    for row in stxo:
        spent_outs_count += 1
        sent_amount += row["amount"]

        if not fsp:
            fsp = row["s_pointer"]

        try:
            tx_map[row["s_pointer"] >> 20] -= row["amount"]
        except:
            tx_map[row["s_pointer"] >> 20] = 0 - row["amount"]
        if ltp < row["s_pointer"] >> 20:
            ltp = row["s_pointer"] >> 20


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

        if ltp is None or ltp < row["pointer"] >> 20:
            ltp = row["pointer"] >> 20



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
    if frp is not None:
        frp = "%s:%s" %  (frp >> 39, (frp >> 20) & 524287)

    if ltp  is not None:
        ltp = "%s:%s" %  (ltp >> 19, ltp   & 524287)

    if fsp is not None:
        fsp = "%s:%s" %  (fsp >> 39, (fsp >> 20) & 524287)
    print("frp", frp)

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
                     "spentOutsCount": spent_outs_count
                     },
            "time": round(time.time() - q, 4)}


async def address_transactions(address,  type, limit, page, order, mode, timeline, app):
    q = time.time()
    qt = time.time()
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

    print("1", time.time() - qt)
    qt = time.time()

    pages = 0
    for z in a:
        s  = await address_state_extended(z, app)
        pages += s["data"]["receivedTxCount"] + s["data"]["sentTxCount"]
    pages = math.ceil(pages / limit)


    async with app["db_pool"].acquire() as conn:
        # get total transactions count to determine pages count
        print("4", time.time() - qt)
        qt = time.time()

        tx_id_list = await conn.fetch("SELECT  pointer  FROM transaction_map "
                                      "WHERE address = ANY($1) order by  pointer %s "
                                      "LIMIT $2 OFFSET $3;" % order , a, limit,  limit * (page - 1))
        l = [t["pointer"] for t in tx_id_list]

        print("4.1", time.time() - qt)
        qt = time.time()

        rows = await conn.fetch("SELECT  transaction.pointer,"
                                "        transaction.raw_transaction,"
                                "        transaction.tx_id,  "
                                "        transaction.timestamp  "
                                "FROM transaction "
                                "WHERE pointer = ANY($1) "
                                "order by  pointer %s "
                                "LIMIT $2 ;" % order ,l, limit)

        print("3", time.time() - qt)
        qt = time.time()
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
                ts[hash_to_script(d[1:], d[0], hex=True)] = {"r": 0, "s": 0, "i": 0, "o": 0}
            else:
                ts[d[1:].hex()] =  {"r": 0, "s": 0, "i": 0, "o": 0}
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
                        tx_list[i]["vIn"][k]["address"] = hash_to_address(ad[1:], testnet=app["testnet"],
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
                        target_scripts[i][ti]["s"] += tx_list[i]["vIn"][k]["amount"]
                        target_scripts[i][ti]["i"] += 1

        if not tx_list[i]["coinbase"]:
            tx_list[i]["fee"] = tx_list[i]["inputsAmount"] - tx_list[i]["amount"]
        else:
            tx_list[i]["fee"] = 0

        tx_list[i]["outputsAmount"] = tx_list[i]["amount"]

    print("2", time.time() - qt)
    qt = time.time()
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
    print("1", time.time() - qt)
    qt = time.time()

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
                        target_scripts[t][ti]["r"] += tx_list[t]["vOut"][i]["value"]
                        target_scripts[t][ti]["o"] += 1
                if "address" in  tx_list[t]["vOut"][i]:
                    tx_list[t]["outAddressCount"] += 1

            address_amount = 0
            address_received = 0
            address_outputs = 0
            address_sent = 0
            address_inputs = 0
            for ti in target_scripts[t]:
                address_amount += target_scripts[t][ti]["r"] - target_scripts[t][ti]["s"]
                address_received += target_scripts[t][ti]["r"]
                address_outputs += target_scripts[t][ti]["o"]
                address_sent += target_scripts[t][ti]["s"]
                address_inputs += target_scripts[t][ti]["i"]

            tx_list[t]["amount"] = address_amount
            tx_list[t]["addressReceived"] = address_received
            tx_list[t]["addressOuts"] = address_outputs
            tx_list[t]["addressSent"] = address_sent
            tx_list[t]["addressInputs"] = address_inputs


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

    if timeline and tx_list:
        tx_pointer= (tx_list[-1]["blockHeight"] << 39) + (tx_list[-1]["blockIndex"] << 20)
        print(tx_list[-1]["blockHeight"])
        r = await address_state_extended_in_pointer(a, tx_pointer, app)
        print(r)
        state = {"receivedAmount": r['data']["receivedAmount"],
                 "receivedTxCount": r['data']["receivedTxCount"],
                 "sentAmount": r['data']["sentAmount"],
                 "sentTxCount": r['data']["sentTxCount"],
                 "receivedOutsCount": r['data']["receivedOutsCount"],
                 "spentOutsCount": r['data']["spentOutsCount"]}

        k = len(tx_list)
        l = len(tx_list)
        while k:
            k -= 1
            if order == "desc":
                i = k
            else:
                i = l - (k + 1)
            tx = tx_list[i]
            state["receivedAmount"] += tx["addressReceived"]
            state["receivedOutsCount"] += tx["addressOuts"]
            state["sentAmount"] += tx["addressSent"]
            state["spentOutsCount"] += tx["addressInputs"]
            if  tx["addressReceived"] >= tx["addressSent"]:
                state["receivedTxCount"] += 1
            else:
                state["sentTxCount"] += 1
            tx_list[i]["timelineState"] = {"receivedAmount": state["receivedAmount"],
                                             "receivedTxCount": state["receivedTxCount"],
                                             "sentAmount": state["sentAmount"],
                                             "sentTxCount": state["sentTxCount"],
                                             "receivedOutsCount": state["receivedOutsCount"],
                                             "spentOutsCount": state["spentOutsCount"]}



    return {"data": {"page": page,
                     "limit": limit,
                     "pages": pages,
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
                if pointer is not None:
                    tx_list[i]["vIn"][k]["blockHeight"] = pointer >> 39
                    tx_list[i]["vIn"][k]["confirmations"] = app["last_block"] - (pointer >> 39) + 1
                else:
                    tx_list[i]["vIn"][k]["blockHeight"] = None
                    tx_list[i]["vIn"][k]["confirmations"] = None

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

