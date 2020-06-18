import time
import json
from pybtc import rh2s, script_to_address, hash_to_address, address_to_script, decode_script, SCRIPT_N_TYPES
from utils import APIException
from utils import NOT_FOUND
import traceback


async def outpoints_info(outpoints, app):
    q = time.time()
    o = [i[2] for i in outpoints]
    t = [i[0] for i in outpoints]
    out_map_pointer = dict()
    async with app["db_pool"].acquire() as conn:
        utxo = await conn.fetch("SELECT outpoint, pointer, address, amount "
                                "FROM connector_utxo "
                                "WHERE outpoint = ANY($1) LIMIT $2;",
                                o, len(o))
        uutxo = await conn.fetch("SELECT outpoint, address, amount "
                                "FROM connector_unconfirmed_utxo "
                                "WHERE outpoint = ANY($1) LIMIT $2;",
                                o, len(o))
        ustxo = await conn.fetch("SELECT outpoint, tx_id "
                                "FROM connector_unconfirmed_stxo "
                                "WHERE outpoint = ANY($1);", o)
        stxo = []

        if len(utxo) + len(uutxo) < len(o):
            tx_pointers = await conn.fetch("SELECT pointer, tx_id FROM transaction "
                                           "WHERE tx_id = ANY($1) LIMIT $2;",
                                           t, len(t))
            print(tx_pointers)
            tx_map_pointer = dict()
            for row in tx_pointers:
                tx_map_pointer[row["tx_id"]] = row["pointer"]
            s = set()

            for i in outpoints:
                try:
                    s.add(tx_map_pointer[i[0]] +(1 << 19)+ i[1])
                    out_map_pointer[tx_map_pointer[i[0]]+ (1 << 19) + i[1]] = i[2]
                except:
                    pass
            stxo = await conn.fetch("SELECT stxo.pointer, stxo.address, stxo.amount, transaction.tx_id "
                                    "FROM stxo "
                                    "JOIN transaction "
                                    "ON transaction.pointer = (stxo.s_pointer >> 18)<<18"
                                    "WHERE stxo.pointer = ANY($1) LIMIT $2",
                                    s, len(s))
            print(stxo)
    outpoints_map = dict()

    for row in utxo:
        outpoints_map[row["outpoint"]] = [row["pointer"] >> 39, row["address"], row["amount"], []]

    for row in uutxo:
        outpoints_map[row["outpoint"]] = [None, row["address"], row["amount"], []]

    for row in ustxo:
        try:
            outpoints_map[row["outpoint"]][3].append(rh2s(row["tx_id"]))
        except:
            pass

    for row in stxo:
        outpoint = out_map_pointer[row["pointer"]]
        outpoints_map[outpoint] = [row["pointer"] >> 39, row["address"], row["amount"], [rh2s(row["tx_id"])]]

    result = []
    for i in outpoints:
        outpoint = "%s:%s" % (rh2s(i[0]), i[1])
        try:
            h, address, amount, s = outpoints_map[i[2]]
            r = dict()
            if address[0] in (0, 1, 5, 6):
                script_hash = True if address[0] in (1, 6) else False
                witness_version = None if address[0] < 5 else 0
                try:
                    r["address"] = hash_to_address(address[1:], testnet=app["testnet"],
                                              script_hash=script_hash, witness_version=witness_version)
                    r["script"] = address_to_script(r["address"], hex=1)
                except:
                    raise
            elif address[0] == 2:
                r["address"] = script_to_address(address[1:], testnet=app["testnet"])
                r["script"] = r["address"][1:].hex()
            else:
                r["script"] = r["address"][1:].hex()
            r["scriptOpcodes"] = decode_script(r["script"])
            r["scriptAsm"] = decode_script(r["script"], 1)

            r["height"] = h
            r["spent"] = s
            r["type"] = SCRIPT_N_TYPES[address[0]]
            r["amount"] = amount
            result.append({outpoint: r})
        except :
            result.append({outpoint: None})



    return {"data": result,
            "time": round(time.time() - q, 4)}

