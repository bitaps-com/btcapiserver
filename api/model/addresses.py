import time
import json
from utils import APIException
from utils import NOT_FOUND


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


async def block_addresses_stat(pointer, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if pointer == 'last':
            stmt = await conn.prepare("SELECT height,"
                                      "       addresses "
                                      "FROM block_address_stat  ORDER BY height desc LIMIT 1;")
            row = await stmt.fetchrow()
        else:
            if type(pointer) == bytes:
                stmt = await conn.prepare("SELECT height FROM blocks  WHERE hash = $1 LIMIT 1;")
                row = await stmt.fetchval(pointer)
                pointer = row


            stmt = await conn.prepare("SELECT height, addresses " 
                                      "FROM block_address_stat  WHERE height = $1 LIMIT 1;")
            row = await stmt.fetchrow(pointer)

    if row is None:
        raise APIException(NOT_FOUND, "block not found", status=404)

    block = dict()
    block["height"] = row["height"]
    block["statistics"] = json.loads(row["addresses"])

    resp = {"data": block,
            "time": round(time.time() - q, 4)}
    return resp


async def blockchain_addresses_stat(pointer, app):
    pool = app["db_pool"]
    q = time.time()
    async with pool.acquire() as conn:
        if pointer == 'last':
            stmt = await conn.prepare("SELECT height,"
                                      "       addresses "
                                      "FROM blockchian_address_stat  ORDER BY height desc LIMIT 1;")
            row = await stmt.fetchrow()
        else:
            if type(pointer) == bytes:
                stmt = await conn.prepare("SELECT height FROM blocks  WHERE hash = $1 LIMIT 1;")
                row = await stmt.fetchval(pointer)
                pointer = row


            stmt = await conn.prepare("SELECT height, addresses " 
                                      "FROM blockchian_address_stat  WHERE height = $1 LIMIT 1;")
            row = await stmt.fetchrow(pointer)

    if row is None:
        raise APIException(NOT_FOUND, "block not found", status=404)

    block = dict()
    block["height"] = row["height"]
    block["statistics"] = json.loads(row["addresses"])

    resp = {"data": block,
            "time": round(time.time() - q, 4)}
    return resp

