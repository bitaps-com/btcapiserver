from pybtc import *
from utils import APIException
from utils import PARAMETER_ERROR
from utils import NOT_FOUND
import time
import base64


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
