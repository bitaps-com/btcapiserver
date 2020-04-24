from aiohttp import web
import traceback
import json
import zlib
import asyncio

from pybtc import c_int_to_int
from pybtc import c_int_len
from pybtc import decode_gcs
from pybtc import map_into_range
from pybtc import siphash


async def test_filter(request):

    try:
        async with request.app["db_pool"].acquire() as conn:
            row = await conn.fetchrow("SELECT height, filter  "
                                          "FROM block_filters_checkpoints "
                                       "order by height asc limit 1;" )

        N = c_int_to_int(row["filter"])
        i = c_int_len(N)
        f = zlib.decompress(row["filter"][i:])
        h = row["height"]
        f = decode_gcs(f, N, P=request.app["block_filter_bits"])
        print(">>>",len(f), h)

        # 141788

        async with request.app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT distinct  address  "
                                    "FROM transaction_map "
                                    "where  pointer < $1;" ,  10000 << 39 )

        print("affected addresses ", len(rows))
        N = request.app["block_filter_fps"]
        M = request.app["block_filter_capacity"]
        elements = [map_into_range(siphash(e["address"]), N * M) for e in rows]

        for a in elements:
            if a not in f:
                print("false negative!")
                await asyncio.sleep(0)



        # print("positive ", i)
        # print("false positive ", e2)
        # print("errors ", e)

        resp = ["ok"]

    except Exception as err:
        print(str(traceback.format_exc()))
        return web.json_response([str(err)], dumps=json.dumps, status=200)
    return web.json_response(resp, dumps=json.dumps, status = 200)

async def about(request):
    log = request.app["log"]
    log.info(request.url)
    log.info('404')
    resp = {"about": "BTCAPI server",
            "error": "Endpoint not found"}
    return web.json_response(resp, dumps=json.dumps, status = 404)
