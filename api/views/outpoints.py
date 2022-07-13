from aiohttp import web
import traceback
import json
from model import outpoints_info
from pybtc import s2rh, int_to_bytes
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import PARAMETER_ERROR
from utils import JSON_DECODE_ERROR


async def get_outpoints_info(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    addresses = dict()
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    try:
        try:
            await request.post()
            data = await request.json()
            outpoints = []
            if len(data) > 100:
                raise APIException(PARAMETER_ERROR, "only 100 outpoints allowed")
            """
            connector_utxo
            connector_unconfirmed_utxo
            connector_unconfirmed_stxo
            
            """
            for outpoint in data:
                try:
                    o, a = outpoint.split(":")
                    tx_id = s2rh(o)
                    if len(tx_id) != 32:
                        raise Exception()
                    outpoints.append((tx_id, int(a),
                                      b"".join((tx_id, int_to_bytes(int(a))))))
                except:
                    raise APIException(PARAMETER_ERROR, "invalid outpoint %s" % outpoint)


        except:
            raise APIException(JSON_DECODE_ERROR, "invalid outpoints list")

        response = await outpoints_info(outpoints, request.app)

        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]:
            log.error(str(traceback.format_exc()))
        else:
            log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)
