from aiohttp import web
import traceback
import json
from model import tx_by_pointers_opt_tx
from model import tx_hash_by_pointers
from pybtc import s2rh
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import PARAMETER_ERROR
from utils import JSON_DECODE_ERROR


async def get_transaction_by_pointer_list(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    pointers = list()
    hashes = list()
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    option_raw_tx = False
    try:
        if request.rel_url.query['raw_tx'] in ('True','1'):
            option_raw_tx = True
    except:
        pass

    try:
        try:
            await request.post()
            data = await request.json()
            if len(data) > 100:
                raise APIException(PARAMETER_ERROR, "only 100 transactions per request limit")
            for pointer in data:
                if len(pointer) == 64:
                    try:
                        pointer = s2rh(pointer)
                        hashes.append(pointer)
                    except:
                        raise APIException(PARAMETER_ERROR, "invalid transaction hash %s" % pointer)
                else:
                    try:
                        k = pointer.split(":")
                        b = int(k[0])
                        t = int(k[1])
                        if b < 0: raise Exception()
                        if t < 0: raise Exception()
                        pointer = (b << 39) + (t << 20)
                        pointers.append(pointer)
                    except:
                        raise APIException(PARAMETER_ERROR, "invalid transaction pointer %s" % pointer)

        except:
            raise APIException(JSON_DECODE_ERROR, "invalid transaction pointers list")

        response = await tx_by_pointers_opt_tx(pointers, hashes, option_raw_tx, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details}
    except Exception as err:
        if request.app["debug"]:
            log.error(str(traceback.format_exc()))
        else:
            log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)

async def get_transaction_hash_by_pointers(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    pointers = list()
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        try:
            await request.post()
            data = await request.json()
            if len(data) > 500:
                raise APIException(PARAMETER_ERROR, "only 1000 transactions per request limit")
            for pointer in data:
                k = pointer.split(":")
                b = int(k[0])
                t = int(k[1])
                if b < 0:
                    raise Exception()
                if t < 0:
                    raise Exception()

                pointers.append((b << 39) + (t << 20))
        except Exception:
            raise APIException(JSON_DECODE_ERROR, "invalid transaction pointers list")

        response = await tx_hash_by_pointers(pointers, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details}
    except Exception as err:
        if request.app["debug"]:
            log.error(str(traceback.format_exc()))
        else:
            log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)

