from aiohttp import web
import traceback
import json
from model import tx_by_pointer_opt_tx
from model import tx_hash_by_pointer
from model import calculate_tx_merkle_proof_by_pointer
from model import tx_merkle_proof_by_pointer
from pyltc import s2rh
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import PARAMETER_ERROR
import math

async def get_transaction_by_pointer(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
    pointer = request.match_info['tx_pointer']

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
        if len(pointer) == 64:
            try:
                pointer = s2rh(pointer)
            except:
                raise APIException(PARAMETER_ERROR, "invalid transaction hash")
        else:
            try:
                k = pointer.split(":")
                b = int(k[0])
                t = int(k[1])
                if b < 0: raise Exception()
                if t < 0: raise Exception()
                pointer = (b << 39) + (t << 20)
            except:
                raise APIException(PARAMETER_ERROR, "invalid transaction pointer")

        response = await tx_by_pointer_opt_tx(pointer, option_raw_tx, request.app)
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

async def get_transaction_hash_by_pointer(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
    pointer = request.match_info['tx_blockchain_pointer']
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    try:
        try:
            k = pointer.split(":")
            b = int(k[0])
            t = int(k[1])
            if b < 0:
                raise Exception()
            if t < 0:
                raise Exception()
            pointer = (b << 39) + (t << 20)
        except:
            raise APIException(PARAMETER_ERROR, "invalid transaction pointer")

        response = await tx_hash_by_pointer(pointer, request.app)

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

async def calculate_transaction_merkle_proof(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
    pointer = request.match_info['tx_pointer']

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    try:
        if len(pointer) == 64:
            try:
                pointer = s2rh(pointer)
            except:
                raise APIException(PARAMETER_ERROR, "invalid transaction hash")
        else:
            try:
                k = pointer.split(":")
                b = int(k[0])
                t = int(k[1])
                if b < 0: raise Exception()
                if t < 0: raise Exception()
                pointer = (b << 39) + (t << 20)
            except:
                raise APIException(PARAMETER_ERROR, "invalid transaction pointer")


        response = await calculate_tx_merkle_proof_by_pointer(pointer, request.app)
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

async def get_transaction_merkle_proof(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
    pointer = request.match_info['tx_pointer']

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        if len(pointer) == 64:
            try:
                pointer = s2rh(pointer)
            except:
                raise APIException(PARAMETER_ERROR, "invalid transaction hash")
        else:
            try:
                k = pointer.split(":")
                b = int(k[0])
                t = int(k[1])
                if b < 0:
                    raise Exception()
                if t < 0:
                    raise Exception()
                pointer = (b << 39) + (t << 20)
            except:
                raise APIException(PARAMETER_ERROR, "invalid transaction pointer")

        response = await tx_merkle_proof_by_pointer(pointer, request.app)
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