from aiohttp import web
import traceback
import json
from pybtc import s2rh
from model import blockchain_addresses_stat
from model import block_addresses_stat
from model import address_list_state
from pyltc import address_type
from pyltc import address_net_type
from pyltc import address_to_hash
from pyltc import bytes_needed
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import PARAMETER_ERROR
from utils import JSON_DECODE_ERROR
from utils import INVALID_BLOCK_POINTER
import math

async def get_address_state_by_list(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    addresses = dict()
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    parameters = request.rel_url.query

    type = None
    try:
        if parameters["type"] == "P2PKH":
            type = 0
        if parameters["type"] == "PUBKEY":
            type = 2
    except:
        pass

    try:
        try:
            await request.post()
            data = await request.json()
            if len(data) > 50:
                raise APIException(PARAMETER_ERROR, "only 50 addresses allowed")
            for address in data:
                origin = address
                addr_type = address_type(address, num=True)
                if addr_type in (0, 1, 5, 6):
                    address_net = address_net_type(address)
                    if address_net == "testnet" and not request.app["testnet"]:
                        raise APIException(PARAMETER_ERROR, "testnet address is invalid for mainnet")
                    if address_net == "mainnet" and request.app["testnet"]:
                        raise APIException(PARAMETER_ERROR, "mainnet address is invalid for testnet")
                    try:
                        address = b"".join((bytes([addr_type]), address_to_hash(address, hex=False)))
                    except:
                        raise APIException(PARAMETER_ERROR, "invalid address")
                else:
                    try:
                        address = bytes_needed(address)
                    except:
                        raise APIException(PARAMETER_ERROR, "invalid address")
                addresses[address] = origin
        except:
            raise APIException(JSON_DECODE_ERROR, "invalid transaction pointers list")

        response = await address_list_state(addresses, type, request.app)

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

async def get_block_addresses_stat(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    try:
        pointer = request.match_info['pointer']
        if pointer != "last":
            if len(pointer) < 12:
                try:
                    pointer = int(pointer)
                except:
                    raise APIException(INVALID_BLOCK_POINTER, "invalid block pointer")
            elif len(pointer) == 64:
                try:
                    pointer = s2rh(pointer)
                except:
                    raise APIException(INVALID_BLOCK_POINTER, "invalid block pointer")
            else:
                raise APIException(INVALID_BLOCK_POINTER, "invalid block pointer")
        response = await block_addresses_stat(pointer, request.app)
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

async def get_blockchain_addresses_stat(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    try:
        pointer = request.match_info['pointer']
        if pointer != "last":
            if len(pointer) < 12:
                try:
                    pointer = int(pointer)
                except:
                    raise APIException(INVALID_BLOCK_POINTER, "invalid block pointer")
            elif len(pointer) == 64:
                try:
                    pointer = s2rh(pointer)
                except:
                    raise APIException(INVALID_BLOCK_POINTER, "invalid block pointer")
            else:
                raise APIException(INVALID_BLOCK_POINTER, "invalid block pointer")
        response = await blockchain_addresses_stat(pointer, request.app)
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