import math
import time
from aiohttp import web
import traceback
from model import *
from pybtc import *
from utils import *


async def get_block_last(request):
    return await get_block_by_pointer(request, 'last')

async def get_block_by_pointer(request, pointer=None):
    log = request.app["log"]
    # parameters = request.rel_url.query
    # if "transactions" in parameters:
    #     tl = parameters["transactions"]
    #     if tl == 'True' or tl == '1':
    #         tx_list = True
    # if "block_statistic" in parameters:
    #     tl = parameters["block_statistic"]
    #     if tl == 'True' or tl == '1':
    #         block_stat = True
    # if "blockchain_state" in parameters:
    #     tl = parameters["blockchain_state"]
    #     if tl == 'True' or tl == '1':
    #         blockchain_stat = True

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        if pointer is None:
            pointer = request.match_info['block_pointer']
            if len(pointer) < 8:
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
        log.debug("get block %s" % (pointer if not isinstance(pointer, bytes) else pointer.hex()))
        response = await block_by_pointer(pointer, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        log.error(str(traceback.format_exc()))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


async def get_block_headers(request):
    log = request.app["log"]


    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        pointer = request.match_info['block_pointer']
        if len(pointer) < 8:
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

        try:
           count = request.match_info['count']
        except:
            count = 2000
        try:
            count = int(count)
            if count < 1 and count > 2000:
                count = 2000
        except:
            raise APIException(PARAMETER_ERROR, "invalid count parameter")


        log.debug("get block headers from %s [%s]" % (pointer if not isinstance(pointer, bytes) else rh2s(pointer),
                                                      count))
        response = await block_headers(pointer, count, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        log.error(str(traceback.format_exc()))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)

async def get_block_utxo(request):
    log = request.app["log"]
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    parameters = request.rel_url.query

    try:
        limit = int(parameters["limit"])
        if  not (limit > 0 and limit <= request.app["get_block_utxo_page_limit"]): raise Exception()
    except: limit = request.app["get_block_utxo_page_limit"]

    try:
        page = int(parameters["page"])
        if page <= 0: raise Exception()
    except: page = 1

    try: order = "asc" if parameters["order"] == "desc" else "desc"
    except: order = "asc"

    try:
        pointer = request.match_info['block_pointer']
        if len(pointer) < 8:
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

        log.debug("get block utxo %s " % (pointer if not isinstance(pointer, bytes) else rh2s(pointer),))
        response = await block_utxo(pointer, limit, page, order, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        log.error(str(traceback.format_exc()))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)

async def get_block_transactions(request):
    log = request.app["log"]
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    parameters = request.rel_url.query

    try:
        limit = int(parameters["limit"])
        if  not (limit > 0 and limit <= request.app["get_block_tx_page_limit"]): raise Exception()
    except: limit = request.app["get_block_tx_page_limit"]

    try:
        page = int(parameters["page"])
        if page <= 0: raise Exception()
    except: page = 1

    try: order = "asc" if parameters["order"] == "desc" else "desc"
    except: order = "asc"

    try:
        pointer = request.match_info['block_pointer']
        if len(pointer) < 8:
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

        log.debug("get block utxo %s " % (pointer if not isinstance(pointer, bytes) else rh2s(pointer),))
        response = await block_transactions(pointer, limit, page, order, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        log.error(str(traceback.format_exc()))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


async def get_transaction_by_pointer(request):
    log = request.app["log"]
    pointer = request.match_info['tx_pointer']

    log.debug("get transaction %s" % pointer)
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    # try:
    #     if request.rel_url.query['raw_tx'] in ('True','1'):
    #         option_raw_tx = True
    # except:
    #     option_raw_tx = False
    #
    # try:
    #     if request.rel_url.query['merkle_proof'] in ('True','1'):
    #         option_merkle_proof = True
    # except:
    #     option_merkle_proof = False
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

        response = await tx_by_pointer(pointer, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        log.error(str(traceback.format_exc()))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


async def get_address_state(request):
    log = request.app["log"]
    address = request.match_info['address']
    addr_type = address_type(address, num=True)
    log.info("get address %s" % address)


    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    try:
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

        response = await address_state(address, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        log.error(str(traceback.format_exc()))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


async def get_address_confirmed_utxo(request):
    log = request.app["log"]
    address = request.match_info['address']
    addr_type = address_type(address, num=True)
    log.info("get address %s" % address)


    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    try:
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

        response = await address_confirmed_utxo(address, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        log.error(str(traceback.format_exc()))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)




async def about(request):
    log = request.app["log"]
    log.info(request.url)
    log.info('404')
    resp = {"about": "BTCAPI server",
            "error": "Endpoint not found"}
    return web.json_response(resp, dumps=json.dumps, status = 404)
