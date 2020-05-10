from aiohttp import web
import traceback
import json
from model import block_by_pointer
from model import block_utxo
from model import block_data_by_pointer
from model import block_transactions
from model import block_transaction_id_list
from pybtc import s2rh
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import INVALID_BLOCK_POINTER


async def get_block_last(request):
    return await get_block_by_pointer(request, 'last')

async def get_block_by_pointer(request, pointer=None):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        if pointer is None:
            pointer = request.match_info['block_pointer']
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
        response = await block_by_pointer(pointer, request.app)
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

async def get_block_utxo(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
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

    try: order = "asc" if parameters["order"] == "asc" else "desc"
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

        response = await block_utxo(pointer, limit, page, order, request.app)
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

async def get_block_data_last(request):
    return await get_block_data_by_pointer(request, 'last')

async def get_block_data_by_pointer(request, pointer=None):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    parameters = request.rel_url.query

    try:
        if parameters["statistics"] in ('1', 'True'):
            stat = True
        else:
            stat = False
    except:
        stat = False

    try:
        if pointer is None:
            pointer = request.match_info['block_pointer']
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
        response = await block_data_by_pointer(pointer, stat, request.app)
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

async def get_block_transactions(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    parameters = request.rel_url.query
    try:
        limit = int(parameters["limit"])
        if  not (limit >= 0):
            raise Exception()
    except:
        limit = 50

    try:
        mode = parameters['mode']
    except:
        mode = "brief"

    if mode not in ["brief", "verbose"]:
        mode = "brief"

    try:
        page = int(parameters["page"])
        if page <= 0: raise Exception()
    except:
        page = 1

    if limit == 0 or limit > request.app["get_block_tx_page_limit"]:
        if limit == 0:
            page = 1
        limit = request.app["get_block_tx_page_limit"]

    if limit * page > 2 ** 19 - 1:
        limit = 50
        page = 1

    try:
        order = "asc" if parameters["order"] == "asc" else "desc"
    except:
        order = "asc"

    try:
        pointer = request.match_info['block_pointer']
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

        option_raw_tx = False
        try:
            if request.rel_url.query['raw_tx'] in ('True', '1'):
                option_raw_tx = True
        except:
            pass

        response = await block_transactions(pointer, option_raw_tx, limit, page, order, mode, request.app)
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

async def get_block_transactions_list(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    parameters = request.rel_url.query
    try:
        try:
            limit = int(parameters["limit"])
            if not (limit > 0 and limit <= request.app["get_block_tx_page_limit"]):
                raise Exception()
        except:
            limit = request.app["get_block_tx_page_limit"]

        try:
            page = int(parameters["page"])
            if page <= 0:
                raise Exception()
        except:
            page = 1

        try:
            order = "asc" if parameters["order"] == "asc" else "desc"
        except:
            order = "asc"

        pointer = request.match_info['block_pointer']
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

        response = await block_transaction_id_list(pointer, limit, page, order, request.app)

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
