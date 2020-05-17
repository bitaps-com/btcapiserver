from aiohttp import web
import traceback
import json
from model import mempool_transactions
from model import mempool_state
from model import fee
from model import invalid_transactions
from model import mempool_doublespend
from utils import APIException
from utils import INTERNAL_SERVER_ERROR


async def get_mempool_transactions(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    parameters = request.rel_url.query

    try:
        limit = int(parameters["limit"])
        if  not (limit > 0 and limit <= 50):
            limit = 50
    except:
        limit = 50

    mode = "brief"
    try:
        if parameters["mode"] == "verbose":
            mode = "verbose"
    except:
        pass


    try:
        from_timestamp = int(parameters["from_timestamp"])
    except:
        from_timestamp = 0

    try:
        page = int(parameters["page"])
        if page <= 0: raise Exception()
    except: page = 1

    try:
        order = "asc" if parameters["order"] == "asc" else "desc"
    except:
        order = "desc"

    try:
        response = await mempool_transactions(limit, page, order, from_timestamp,  mode, request.app)
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
        return web.json_response(response, dumps=json.dumps, status=status)

async def get_mempool_state(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        response = await mempool_state(request.app)
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
        return web.json_response(response, dumps=json.dumps, status=status)

async def get_fee(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        response = await fee(request.app)
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
        return web.json_response(response, dumps=json.dumps, status=status)



async def get_mempool_invalid_transactions(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    parameters = request.rel_url.query

    try:
        limit = int(parameters["limit"])
        if not (limit > 0 and limit <= 50):
            limit = 50
    except:
        limit = 50

    try:
        from_timestamp = int(parameters["from_timestamp"])
    except:
        from_timestamp = 0

    try:
        page = int(parameters["page"])
        if page <= 0: raise Exception()
    except:
        page = 1

    mode = "brief"
    try:
        if parameters["mode"] == "verbose":
            mode = "verbose"
    except:
        pass

    try:
        order = "asc" if parameters["order"] == "asc" else "desc"
    except:
        order = "desc"

    try:
        response = await invalid_transactions(limit, page, order, from_timestamp, mode, request.app)
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
        return web.json_response(response, dumps=json.dumps, status=status)

async def get_mempool_doublespend_transactions(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    parameters = request.rel_url.query

    try:
        limit = int(parameters["limit"])
        if  not (limit > 0 and limit <= 50):
            limit = 50
    except:
        limit = 50

    try:
        from_timestamp = int(parameters["from_timestamp"])
    except:
        from_timestamp = 0


    try:
        page = int(parameters["page"])
        if page <= 0: raise Exception()
    except: page = 1

    try:
        order = "asc" if parameters["order"] == "asc" else "desc"
    except:
        order = "desc"

    mode = "brief"
    try:
        if parameters["mode"] == "verbose":
            mode = "verbose"
    except:
        pass

    dbs_type = None
    try:
        if parameters["type"] == "child":
            dbs_type = 1
        elif parameters["type"] == "direct":
            dbs_type = 0
    except:
        pass


    try:
        response = await mempool_doublespend(limit, page, order, from_timestamp, dbs_type, mode, request.app)
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
        return web.json_response(response, dumps=json.dumps, status=status)
