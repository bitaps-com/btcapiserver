from aiohttp import web
import traceback
import json
import datetime
import time
from model import last_n_blocks
from model import blocks_daily
from model import data_last_n_blocks
from model import blocks_last_n_hours
from model import data_blocks_daily
from model import blocks_data_last_n_hours
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import PARAMETER_ERROR
from utils import DECODE_ERROR


async def get_last_n_blocks(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        n = request.match_info['n']
        n = int(n)
        if n > 2016:
            n = 2016
        response = await last_n_blocks(n, request.app)
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

async def get_daily_blocks(request):
    r = await get_blocks_by_day(request, today=True)
    return r

async def get_blocks_by_day(request, today=False):
    log = request.app["log"]
    log.info("get daily  blocks")
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""
               }
    try:
        if today:
            day = (int(time.time()) // 86400) * 86400
        else:
            try:
                day = request.match_info['day']
                day = datetime.datetime.strptime(day, '%Y%m%d')
                day = int(day.timestamp())
            except:
                raise APIException(PARAMETER_ERROR, "date format YYYYMMDD")

        response = await blocks_daily(day, request.app)
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

async def get_last_n_hours_blocks(request):
    log = request.app["log"]
    n = request.match_info['n']
    log.info("get last %s blocks" % n)
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        try:
            n = int(n)
            assert n > 0
        except:
            raise APIException(DECODE_ERROR, "hours should be numeric 1...24")
        if n > 24:
            raise APIException(PARAMETER_ERROR, "maximum 24 hours last blocks allowed")
        response = await blocks_last_n_hours(n, request.app)

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

async def get_data_last_n_blocks(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        n = request.match_info['n']
        n = int(n)
        if n > 2016:
            n = 2016
        response = await data_last_n_blocks(n, request.app)
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

async def get_data_daily_blocks(request):
    r = await get_data_blocks_by_day(request, today=True)
    return r

async def get_data_blocks_by_day(request, today=False):
    log = request.app["log"]
    log.info("get daily  blocks")
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""
               }
    try:
        if today:
            day = (int(time.time()) // 86400) * 86400
        else:
            try:
                day = request.match_info['day']
                day = datetime.datetime.strptime(day, '%Y%m%d')
                day = int(day.timestamp())
            except:
                raise APIException(PARAMETER_ERROR, "date format YYYYMMDD")

        response = await data_blocks_daily(day, request.app)
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

async def get_data_last_n_hours_blocks(request):
    log = request.app["log"]
    n = request.match_info['n']
    log.info("get last %s hours blocks" % n)
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    try:
        try:
            n = int(n)
            assert n > 0
        except:
            raise APIException(DECODE_ERROR, "hours should be numeric 1...24")
        if n > 24:
            raise APIException(PARAMETER_ERROR, "maximum 24 hours last blocks allowed")
        response = await blocks_data_last_n_hours(n, request.app)

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
