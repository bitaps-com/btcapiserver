from aiohttp import web
import traceback
import json
from model import block_filters_headers
from model import block_filters_batch_headers
from model import block_filters
from pybtc import s2rh
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import INVALID_BLOCK_POINTER
from utils import PARAMETER_ERROR


async def get_block_filters_headers(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    filter_type = int(request.match_info['filter_type'])
    start_height = int(request.match_info['start_height'])
    stop_hash = request.match_info['stop_hash']

    try:
        try:
            stop_hash = s2rh(stop_hash)
            if len(stop_hash) != 32:
                raise Exception()
        except:
            raise APIException(INVALID_BLOCK_POINTER, "invalid stop hash")

        if filter_type < 1:
            raise APIException(PARAMETER_ERROR, "invalid filter_type")

        if start_height < 0:
            raise APIException(PARAMETER_ERROR, "invalid start_height")

        response = await block_filters_headers(filter_type,
                                              start_height,
                                              stop_hash,
                                              log,
                                              request.app)
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

async def get_block_filters_batch_headers(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    filter_type = int(request.match_info['filter_type'])
    start_height = int(request.match_info['start_height'])
    stop_hash = request.match_info['stop_hash']

    try:
        try:
            stop_hash = s2rh(stop_hash)
            if len(stop_hash) != 32:
                raise Exception()
        except:
            raise APIException(INVALID_BLOCK_POINTER, "invalid stop hash")

        if filter_type < 1:
            raise APIException(PARAMETER_ERROR, "invalid filter_type")

        if start_height < 0:
            raise APIException(PARAMETER_ERROR, "invalid start_height")

        response = await block_filters_batch_headers(filter_type,
                                              start_height,
                                              stop_hash,
                                              log,
                                              request.app)
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

async def get_block_filters(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
    filter_type = int(request.match_info['filter_type'])
    start_height = int(request.match_info['start_height'])
    stop_hash = request.match_info['stop_hash']

    try:
        try:
            stop_hash = s2rh(stop_hash)
            if len(stop_hash) != 32:
                raise Exception()
        except:
            raise APIException(INVALID_BLOCK_POINTER, "invalid stop hash")

        if filter_type < 1:
            raise APIException(PARAMETER_ERROR, "invalid filter_type")

        if start_height < 0:
            raise APIException(PARAMETER_ERROR, "invalid start_height")

        response = await block_filters(filter_type,
                                              start_height,
                                              stop_hash,
                                              log,
                                              request.app)
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

