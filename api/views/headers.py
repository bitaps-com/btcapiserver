from aiohttp import web
import traceback
import json
from model import block_headers
from pybtc import s2rh
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import INVALID_BLOCK_POINTER
from utils import PARAMETER_ERROR



async def get_block_headers(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}
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

        response = await block_headers(pointer, count, request.app)
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

