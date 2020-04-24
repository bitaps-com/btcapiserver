from aiohttp import web
import traceback
import json
from model import address_list_state
from pybtc import address_type
from pybtc import address_net_type
from pybtc import address_to_hash
from pybtc import bytes_needed
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import PARAMETER_ERROR
from utils import JSON_DECODE_ERROR


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


