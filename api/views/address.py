from aiohttp import web
import traceback
import json
from model import address_state
from model import address_confirmed_utxo
from model import address_unconfirmed_utxo
from model import address_state_extended
from model import address_transactions
from model import address_unconfirmed_transactions
from pyltc import address_type
from pyltc import address_net_type
from pyltc import address_to_hash
from pybtc import hash_to_script
from pyltc import bytes_needed
from utils import APIException
from utils import INTERNAL_SERVER_ERROR
from utils import PARAMETER_ERROR
import math

async def get_address_state(request):
    log = request.app["log"]
    address = request.match_info['address']
    addr_type = address_type(address, num=True)
    log.info("GET %s" % str(request.rel_url))

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

        response = await address_state(address, type, request.app)
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

async def get_address_confirmed_utxo(request):
    log = request.app["log"]
    address = request.match_info['address']
    addr_type = address_type(address, num=True)
    log.info("get address confirmed utxo %s" % address)

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
        from_block = int(parameters["from_block"])
    except:
        from_block = 0

    try:
        order = "asc" if parameters["order"] == "asc" else "desc"
    except:
        order = "asc"

    order_by = "pointer"
    try:
        if parameters["order_by_amount"] in ("True", "1"):
            order_by = "amount"
    except:
        pass

    try:
        limit = int(parameters["limit"])
        if  not (limit > 0 and limit <= request.app["get_block_utxo_page_limit"]):
            raise Exception()
    except:
        limit = request.app["get_block_utxo_page_limit"]

    try:
        page = int(parameters["page"])
        if page <= 0: raise Exception()
    except:
        page = 1
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
            raise APIException(PARAMETER_ERROR, "invalid address")

        response = await address_confirmed_utxo(address, type, from_block, order, order_by, limit, page, request.app)
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

async def get_address_unconfirmed_utxo(request):
    log = request.app["log"]
    address = request.match_info['address']
    addr_type = address_type(address, num=True)
    log.info("get address confirmed utxo %s" % address)

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
        order = "asc" if parameters["order"] == "asc" else "desc"
    except:
        order = "asc"

    try:
        limit = int(parameters["limit"])
        if  not (limit > 0 and limit <= request.app["get_block_utxo_page_limit"]):
            raise Exception()
    except:
        limit = request.app["get_block_utxo_page_limit"]

    try:
        page = int(parameters["page"])
        if page <= 0: raise Exception()
    except:
        page = 1

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
            raise APIException(PARAMETER_ERROR, "invalid address")

        response = await address_unconfirmed_utxo(address, type, order, limit, page, request.app)
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

async def get_address_state_extended(request):
    log = request.app["log"]
    address = request.match_info['address']
    addr_type = address_type(address, num=True)
    log.info("get address extended state  %s" % str(request.rel_url))

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
        if parameters["type"] == "splitted":
            type = 3
    except:
        pass

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
            raise APIException(PARAMETER_ERROR, "invalid address")


        if address[0] == 0:
            if type is None or type == 0 or type == 3:
                p2pkh = await address_state_extended(address, request.app)
            if type is None or type == 2 or type == 3:
                address =  b"".join((b"\x02", address[1:]))
                pubkey = await address_state_extended(address, request.app)

            if type is None:

                # firstReceivedTxPointer
                if p2pkh["data"]["firstReceivedTxPointer"] is None and \
                        pubkey["data"]["firstReceivedTxPointer"] is None:
                    frp = None
                elif p2pkh["data"]["firstReceivedTxPointer"] is not None and \
                        pubkey["data"]["firstReceivedTxPointer"] is not None:
                    print(p2pkh["data"]["firstReceivedTxPointer"])
                    print(pubkey["data"]["firstReceivedTxPointer"])
                    p1 =  p2pkh["data"]["firstReceivedTxPointer"].split(":")
                    p2 =  pubkey["data"]["firstReceivedTxPointer"].split(":")

                    p1 = int(p1[0])<<20 + int(p1[1])
                    p2 = int(p2[0])<<20 + int(p2[1])

                    if p1 > p2:
                        frp = pubkey["data"]["firstReceivedTxPointer"]
                    else:
                        frp = p2pkh["data"]["firstReceivedTxPointer"]

                elif  p2pkh["data"]["firstReceivedTxPointer"] is not None:
                    frp = p2pkh["data"]["firstReceivedTxPointer"]
                else:
                    frp = pubkey["data"]["firstReceivedTxPointer"]

                # firstSentTxPointer
                if p2pkh["data"]["firstSentTxPointer"] is None and \
                        pubkey["data"]["firstSentTxPointer"] is None:
                    fsp = None
                elif p2pkh["data"]["firstSentTxPointer"] is not None and \
                        pubkey["data"]["firstSentTxPointer"] is not None:

                    p1 =  p2pkh["data"]["firstSentTxPointer"].split(":")
                    p2 =  pubkey["data"]["firstSentTxPointer"].split(":")

                    p1 = int(p1[0])<<20 + int(p1[1])
                    p2 = int(p2[0])<<20 + int(p2[1])

                    if p1 > p2:
                        fsp = pubkey["data"]["firstSentTxPointer"]
                    else:
                        fsp = p2pkh["data"]["firstSentTxPointer"]

                elif  p2pkh["data"]["firstSentTxPointer"] is not None:
                    fsp = p2pkh["data"]["firstSentTxPointer"]
                else:
                    fsp = pubkey["data"]["firstSentTxPointer"]

                # lastTxPointer
                if p2pkh["data"]["lastTxPointer"] is None and pubkey["data"]["lastTxPointer"] is None:
                    ltp = None
                elif p2pkh["data"]["lastTxPointer"] is not None and pubkey["data"]["lastTxPointer"] is not None:

                    p1 = p2pkh["data"]["lastTxPointer"].split(":")
                    p2 = pubkey["data"]["lastTxPointer"].split(":")

                    p1 = int(p1[0]) << 20 + int(p1[1])
                    p2 = int(p2[0]) << 20 + int(p2[1])

                    if p1 > p2:
                        ltp = pubkey["data"]["lastTxPointer"]
                    else:
                        ltp = p2pkh["data"]["lastTxPointer"]

                else:
                    if p2pkh["data"]["lastTxPointer"] is not None:
                        ltp = p2pkh["data"]["lastTxPointer"]
                    else:
                        ltp = pubkey["data"]["lastTxPointer"]

                if p2pkh["data"]["largestSpentTxAmount"] is None and pubkey["data"]["lastTxPointer"] is None:
                    lsta = None
                    lstp = None
                elif p2pkh["data"]["largestSpentTxAmount"] is not None and \
                        pubkey["data"]["largestSpentTxAmount"] is not None:
                    if p2pkh["data"]["largestSpentTxAmount"] > pubkey["data"]["largestSpentTxAmount"]:
                        lsta = p2pkh["data"]["largestSpentTxAmount"]
                        lstp = p2pkh["data"]["largestSpentTxPointer"]
                    else:
                        lsta = pubkey["data"]["largestSpentTxAmount"]
                        lstp = pubkey["data"]["largestSpentTxPointer"]
                else:
                    if p2pkh["data"]["largestSpentTxAmount"] is not None:
                        lsta = p2pkh["data"]["largestSpentTxAmount"]
                        lstp = p2pkh["data"]["largestSpentTxPointer"]
                    else:
                        lsta = pubkey["data"]["largestSpentTxAmount"]
                        lstp = pubkey["data"]["largestSpentTxPointer"]

                if p2pkh["data"]["largestReceivedTxAmount"] is None and \
                        pubkey["data"]["largestReceivedTxAmount"] is None:
                    lrta = None
                    lrtp = None
                elif p2pkh["data"]["largestReceivedTxAmount"] is not None and \
                        pubkey["data"]["largestReceivedTxAmount"] is not None:
                    if p2pkh["data"]["largestReceivedTxAmount"] > pubkey["data"]["largestReceivedTxAmount"]:
                        lrta = p2pkh["data"]["largestReceivedTxAmount"]
                        lrtp = p2pkh["data"]["largestReceivedTxPointer"]
                    else:
                        lrta = pubkey["data"]["largestReceivedTxAmount"]
                        lrtp = pubkey["data"]["largestReceivedTxPointer"]
                else:
                    if p2pkh["data"]["largestReceivedTxAmount"] is not None:
                        lrta = p2pkh["data"]["largestReceivedTxAmount"]
                        lrtp = p2pkh["data"]["largestReceivedTxPointer"]
                    else:
                        lrta = pubkey["data"]["largestReceivedTxAmount"]
                        lrtp = pubkey["data"]["largestReceivedTxPointer"]

                response = {"data": {"balance": p2pkh["data"]["balance"] + pubkey["data"]["balance"],
                                   "receivedAmount": p2pkh["data"]["receivedAmount"] + pubkey["data"]["receivedAmount"],
                                   "receivedTxCount": p2pkh["data"]["receivedTxCount"] + pubkey["data"]["receivedTxCount"],
                                   "sentAmount": p2pkh["data"]["sentAmount"] + pubkey["data"]["sentAmount"],
                                   "sentTxCount": p2pkh["data"]["sentTxCount"] + pubkey["data"]["sentTxCount"],
                                   "firstReceivedTxPointer": frp,
                                   "firstSentTxPointer": fsp,
                                   "lastTxPointer": ltp,
                                   "largestReceivedTxAmount": lrta,
                                   "largestReceivedTxPointer": lrtp,
                                   "largestSpentTxAmount": lsta,
                                   "largestSpentTxPointer": lstp,
                                   "receivedOutsCount": p2pkh["data"]["receivedOutsCount"] + pubkey["data"]["receivedOutsCount"],
                                   "spentOutsCount": p2pkh["data"]["spentOutsCount"] + pubkey["data"]["spentOutsCount"],
                                   "pendingReceivedAmount": p2pkh["data"]["pendingReceivedAmount"] + pubkey["data"]["pendingReceivedAmount"],
                                   "pendingSentAmount": p2pkh["data"]["pendingSentAmount"] + pubkey["data"]["pendingSentAmount"],
                                   "pendingReceivedTxCount": p2pkh["data"]["pendingReceivedTxCount"] + pubkey["data"]["pendingReceivedTxCount"],
                                   "pendingSentTxCount": p2pkh["data"]["pendingSentTxCount"] + pubkey["data"]["pendingSentTxCount"],
                                   "pendingReceivedOutsCount": p2pkh["data"]["pendingReceivedOutsCount"] + pubkey["data"]["pendingReceivedOutsCount"],
                                   "pendingSpentOutsCount": p2pkh["data"]["pendingSpentOutsCount"] + pubkey["data"]["pendingSpentOutsCount"],
                                   "type": "PUBKEY+P2PKH"},
                        "time": p2pkh["time"] + pubkey["time"]}
            elif type == 3:
                response = {"data": {"P2PKH": p2pkh["data"],
                                     "PUBKEY": pubkey["data"]},
                            "time": p2pkh["time"] + pubkey["time"]}
            elif type == 0:
                response = p2pkh
            elif type == 2:
                response = pubkey

        else:
            response = await address_state_extended(address, request.app)
            if type == 3:
                response["data"] = {response["data"]["type"]: response["data"]}
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

async def get_address_transactions(request):
    log = request.app["log"]
    address = request.match_info['address']
    addr_type = address_type(address, num=True)
    log.info("GET %s" % str(request.rel_url))

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
        limit = int(parameters["limit"])
        if  not (limit > 0 and limit <= 50):
            limit = 50
    except:
        limit = 50

    try:
        from_block = int(parameters["from_block"])
        if from_block > 2 ** 25:
            from_block = 0
    except:
        from_block = 0

    try:
        page = int(parameters["page"])
        if page <= 0: raise Exception()
    except: page = 1

    try:
        order = "asc" if parameters["order"] == "asc" else "desc"
    except:
        order = "desc"

    try:
        mode = "verbose" if parameters["mode"] == "verbose" else "brief"
    except:
        mode = "brief"

    try:
        timeline = True if parameters["timeline"] in ("1", "True") else False
    except:
        timeline = False

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

        response = await address_transactions(address, type, limit, page, order, mode, timeline, request.app)
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

async def get_address_unconfirmed_transactions(request):
    log = request.app["log"]
    address = request.match_info['address']
    addr_type = address_type(address, num=True)
    log.info("GET %s" % str(request.rel_url))

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
        limit = int(parameters["limit"])
        if  not (limit > 0 and limit <= 50):
            limit = 50
    except:
        limit = 50

    try:
        page = int(parameters["page"])
        if page <= 0:
            raise Exception()
    except: page = 1

    try:
        order = "asc" if parameters["order"] == "asc" else "desc"
    except:
        order = "desc"

    try:
        mode = "verbose" if parameters["mode"] == "verbose" else "brief"
    except:
        mode = "brief"

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

        response = await address_unconfirmed_transactions(address, type, limit, page, order, mode, request.app)
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
