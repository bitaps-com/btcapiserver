import math
import time
from aiohttp import web
import traceback
from model import *
from pybtc import *
from utils import *
import zlib

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
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]:
            log.error(str(traceback.format_exc()))
        else:
            log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)

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

        response = await block_utxo(pointer, limit, page, order, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
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
            if request.rel_url.query['raw_tx'] in ('True', '1'):
                option_raw_tx = True
        except:
            option_raw_tx = False
        response = await block_transactions_opt_tx(pointer, option_raw_tx, limit, page, order, request.app)

        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
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

        response = await block_transactions(pointer, limit, page, order, request.app)

        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)





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
                    "details": err.details
                    }
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
                    "details": err.details
                    }
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
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]:
            log.error(str(traceback.format_exc()))
        else:
            log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


async def get_transaction_by_pointer(request):
    log = request.app["log"]
    log.info("GET %s" % str(request.rel_url))
    pointer = request.match_info['tx_pointer']

    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    if request.app["transaction"]:
        try:
            if request.rel_url.query['raw_tx'] in ('True','1'):
                option_raw_tx = True
        except:
            option_raw_tx = False

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

        if request.app["transaction"]:
            response = await tx_by_pointer_opt_tx(pointer, option_raw_tx, request.app)
        else:
            response = await tx_by_pointer(pointer, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
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
            if b < 0: raise Exception()
            if t < 0: raise Exception()
            pointer = (b << 39) + (t << 20)
        except:
            raise APIException(PARAMETER_ERROR, "invalid transaction pointer")

        response = await tx_hash_by_pointer(pointer, request.app)

        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


async def get_transaction_by_pointer_list(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    pointers = list()
    hashes = list()
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    if request.app["transaction"]:
        try:
            if request.rel_url.query['raw_tx'] in ('True','1'):
                option_raw_tx = True
        except:
            option_raw_tx = False


    try:
        try:
            await request.post()
            data = await request.json()
            for pointer in data:
                if len(pointer) == 64:
                    try:
                        pointer = s2rh(pointer)
                        hashes.append(pointer)
                    except:
                        raise APIException(PARAMETER_ERROR, "invalid transaction hash %s" % pointer)
                else:
                    try:
                        k = pointer.split(":")
                        b = int(k[0])
                        t = int(k[1])
                        if b < 0: raise Exception()
                        if t < 0: raise Exception()
                        pointer = (b << 39) + (t << 20)
                        pointers.append(pointer)
                    except:
                        raise APIException(PARAMETER_ERROR, "invalid transaction pointer %s" % pointer)

        except:
            raise APIException(JSON_DECODE_ERROR, "invalid transaction pointers list")

        if request.app["transaction"]:
            response = await tx_by_pointers_opt_tx(pointers, hashes, option_raw_tx, request.app)
        else:
            response = await tx_by_pointers(pointers, hashes, request.app)

        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


async def get_transactions_hash_by_pointer(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    pointers = list()
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    try:

        try:
            await request.post()
            data = await request.json()
            for pointer in data:
                k = pointer.split(":")
                b = int(k[0])
                t = int(k[1])
                if b < 0: raise Exception()
                if t < 0: raise Exception()

                pointers.append((b << 39) + (t << 20))
        except Exception:
            raise APIException(JSON_DECODE_ERROR, "invalid transaction pointers list")


        response = await tx_hash_by_pointers(pointers, request.app)

        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
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
                if b < 0: raise Exception()
                if t < 0: raise Exception()
                pointer = (b << 39) + (t << 20)
            except:
                raise APIException(PARAMETER_ERROR, "invalid transaction pointer")


        response = await tx_merkle_proof_by_pointer(pointer, request.app)
        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
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
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


async def get_address_state(request):
    log = request.app["log"]
    address = request.match_info['address']
    addr_type = address_type(address, num=True)
    log.info("GET %s" % str(request.rel_url))


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
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


async def get_address_state_by_list(request):
    log = request.app["log"]
    log.info("POST %s" % str(request.rel_url))
    addresses = dict()
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}


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

        response = await address_list_state(addresses, request.app)

        status = 200
    except APIException as err:
        status = err.status
        response = {"error_code": err.err_code,
                    "message": err.message,
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
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


async def get_address_unconfirmed_utxo(request):
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

        response = await address_unconfirmed_utxo(address, request.app)
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


async def test_filter(request):

    try:
        async with request.app["db_pool"].acquire() as conn:
            row = await conn.fetchrow("SELECT height, filter  "
                                          "FROM block_filters_checkpoints "
                                       "order by height asc limit 1;" )

        N = c_int_to_int(row["filter"])
        i = c_int_len(N)
        f = zlib.decompress(row["filter"][i:])
        h = row["height"]
        f = decode_gcs(f, N, P=request.app["block_filter_bits"])
        print(">>>",len(f), h)

        # 141788

        async with request.app["db_pool"].acquire() as conn:
            rows = await conn.fetch("SELECT distinct  address  "
                                    "FROM transaction_map "
                                    "where  pointer < $1;" ,  10000 << 39 )

        print("affected addresses ", len(rows))
        N = request.app["block_filter_fps"]
        M = request.app["block_filter_capacity"]
        elements = [map_into_range(siphash(e["address"]), N * M) for e in rows]

        for a in elements:
            if a not in f:
                print("false negative!")
                await asyncio.sleep(0)



        # print("positive ", i)
        # print("false positive ", e2)
        # print("errors ", e)

        resp = ["ok"]

    except Exception as err:
        print(str(traceback.format_exc()))
        return web.json_response([str(err)], dumps=json.dumps, status=200)
    return web.json_response(resp, dumps=json.dumps, status = 200)



async def about(request):
    log = request.app["log"]
    log.info(request.url)
    log.info('404')
    resp = {"about": "BTCAPI server",
            "error": "Endpoint not found"}
    return web.json_response(resp, dumps=json.dumps, status = 404)
