import math
import time
from aiohttp import web
import traceback
from model import *
from pybtc import *
from utils import *
import datetime
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
                    "details": err.details
                    }
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


async def get_daily_blocks(request):
    r = await get_blocks_by_day(request, today=True)
    return r

async def get_last_n_hours_blocks(request):
    # todo
    # кеширование запросов
    log = request.app["log"]
    db_pool = request.app["db_pool"]
    n = request.match_info['n']
    log.info("get last %s blocks" % n)
    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""
               }
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
                "details": ""
               }
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
        log.error(str(traceback.format_exc()))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


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
        log.error(str(traceback.format_exc()))
    finally:
        return web.json_response(response, dumps=json.dumps, status = status)


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



async def get_block_data_last(request):
    return await get_block_data_by_pointer(request, 'last')

async def get_block_data_by_pointer(request, pointer=None):
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
        response = await block_data_by_pointer(pointer, request.app)
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
    except: page = 1


    if limit == 0 or limit > request.app["get_block_tx_page_limit"]:
        if limit == 0:
            page = 1
        limit = request.app["get_block_tx_page_limit"]


    if limit * page > 2 ** 19 - 1:
        limit = 50
        page = 1

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
        response = await block_transactions(pointer, option_raw_tx, limit, page, order, mode, request.app)

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
        if not request.app["transaction"]:
            raise APIException(UNAVAILABLE_METHOD, "unavailable method")

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
            order = "asc" if parameters["order"] == "desc" else "desc"
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

        response = await tx_by_pointer_opt_tx(pointer, option_raw_tx, request.app)
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

    try:
        if request.rel_url.query['raw_tx'] in ('True','1'):
            option_raw_tx = True
    except:
        option_raw_tx = False


    try:
        try:
            await request.post()
            data = await request.json()
            if len(data) > 100:
                raise APIException(PARAMETER_ERROR, "only 100 transactions per request limit")
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

        response = await tx_by_pointers_opt_tx(pointers, hashes, option_raw_tx, request.app)
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


async def get_transaction_hash_by_pointers(request):
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
            if len(data) > 500:
                raise APIException(PARAMETER_ERROR, "only 1000 transactions per request limit")
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

    parameters = request.rel_url.query

    try:
        type = None
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
                    "details": err.details
                    }
    except Exception as err:
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
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

    try:
        type = None
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

        response = await address_transactions(address, type, request.app)
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

    parameters = request.rel_url.query

    try:
        type = None
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
        if request.app["debug"]: log.error(str(traceback.format_exc()))
        else: log.error(str(err))
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
                if p2pkh["data"]["firstReceivedTxPointer"] is None and pubkey["data"]["firstReceivedTxPointer"] is None:
                    frp = None
                elif p2pkh["data"]["firstReceivedTxPointer"] is not None and pubkey["data"]["firstReceivedTxPointer"] is not None:
                    if p2pkh["data"]["firstReceivedTxPointer"] > pubkey["data"]["firstReceivedTxPointer"]:
                        frp = pubkey["data"]["firstReceivedTxPointer"]
                    else:
                        frp = p2pkh["data"]["firstReceivedTxPointer"]
                elif  p2pkh["data"]["firstReceivedTxPointer"] is not None:
                    frp = p2pkh["data"]["firstReceivedTxPointer"]
                else:
                    frp = pubkey["data"]["firstReceivedTxPointer"]

                if p2pkh["data"]["firstSentTxPointer"] is None and pubkey["data"]["firstSentTxPointer"] is None:
                    fsp = None
                elif p2pkh["data"]["firstSentTxPointer"] is not None and pubkey["data"]["firstSentTxPointer"] is not None:
                    if p2pkh["data"]["firstSentTxPointer"] > pubkey["data"]["firstSentTxPointer"]:
                        fsp = pubkey["data"]["firstSentTxPointer"]
                    else:
                        fsp = p2pkh["data"]["firstSentTxPointer"]
                elif  p2pkh["data"]["firstSentTxPointer"] is not None:
                    fsp = p2pkh["data"]["firstSentTxPointer"]
                else:
                    fsp = pubkey["data"]["firstSentTxPointer"]


                if p2pkh["data"]["lastTxPointer"] is None and pubkey["data"]["lastTxPointer"] is None:
                    ltp = None
                elif p2pkh["data"]["lastTxPointer"] is not None and pubkey["data"]["lastTxPointer"] is not None:
                    if p2pkh["data"]["lastTxPointer"] > pubkey["data"]["lastTxPointer"]:
                        ltp = p2pkh["data"]["lastTxPointer"]
                    else:
                        ltp = pubkey["data"]["lastTxPointer"]
                else:
                    if p2pkh["data"]["lastTxPointer"] is not None:
                        ltp = p2pkh["data"]["lastTxPointer"]
                    else:
                        ltp = pubkey["data"]["lastTxPointer"]

                if p2pkh["data"]["largestSpentTxAmount"] is None and pubkey["data"]["lastTxPointer"] is None:
                    lsta = None
                    lstp = None
                elif p2pkh["data"]["largestSpentTxAmount"] is not None and pubkey["data"]["largestSpentTxAmount"] is not None:
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

                if p2pkh["data"]["largestReceivedTxAmount"] is None and pubkey["data"]["largestReceivedTxAmount"] is None:
                    lrta = None
                    lrtp = None
                elif p2pkh["data"]["largestReceivedTxAmount"] is not None and pubkey["data"]["largestReceivedTxAmount"] is not None:
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
    log.info("get address confirmed utxo %s" % address)


    status = 500
    response = {"error_code": INTERNAL_SERVER_ERROR,
                "message": "internal server error",
                "details": ""}

    parameters = request.rel_url.query

    try:
        type = None
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
        order = "asc" if parameters["order"] == "desc" else "desc"
    except:
        order = "asc"

    try:
        order_by = "pointer"
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
        log.error(str(traceback.format_exc()))
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

    try:
        type = None
        if parameters["type"] == "P2PKH":
            type = 0
        if parameters["type"] == "PUBKEY":
            type = 2
    except:
        pass

    try:
        order = "asc" if parameters["order"] == "desc" else "desc"
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
