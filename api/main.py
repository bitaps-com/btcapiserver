import configparser
import logging
import sys
import colorlog
import model
from routes import *
from pybtc import LRU



config_file =   "../config/btcapi-server.conf"
log_level = logging.WARNING
logger = colorlog.getLogger('API')
config = configparser.ConfigParser()
config.read(config_file)

try:
    postgres_dsn = config["POSTGRESQL"]["dsn"]
    pool_threads = config["POSTGRESQL"]["client_threads"]
    testnet = int(config["CONNECTOR"]["testnet"])
    # log_level = config["SERVER"]["log_level"]
except Exception as err:
    logger.critical("Сonfig failed: %s" % err)
    logger.critical("Shutdown")
    sys.exit(0)

try:
    log_level = log_level_map[config["SERVER"]["api_log_level"]]
    dbg = True
except:
    dbg = False

logger.setLevel(log_level)
ch = logging.StreamHandler()
ch.setLevel(log_level)
formatter = colorlog.ColoredFormatter('%(log_color)s%(asctime)s %(levelname)s: %(message)s (%(module)s:%(lineno)d)')
ch.setFormatter(formatter)
logger.addHandler(ch)

app = web.Application()


app['dsn'] = postgres_dsn
app['debug'] = dbg
app['pool_threads'] = int(pool_threads)
app['log'] = logger
app['testnet'] = testnet

app["transaction"] = True if config["OPTIONS"]["transaction"] == "on" else False
app["merkle_proof"] = True if config["OPTIONS"]["merkle_proof"] == "on" else False
app["address_state"] = True if config["OPTIONS"]["address_state"] == "on" else False
app["address_timeline"] = True if config["OPTIONS"]["address_timeline"] == "on" else False
app["blocks_data"] = True if config["OPTIONS"]["blocks_data"] == "on" else False
app["blockchain_analytica"] = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
app["mempool_analytica"] = True if config["OPTIONS"]["mempool_analytica"] == "on" else False
app["transaction_history"] = True if config["OPTIONS"]["transaction_history"] == "on" else False

app["block_filters"] = True if config["OPTIONS"]["block_filters"] == "on" else False
app["merkle_tree_cache"] = LRU(1000)
app["block_transactions"] = LRU(500)
app["block_transaction_id_list"] = LRU(500)
app["node_rpc_url"] = config["CONNECTOR"]["rpc"]
app["rpc"] = None

try: app["get_block_utxo_page_limit"] = int(config["API"]["get_block_utxo_page_limit"])
except: app["get_block_utxo_page_limit"] = 5000

try: app["get_block_tx_page_limit"] = int(config["API"]["get_block_tx_page_limit"])
except: app["get_block_tx_page_limit"] = 2 ** 19 - 1

app.on_startup.append(model.init_db_pool)
app.on_startup.append(model.load_block_map)
app.on_cleanup.append(model.close_db_pool)
setup_routes(app)