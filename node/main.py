import configparser
import logging
import sys
import colorlog
from pybtc import LRU

import asyncio
import signal
import argparse
from setproctitle import setproctitle
import uvloop
from protocol import Bitcoin
import traceback
import asyncpg
import aiodns
import time
import aiosocks
from utils import *



class App:

    def __init__(self, logger, config):
        setproctitle('btcapi-node')
        self.loop = asyncio.get_event_loop()
        self.log = logger
        self.config = config
        self.db_pool = False
        self.network = {"port": int(config["NODE"]["port"]),
                        "magic": int(config["NODE"]["magic"], 16),
                        "version": int(config["NODE"]["version"]),
                        "services": int(config["NODE"]["services"], 2),
                        "ping_timeout": int(config["NODE"]["ping_timeout"]),
                        "connect_timeout": int(config["NODE"]["connect_timeout"]),
                        "handshake_timeout": int(config["NODE"]["handshake_timeout"]),
                        "ip": config["NODE"]["host"],
                        "user_agent": config["NODE"]["user_agent"],
                        }
        self.testnet = True if config["NODE"]["testnet"] != "1" else False
        self.block_interval = float(config["NODE"]["block_interval"])
        self.seed_domain = config["NODE"]["seed_dns"].split(",")
        self.dsn = config['POSTGRESQL']['dsn']
        self.psql_pool_threads = 5

        self.outgoing = []
        self.incoming_server = None
        self.incoming = []
        self.background_tasks = []
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)
        asyncio.ensure_future(self.start())

    async def start(self):
        # init database
        try:
            self.log.info("Init db pool ")
            self.db_pool = await asyncpg.create_pool(dsn=self.dsn,
                                                     loop=self.loop,
                                                     min_size=1, max_size=self.psql_pool_threads)
        except Exception as err:
            self.log.error("Start failed")
            self.log.error(str(traceback.format_exc()))
            self.terminate(None, None)
        self.background_tasks.append(self.loop.create_task(self.incoming_connections_server()))

    async def handle_connection(self, reader, writer):
        s = writer.get_extra_info("socket")
        ip, port = s.getpeername()
        print("new connection from", ip , port)
        with Bitcoin(ip, port, self.network, self.db_pool, self.log, reader=reader, writer=writer) as conn:
            await conn.disconnected
        print("disconnected")


    async def incoming_connections_server(self):
        while True:
            try:
                if self.incoming_server is None or not self.incoming_server.is_serving():
                    self.incoming_server = await asyncio.start_server(self.handle_connection,
                                                        self.network["ip"],
                                                        self.network["port"])
                    print("binded to ", self.network["ip"], self.network["port"])
            except asyncio.CancelledError:
                self.log.warning("Incoming connections server task stopped")
                break
            except Exception as err:
                self.log.critical("Incoming connections server task  error %s" % err)
                self.log.critical(str(traceback.format_exc()))
            await asyncio.sleep(5)





    async def get_bootstrap_from_db(self):
        rows =await model.get_last_24hours_addresses(self.db_pool)
        for row in rows:
            self.not_scanned_addresses[row["ip"].decode()] = {"port" :row["port"],
                                                             "address": row["ip"].decode()}
        self.log.info("Last 24 hours active addresses from db %s" % len(rows))

    async def get_seed_from_dns(self):
        self.log.info("Get bootstrap addresses from dns seeds")
        tasks = []
        for domain in self.seed_domain:
            tasks.append(self.loop.create_task(self.resolve_domain(domain)))
        await asyncio.wait(tasks)
        self.log.info("All seed domains resolved, received %s addresses" % len(self.not_scanned_addresses))

    def add_bootstrap_tor_seed(self):
        if self.testnet:
            tor_seeds = []
        else:
            tor_seeds = []
        for a in tor_seeds:
            self.not_scanned_addresses[a[0]] = {"port": a[1],
                                                "address": a[0]}

    async def resolve_domain(self, domain):
        self.log.debug('resolving domain %s' % domain)
        resolver = aiodns.DNSResolver(loop=self.loop)
        query = resolver.query(domain, 'A')
        try:
            result = await asyncio.ensure_future(query, loop=self.loop)
        except Exception as err:
            self.log.error('%s %s' % (domain, err))
            return
        c = 0
        for i in result:
            self.log.debug('%s received from %s' % (i.host, domain))
            c += 1
            if not self.not_scanned_addresses or 1:
                self.not_scanned_addresses[i.host] = {"port": self.network["port"],
                                                               "address": i.host}
        query = resolver.query(domain, 'AAAA')
        try:
            result = await asyncio.ensure_future(query, loop=self.loop)
        except Exception as err:
            self.log.error('%s %s' % (domain, err))
            return
        t = 0
        c2 = 0
        for i in result:
            self.log.debug('%s received from %s' % (i.host, domain))
            c2 += 1
            if not self.not_scanned_addresses or 1:
                a = bytes_to_address(ip_address_to_bytes(i.host))
                if a.endswith('.onion'):
                    t += 1
                self.not_scanned_addresses[i.host] = {"port": self.network["port"],
                                                               "address": i.host}



        self.log.info('%s ipv4 %s ipv6 %s  tor addresses received from %s' % (c, c2, t, domain))

    async def scan_address(self, address, port):
        try:
            if address.endswith(".onion"):
                proxy = aiosocks.Socks5Addr('127.0.0.1', 9050)
            else:
                proxy = None
            conn = BitcoinProtocol(address, port, self.network, self.testnet, self.block_interval, self.log, proxy = proxy)
            try:
                await asyncio.wait_for(conn.handshake, timeout=10)
            except:
                pass
            if conn.handshake.result() == True:
                try:
                    await asyncio.wait_for(conn.addresses_received, timeout=10)
                except:
                    pass
                for a in conn.addresses:
                    if a["address"] not in self.not_scanned_addresses:
                        if a["address"] not in self.scanning_addresses:
                            if a["address"] not in self.scanned_addresses:
                                self.not_scanned_addresses[a["address"]] = a
                self.online_nodes += 1
                # add record to db
                net = network_type(address)
                if net == "TOR":
                    geo = {"country": None,
                            "city": None,
                            "geo": None,
                            "timezone": None,
                            "asn": None,
                            "org": None}
                else:
                    geo = await self.loop.run_in_executor(None, model.get_geoip, address)

                await model.report_online(address,
                                          port,
                                          net,
                                          conn.user_agent,
                                          conn.latency,
                                          conn.version,
                                          conn.start_height,
                                          conn.services,
                                          geo,
                                          int(time.time()),
                                          self.db_pool)
            else:
                await model.report_offline(address,
                                           self.db_pool)
            conn.__del__()

        except:
            try:
                await model.report_offline(address,
                                           self.db_pool)
                conn.__del__()
            except:
                pass

        self.scan_threads -= 1
        del self.scanning_addresses[address]
        self.scanned_addresses.add(address)






    def _exc(self, a, b, c):
        return

    def terminate(self, a, b):
        self.loop.create_task(self.terminate_coroutine())

    async def terminate_coroutine(self):
        sys.excepthook = self._exc
        self.log.error('Stop request received')

        for task in self.background_tasks:
            task.cancel()
        if self.db_pool:
            await self.db_pool.close()

        self.log.info("Server stopped")
        self.loop.stop()


def init(argv):
    config_file = "../config/btcapi-server.conf"
    log_level = logging.DEBUG
    config = configparser.ConfigParser()
    config.read(config_file)

    logger = colorlog.getLogger('node')
    logger.setLevel(log_level)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    formatter = colorlog.ColoredFormatter('%(log_color)s%(asctime)s %(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    config = configparser.ConfigParser()
    config.read(config_file)

    try:
        config["POSTGRESQL"]["dsn"]
        config["NODE"]["seed_dns"]
        config["NODE"]["port"]
        config["NODE"]["host"]
        config["NODE"]["magic"]
        config["NODE"]["version"]
        config["NODE"]["services"]
        config["NODE"]["user_agent"]
        config["NODE"]["ping_timeout"]
        config["NODE"]["connect_timeout"]
        config["NODE"]["handshake_timeout"]
        config["NODE"]["block_interval"]
        config["OPTIONS"]["transaction"]
        config["OPTIONS"]["merkle_proof"]
        config["OPTIONS"]["address_state"]
        config["OPTIONS"]["address_timeline"]
        config["OPTIONS"]["blockchain_analytica"]
        config["OPTIONS"]["transaction_history"]
        config["OPTIONS"]["block_filters"]
    except Exception as err:
        logger.critical("Configuration failed: %s" % err)
        logger.critical("Shutdown")
        sys.exit(0)

    app = App(logger, config)
    return app


if __name__ == '__main__':
    uvloop.install()
    loop = asyncio.get_event_loop()
    app = init(sys.argv[1:])
    loop.run_forever()
    pending = asyncio.all_tasks()
    for task in pending:
        task.cancel()
    if pending:
        loop.run_until_complete(asyncio.wait(pending))
    loop.close()



