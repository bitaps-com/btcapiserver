from utils import *
import time
import aiosocks
import random
import asyncio
from pybtc import *


class Bitcoin():

    def __init__(self, ip, port, settings, db_pool, log,
                 testnet = False,
                 block_interval = 10,
                 verbose = 1,
                 proxy = None,
                 reader = None,
                 writer = None):
        self.settings = settings
        self.last_message_timestamp = 0
        self.ip = ip
        self.proxy = proxy
        self.verbose = verbose
        self.port = port
        self.log = log
        self.block_interval = block_interval
        self.testnet = testnet
        self.loop = asyncio.get_event_loop()
        self.status = "connecting"
        self.handshake = asyncio.Future()
        self.disconnected = asyncio.Future()
        self.addresses_received = asyncio.Future()
        self.getheaders_future = None
        self.getaddr_sent = False
        self.start_time = int(time.time())
        self.tasks = list()
        self.reader = reader
        self.writer = writer
        self.verack_received = False
        self.verack_sent = False
        self.version_received = False
        self.version_sent = False
        self.latency = 0
        self.version_nonce = None
        self.addresses = list()
        self.version = None
        self.services = None
        self.user_agent = None
        self.start_height = None
        self.relay = None
        self.db_pool = db_pool
        self.cmd_map = {b"ping": self.ping,
                        b"pong": self.pong,
                        b"headers": self.headers,
                        b"verack": self.verack,
                        b"version": self.version_rcv,
                        b"getheaders": self.getheaders,
                        b"addr": self.address}
        self.pong = None
        self.ping_pong_future = None
        self.timestamp = False
        self.addr_recv = False
        self.addr_from = False

        self.loop.create_task(self.start())

    def __enter__(self):
        print("enter")
        return self

    def __exit__(self, *err):
        for t in self.tasks:
            t.cancel()
        if self.writer is not None:
            self.writer.close()
        self.log.debug('disconnected from %s:%s' % (self.ip, self.port))




    async def start(self):
        if self.reader is None:
            self.log.debug('connecting to %s:%s' % (self.ip, self.port))
            try:
                if self.proxy is not None:
                    r = await asyncio.wait_for(aiosocks.open_connection(proxy=self.proxy,
                                                                        proxy_auth = None,
                                                                        dst=(self.ip, int(self.port)),
                                                                        remote_resolve=True),
                                                                        self.settings["connect_timeout"])
                else:
                    r = await asyncio.wait_for(asyncio.open_connection(self.ip,
                                                                       int(self.port)),
                                                                       self.settings["connect_timeout"])
                self.reader, self.writer = r
                self.log.debug('start handshake with %s:%s' % (self.ip, self.port))
                await self.send_msg(self.create_message('version', self.create_version()))
                self.version_sent = True
            except Exception as err:
                self.log.debug('connecting filed %s:%s %s' % (self.ip, self.port, err))
                return
        else:
            self.log.debug('wait handshake with %s:%s' % (self.ip, self.port))

        self.tasks.append(self.loop.create_task(self.get_next_message()))

        try:
            await asyncio.wait_for(self.handshake, timeout=self.settings["handshake_timeout"])
        except Exception as err:
            self.log.debug('Bitcoin protocol handshake error')
            self.disconnected.set_result(True)
            return
        if self.handshake.result() == True:
            self.log.debug('handshake success %s:%s' % (self.ip, self.port))
            self.tasks.append(self.loop.create_task(self.ping_pong_task()))
            # await self.send_msg(self.create_message('getaddr', b''))
        else:
            self.disconnected.set_result(True)





    # incoming messages

    def ping(self, header_opt, checksum, data):
        msg = self.create_message('pong', data)
        self.loop.create_task(self.send_msg(msg))

    def pong(self, header_opt, checksum, data):
        if self.ping_pong_future is not None:
            if not self.ping_pong_future.done():
                self.ping_pong_future.set_result(data)

    def verack(self, header_opt, checksum, data):
        if self.handshake.done():
            return
        self.verack = True
        if self.version_received and self.version_sent:
            self.handshake.set_result(True)

    def version_rcv(self, header_opt, checksum, data):
        if self.version_received:
            return
        if self.version_nonce == data[72:80]:
            self.log.debug('Itself connection detected')
            self.handshake.set_result(False)
            return
        self.version = int.from_bytes(data[0:4], byteorder='little')
        if self.version < 70000:
            self.log.debug('protocol verson < 70000 reject connection')
            self.handshake.set_result(False)
            return
        # exclude bitcoin cash nodes
        NODE_UNSUPPORTED_SERVICE_BIT_5 = (1 << 5)

        self.services = int.from_bytes(data[4:12], byteorder='little')
        if self.services & NODE_UNSUPPORTED_SERVICE_BIT_5:
            self.handshake.set_result(False)
            return
        self.timestamp = int.from_bytes(data[12:20], byteorder='little')

        l = get_var_int_len(data[80:89])
        l2 = var_int_to_int(data[80:80 + l])
        try:
            self.user_agent = data[80 + l:80 + l + l2].decode()
        except:
            self.user_agent = ''
        self.start_height = int.from_bytes(data[80 + l + l2:80 + l + l2 + 4], byteorder='little')
        if self.version < 70002:
            self.relay = 1
        else:
            self.relay = int.from_bytes(data[-1:], byteorder='little')
        # check if this node not from future
        # print(self.start_height)
        if not self.testnet:
            if int((int(time.time()) - 1231469665) / 60 / (self.block_interval * 0.8)) < self.start_height:
                if self.verbose:
                    self.log.error('%s start_height %s  ' % (self.ip, self.start_height))
                self.version = None
                self.handshake.set_result(False)
                return
        if self.handshake.done():
            return
        else:
            print(9876, self.version_sent)
            if not self.version_sent:
                msg = self.create_message('version', self.create_version())
                self.loop.create_task(self.send_msg(msg))
            msg = self.create_message('verack', b"")
            self.loop.create_task(self.send_msg(msg))
        self.version_received = True
        if self.verack and self.version_sent:
            self.handshake.set_result(True)
        self.version_sent = True



    def getheaders(self, header_opt, checksum, data):
        error = "node internal error"
        try:
            hash_count = var_int_to_int(data[4:])
            l = var_int_len(hash_count)
            hash_data = data[l+4:hash_count * 32]
            hash_list = [hash_data[i:i+32] for i in range(0, len(hash_data), 32)]
            stop_hash = data[-32:]
            print("getheaders: hash_count", hash_count)
            print("getheaders: hash_data", hash_data)
            print("getheaders: hash_list", hash_list)
            print("getheaders: stop_hash", stop_hash)
            if len(data)!= (4+ l + hash_count*32 + 32):
                error = "invalid message size"
                raise Exception()
            self.loop.create_task(self.reply_headers(hash_list, stop_hash))
        except:
            self.loop.create_task(self.reject_message(b"getheaders", 0x10, error))



    def headers(self, header_opt, checksum, data):
        if self.getheaders_future is not None and self.getheaders_future.done() == False:
            headers_count = var_int_to_int(data[4:])
            l = var_int_len(headers_count)
            if len(data) != (4 + l + headers_count * 32 + 32):
                self.getheaders_future.cancel()
                self.loop.create_task(self.reject_message(b"getheaders", 0x10, "invalid message size"))
            hash_data = data[l + 4:headers_count * 32]

            headers = [hash_data[i:i+32] for i in range(0, len(hash_data), 32)]
            print("headers_count", len(headers))

            self.getheaders_future.set_result(headers)

        l = var_int_len(hash_count)
        hash_data = data[l+4:hash_count * 32]
        hash_list = [hash_data[i:i+32] for i in range(0, len(hash_data), 32)]
        stop_hash = data[-32:]
        print("hash_count", hash_count)
        print("hash_data", hash_data)
        print("hash_list", hash_list)
        print("stop_hash", stop_hash)

        if len(data)!= (4+ l + hash_count*32 + 32):
            self.getheaders_future.cancel()
            self.loop.create_task(self.reject_message(b"getheaders", 0x10, "invalid message size"))
            return
        self.loop.create_task(self.reply_headers(hash_list, stop_hash))


    def address(self, header_opt, checksum, data):
        t = int(time.time()) - 60 * 60 * 24
        l1 = get_var_int_len(data)
        l2 = var_int_to_int(data[:get_var_int_len(data)])
        for i in range(l2):
            timestamp = bytes_to_int(data[l1 + i * 30: l1 + i * 30 + 4], byteorder="little")
            if timestamp < t:
                # ignore older then 24 hours
                continue
            address = data[l1 + i * 30 + 12: l1 + i * 30 + 12 + 16]
            port = data[l1 + i * 30 + 12 + 16: l1 + i * 30 + 12 + 16 + 2]
            self.addresses.append({"address": bytes_to_address(address),
                                   "port": bytes_to_int(port, byteorder="big")})
        if l2 > 1:
            self.addresses_received.set_result(True)



    # outgoing messages

    async def request_headers(self, data):
        if self.getheaders_future is not None:
            return None

        self.getheaders_future = asyncio.Future()
        data = b''.join([int_to_var_int(len(data) // 32), data, b'\x00' * 32, data])
        msg = self.settings["version"].to_bytes(4,byteorder='little') + data
        header =  self.msg_header('getheaders',msg)
        await self.send_msg(header + msg)

        try:
            data =  await asyncio.wait_for(self.getheaders_future, 10)
        except Exception as err:
            self.log.debug("getheaders failed")
            return None
        self.getheaders_future = None

        return data


    async def reply_reject(self, msg, ccode, reason, data=b""):
        err = (int_to_var_int(len(msg)), msg, bytes([ccode]),
               int_to_var_int(len(reason)), reason, data)
        msg = self.create_message('reject', b"".join(err))
        await self.send_msg(msg)

    async def reply_headers(self, block_locator_hashes, hash_stop):
        print("reply_headers")
        try:
            headers = bytearray()
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT height, hash, header FROM blocks "
                                        "WHERE hash = ANY($1)  order by height;", block_locator_hashes)
                if not rows:
                    # start from genesis
                    rows = await conn.fetch("SELECT height, hash, header FROM blocks  order by height desc LIMIT 2000;")
                    headers += int_to_var_int(len(rows))
                    for row in rows:
                        headers += row["header"]
                else:
                    print("implement from location")

            await self.send_msg(self.create_message('headers', headers))
        except:
            pass



    # tasks
    async def ping_pong_task(self, timeout=20):
        count = 0
        total_time = 0
        while True:
            t = time.time()
            if t - self.last_message_timestamp > 10:
                self.ping_pong_future = asyncio.Future()
                nonce = random.randint(0, 0xFFFFFFFFFFFFFFFF).to_bytes(8, byteorder='little')
                await self.send_msg(self.create_message('ping', nonce))
                while True:
                    try:
                        nonce_recieved = await asyncio.wait_for(self.ping_pong_future, self.settings["ping_timeout"])
                    except:
                        self.log.debug('ping timeout')
                        self.disconnected.set_result(True)
                        break
                    if nonce == nonce_recieved:
                        break
                total_time += int(((time.time()) - t) * 1000)
                count += 1
                self.latency = int(total_time/count)
            await asyncio.sleep(timeout)



    # tools
    def create_version(self):
        self.version_nonce = random.randint(0, 0xffffffffffffffff)
        # 4 : version
        msg = self.settings["version"].to_bytes(4, byteorder='little')
        # 8 : services
        msg += self.settings["services"].to_bytes(8, byteorder='little')
        # 8 : timestamp
        msg += int(time.time()).to_bytes(8, byteorder='little')
        # 26 : addr_recv
        msg += self.settings["services"].to_bytes(8, byteorder='little')
        msg +=  ip_address_to_bytes(self.ip) + self.port.to_bytes(2, byteorder='big')
        # 26 : addr_from
        msg += self.settings["services"].to_bytes(8, byteorder='little')

        msg += ip_address_to_bytes(self.settings["ip"])
        msg += self.settings["port"].to_bytes(2, byteorder='big')
        # 8 : version_nonce
        msg += self.version_nonce.to_bytes(8, byteorder='little')
        # varstr : usera gent
        msg += len(self.settings["user_agent"].encode()).to_bytes(1, byteorder='little')
        msg += self.settings["user_agent"].encode()
        # 4 : start_height
        # 1 : relay
        msg += b'\x00\x00\x00\x00\x00'
        return msg



    # service

    async def get_next_message(self):
        header = b''
        magic_bytes = self.settings["magic"].to_bytes(4, byteorder='little')
        while True:
            data = b''
            try:
                header += await self.reader.readexactly(1)
                if header != magic_bytes[:len(header)]:
                    header = b''
                if header != magic_bytes:
                    continue
                header = b''
                header_opt = await self.reader.readexactly(20)
                command, length, checksum = struct.unpack('<12sLL', header_opt)
                command = command.rstrip(b'\0')
                data = await self.reader.readexactly(length)
                if self.verbose:
                    self.log.debug("<< %s" % command.decode())
            except Exception as err:
                return
            if command in self.cmd_map:
                try:
                    self.last_message_timestamp = int(time.time())
                    r = self.cmd_map[command](header_opt, checksum, data)
                    if r == -1:
                        return
                except Exception as err:
                    if self.verbose:
                        self.log.error('command processing error [%s] %s' % (str(command), err))

    def create_message(self, command, payload):
        if isinstance(command, str):
            command = command.encode()
        msg = self.settings["magic"].to_bytes(4, byteorder='little')
        msg += command.ljust(12, b"\x00")
        msg += len(payload).to_bytes(4,byteorder='little')
        msg += self.checksum(payload) + payload
        return msg

    def checksum(self, data):
        return double_sha256(data)[:4]

    def msg_header(self,msg_type,data):
        checksum =  self.checksum(data)
        return b''.join((self.settings["magic"].to_bytes(4, byteorder='little'),
                         msg_type.encode().ljust(12,b'\x00'),
                         len(data).to_bytes(4, byteorder='little', signed=False),
                         checksum))

    async def send_msg(self, data):
        try:
            # self.log.debug(">[%s]" % data)
            self.writer.write(data)
            if self.verbose:
               self.log.debug('>> %s' % data[4:16].decode())
            await self.writer.drain()
        except Exception as err:
            if self.verbose:
                self.log.debug('write error connection closed %s' % err)


