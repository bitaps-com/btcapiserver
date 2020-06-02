import math
from collections import OrderedDict
from pybtc import  c_int_to_int, c_int_len, int_to_c_int, get_stream, read_c_int
import asyncio

log_level_map = {"DEBUG": 10,
                 "debug": 10,
                 "INFO": 20,
                 "info": 20,
                 "WARNING": 30,
                 "warning": 30,
                 "ERROR": 40,
                 "error": 40,
                 "CRITICAL": 50,
                 "critical": 50}

def chunks_by_count(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def chunks_by_threads(l, n):
    n = math.ceil(len(l)/n)
    return chunks_by_count(l,n)




def chunks(a, max_threads, min_thread_limit):
    if not isinstance(a, list):
        a = list(a)
    if len(a)/min_thread_limit > max_threads:
        return chunks_by_threads(a, max_threads)
    else:
        return chunks_by_count(a, min_thread_limit)

def decode_tx_map_data(b):
    r = c_int_to_int(b)
    f = c_int_len(r)
    rc = c_int_to_int(b[f:])
    f += c_int_len(rc)
    if f >= len(b):
        s = 0
        sc = 0
    else:
        s = c_int_to_int(b[f:])
        f += c_int_len(s)
        sc = c_int_to_int(b[f:])
    return r, rc, s, sc



class Cache():
    def __init__(self, max_size=1000):
        self._store = OrderedDict()
        self._max_size = max_size
        self.clear_tail = False
        self._requests = 0
        self._hit = 0

    def set(self, key, value):
        self._check_limit()
        self._store[key] = value

    def _check_limit(self):
        if len(self._store) >= self._max_size:
            self.clear_tail = True
        if self.clear_tail:
            if len(self._store) >= int(self._max_size * 0.75):
                for i in range(20):
                    self._store.popitem(last=False)
            else:
                self.clear_tail = False

    def get(self, key):
        self._requests += 1
        try:
            i = self._store[key]
            self._hit += 1
            return i
        except:
            return None

    def pop(self, key):
        self._requests += 1
        try:
            data = self._store[key]
            del self._store[key]
            self._hit += 1
            return data
        except:
            return None

    def len(self):
        return len(self._store)

    def hitrate(self):
        if self._requests:
            return self._hit / self._requests
        else:
            return 0

class ListCache():
    def __init__(self, max_size=2000):
        self.items = list()
        self._max_size = max_size


    def set(self, value):
        self._check_limit()
        self.items.append(value)

    def _check_limit(self):
        if len(self.items) >= self._max_size:
            self.items.pop(0)


    def get_last(self):
        try:
            return self.items[-1]
        except:
            return None

    def len(self):
        return len(self.items)

async def get_pipe_reader(fd_reader, loop):
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    try:
        await loop.connect_read_pipe(lambda: protocol, fd_reader)
    except:
        return None
    return reader

async def get_pipe_writer(fd_writer, loop):
    try:
        wt, wp = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, fd_writer)
        writer = asyncio.streams.StreamWriter(wt, wp, None, loop)
    except:
        return None
    return writer

async def pipe_get_msg(reader):
    while True:
        try:
            msg = await reader.readexactly(1)
            if msg == b'M':
                msg = await reader.readexactly(1)
                if msg == b'E':
                    msg = await reader.readexactly(4)
                    c = int.from_bytes(msg, byteorder='little')
                    msg = await reader.readexactly(c)
                    if msg:
                        return msg[:20].rstrip(), msg[20:]
            if not msg:
                raise EOFError
        except:
            raise EOFError

def pipe_sent_msg(msg_type, msg, writer):
    msg_type = msg_type[:20].ljust(20)
    msg = b''.join((msg_type, msg))
    msg = b''.join((b'ME', len(msg).to_bytes(4, byteorder='little'), msg))
    writer.write(msg)

def seconds_to_age(time):
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    return "%d:%d:%d:%d" % (day, hour, minutes, seconds)


def format_bytes(size):
    if size == 0:
        return """%s bytes""" % size
    order = int(math.log(size, 1024) // 1)
    if order == 0:
        return """%s bytes""" % size
    if order == 1:
        return """%s Kb""" %  round( size / 1024, 2)
    if order == 2:
        return """%s Mb""" % round(size / (1024 ** 2), 2)
    return """%s Gb""" % round(size / (1024 ** 3), 2)

def format_vbytes(size):
    if size == 0:
        return """%s vBytes""" % size
    order = int(math.log(size, 1000) // 1)
    if order == 0:
        return """%s vBytes""" % size
    if order == 1:
        return """%s vKb""" %  round( size / 1000, 2)
    if order == 2:
        return """%s vMb""" % round(size / (1000 ** 2), 2)
    return """%s vGb""" % round(size / (1000 ** 3), 2)


def serialize_address_data(received_count, received_amount, coins,  frp, lra, lrp,
                           sent_count, sent_amount, coins_destroyed, fsp, lsa, lsp):
    if sent_count:
        return b"".join((int_to_c_int(received_count),
                         int_to_c_int(received_amount),
                         int_to_c_int(coins),
                         int_to_c_int(frp),
                         int_to_c_int(lra),
                         int_to_c_int(lrp),

                         int_to_c_int(sent_count),
                         int_to_c_int(sent_amount),
                         int_to_c_int(coins_destroyed),
                         int_to_c_int(fsp),
                         int_to_c_int(lsa),
                         int_to_c_int(lsp)
                         ))
    else:
        return b"".join((int_to_c_int(received_count),
                         int_to_c_int(received_amount),
                         int_to_c_int(coins),
                         int_to_c_int(frp),
                         int_to_c_int(lra),
                         int_to_c_int(lrp)))

def deserialize_address_data(data):
        data = get_stream(data)
        received_count = c_int_to_int(read_c_int(data))
        received_amount = c_int_to_int(read_c_int(data))
        coins = c_int_to_int(read_c_int(data))
        frp = c_int_to_int(read_c_int(data))
        lra = c_int_to_int(read_c_int(data))
        lrp = c_int_to_int(read_c_int(data))
        try:
            sent_count = c_int_to_int(read_c_int(data))
            sent_amount = c_int_to_int(read_c_int(data))
            coins_destroyed = c_int_to_int(read_c_int(data))
            fsp = c_int_to_int(read_c_int(data))
            lsa = c_int_to_int(read_c_int(data))
            lsp = c_int_to_int(read_c_int(data))
        except:
            sent_count = 0
            sent_amount = 0
            coins_destroyed = 0
            fsp = None
            lsa = None
            lsp = None
        return (received_count, received_amount, coins, frp, lra, lrp,
                sent_count, sent_amount, coins_destroyed, fsp, lsa, lsp)

