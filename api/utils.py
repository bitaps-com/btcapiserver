from binascii import hexlify
import math
import datetime
import pytz
from math import ceil
from pybtc import *


NOT_FOUND = 0
INTERNAL_SERVER_ERROR = 1
INVALID_BLOCK_POINTER = 2
REQUEST_LIMIT_EXCEEDED = 3
DECODE_ERROR = 4
PARAMETER_ERROR = 5

INVALID_FORWARDING_ADDRESS = 7
INVALID_REQUEST_CODE = 5
INVALID_TOKEN = 6
INVALID_CONFIRMATION_TARGET = 8
JSON_DECODE_ERROR = 9
FORWARDING_ADDRESS_REQUIRED = 10
INVALID_ADDRESS = 11
INVALID_PAYMENT_CODE = 12
INVALID_TRANSACTION_HASH = 13
INVALID_OUTPUT = 14
INVALID_AUTHORIZATION_CODE = 15
REQUEST_TO_CALLBACK_FAILED = 16
INVALID_DOMAIN_HASH = 17
INVALID_ACCESS_TOKEN = 18
STACK_ERROR = 19
WALLET_PAY_FAILED = 20
AUTHORIZATION_FAILED = 21
NOT_ENOUGH_FUNDS = 22
NOT_FOUND_ADDRESS = 23
UNAVAILABLE_METHOD = 99

DOMAIN_PREFIX = b'\x08T\xc8'

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


ADDRESS_TYPE_MAP = {'P2PKH': b"\x00",
                    'P2SH': b"\x01",
                    'PUBKEY': b"\x02",
                    'P2WPKH': b"\x05",
                    'P2WSH': b"\x06"}

address_types = {'0': 'P2PKH',
                 '1': 'P2SH',
                 '2': 'PUBKEY',
                 '3': 'NULL_DATA',
                 '4': 'MULTISIG',
                 '5': 'P2WPKH',
                 '6': 'P2WSH',
                 '7': 'NON_STANDARD',
                 '8': 'NULL_DATA_NON_STANDARD',
                 'null': "COINBASE"}


tx_type_map = {b"s": "sent",
               b"r": "received",
               b"m": "mined",
               b"S": "sent",
               b"R": "received",
               b"M": "mined",
               b"G": "sent",
               b"F": "received",
               b"H": "mined"
               }


out_status_map = {
    b"$": "spent",
    b"S": "spent",
    b"s": "spent",
    b"D": "spent",
    b"I": "invalid",
    b"U": "unspent",
    b"u": "unspent",
}



class APIException(Exception):

    def __init__(self, err_code, message="error", details="", status = 400):
        Exception.__init__(self)
        self.err_code = err_code
        self.message = message
        self.details = details
        self.status = status


def timestamp_to_iso8601(tms):
    return datetime.datetime.fromtimestamp(int(tms), pytz.utc).isoformat().replace('+00:00', 'Z')


def timestamp_to_day(tms):
    return datetime.datetime.fromtimestamp(int(tms), pytz.utc).strftime('%Y%m%d')


def month_stamp_parse(n):
    y = int(n / 12)
    m = n % 12
    if m == 0:
        m = 12
    else:
        y += 1
    return y, m


def raw_address_to_addrress(address, testnet=False):
    if address[0] in (0, 2):
        return hash_to_address(address[1:], witness_version=None, testnet=testnet)
    elif address[0] == 1:
        return hash_to_address(address[1:], script_hash=True, witness_version=None, testnet=testnet)
    elif address[0] == 5:
        return hash_to_address(address[1:], witness_version=0, testnet=testnet)
    elif address[0] == 6:
        return hash_to_address(address[1:], script_hash=True, witness_version=0, testnet=testnet)
    return None


def deserialize_address_stat(data):
    offset = 0
    stat = dict()
    stat["receivedTxCount"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["receivedTxCount"])
    stat["receivedAmount"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["receivedAmount"])
    stat["outputsReceivedCount"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["outputsReceivedCount"])
    stat["firstReceivedTxBlock"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["firstReceivedTxBlock"])
    stat["lastReceivedTxBlock"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["lastReceivedTxBlock"])
    stat["largestTxAmount"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["largestTxAmount"])
    stat["largestTxId"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["largestTxId"])
    stat["sentTxCount"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["sentTxCount"])
    stat["sentAmount"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["sentAmount"])
    stat["outputsSpentCount"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["outputsSpentCount"])
    stat["firstSentTxBlock"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["firstSentTxBlock"])
    stat["lastSentTxBlock"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["lastSentTxBlock"])
    stat["mined"] = c_int_to_int(data[offset:])
    offset += c_int_len(stat["mined"])
    if offset < len(data):
        stat["invalidTx"] = c_int_to_int(data[offset:])
        offset += c_int_len(stat["invalidTx"])
        if offset < len(data):
            stat["pendingReceivedTxCount"] = c_int_to_int(data[offset:])
            offset += c_int_len(stat["pendingReceivedTxCount"])
            stat["pendingReceivedAmount"] = c_int_to_int(data[offset:])
            offset += c_int_len(stat["pendingReceivedAmount"])
            stat["outputsUnconfirmedReceivedCount"] = c_int_to_int(data[offset:])
            offset += c_int_len(stat["outputsUnconfirmedReceivedCount"])
            stat["pendingSentTxCount"] = c_int_to_int(data[offset:])
            offset += c_int_len(stat["pendingSentTxCount"])
            stat["pendingSentAmount"] = c_int_to_int(data[offset:])
            offset += c_int_len(stat["pendingSentAmount"])
            stat["outputsUnconfirmedSpentCount"] = c_int_to_int(data[offset:])
            offset += c_int_len(stat["outputsUnconfirmedSpentCount"])
        else:
            stat["pendingReceivedTxCount"] = 0
            stat["pendingReceivedAmount"] = 0
            stat["outputsUnconfirmedReceivedCount"] = 0
            stat["pendingSentTxCount"] = 0
            stat["pendingSentAmount"] = 0
            stat["outputsUnconfirmedSpentCount"] = 0
    else:
        stat["invalidTx"] = 0
        stat["pendingReceivedTxCount"] = 0
        stat["pendingReceivedAmount"] = 0
        stat["outputsUnconfirmedReceivedCount"] = 0
        stat["pendingSentTxCount"] = 0
        stat["pendingSentAmount"] = 0
        stat["outputsUnconfirmedSpentCount"] = 0
    return stat


def deserialize_tx_map(s):
    offset = 0
    data = dict()
    data["received"] = c_int_to_int(s[offset:])
    offset += c_int_len(data["received"])
    data["sent"] = c_int_to_int(s[offset:])
    offset += c_int_len(data["sent"])
    data["countReceived"] = c_int_to_int(s[offset:])
    offset += c_int_len(data["countReceived"])
    data["totalReceived"] = c_int_to_int(s[offset:])
    offset += c_int_len(data["totalReceived"])
    if offset < len(s):
        data["countSent"] = c_int_to_int(s[offset:])
        offset += c_int_len(data["countSent"])
        data["totalSent"] = c_int_to_int(s[offset:])
    else:
        data["countSent"] = 0
        data["totalSent"] = 0
    return data


def serialize_tx_data(tx):
    data = b""
    h = 0 if not tx["coinbase"] else 128
    if tx["coinbase"]:
        if tx["segwit"]:
            h = h + 64
            data += tx["hash"]
    if tx["version"] > 14:
        h += 48
        data += int_to_c_int(tx["version"])
    else:
        h = h + (tx["version"] << 2)
    if tx["lockTime"]:
        h += 2
        data += int_to_c_int(tx["lockTime"])
    if "data" in tx and tx["data"]:
        h += 1
        data += tx["data"]
    return h.to_bytes(1, "little") + data


def deserialize_tx_data(data):
    h = data[0]
    offset = 1
    coinbase = True if h & 128 else False
    if coinbase &  (h & 64):
        witnesshash = data[1:33]
        offset = 33
    else:
        witnesshash = None
    if h & 60 == 0:
        version = c_int_to_int(data[offset:])
        offset = offset + c_int_len(version)
    else:
        version = (h >> 2) & 0b1111
    if h & 2:
        locktime = c_int_to_int(data[offset:])
        offset = offset + c_int_len(locktime)
    else:
        locktime = 0
    if h & 1:
        tx_data = data[offset:]
    else:
        tx_data = None
    if coinbase:
        witnesshash = b"\x00" * 32
    return [coinbase, witnesshash, version, locktime, tx_data]



def deserialize_input_script(data, coinbase):
    pubkey_script = b''
    sig_script = b''
    witness = []
    address = b""
    pubkey_script_type = None
    offset = 0
    l = c_int_to_int(data[offset:])
    offset += c_int_len(l)
    if l:
        pubkey_script = data[offset:offset + l]
        offset += l
    offset += 1
    l = c_int_to_int(data[offset:])
    offset += c_int_len(l)
    if l:
        sig_script = data[offset:offset + l]
        offset += l
    inverted_sequence = c_int_to_int(data[offset:])
    offset += c_int_len(inverted_sequence)
    sequence = 0xffffffff - inverted_sequence
    while data[offset:]:
        l = c_int_to_int(data[offset:])
        offset += c_int_len(l)
        # if l:
        witness.append(data[offset:offset + l])
        offset += l
    if pubkey_script:
        s = parse_script(pubkey_script)
        if "addressHash" in s:
            address = b''.join([int_to_bytes(s["nType"]),
                                s["addressHash"]]) if "addressHash" in s else b''
        pubkey_script_type = s["type"]

    return {"pubkey_script": pubkey_script,
            "sig_script": sig_script,
            "sequence": sequence,
            "witness": witness if not coinbase else [],
            "address": address,
            "pubkey_script_type": pubkey_script_type}


def day_last_block(app, day):
    try:
        block = app["day_map_block"][day]
    except:
        if day < ceil(app["block_map_time"][0] / 86400):
            block = 0
        else:
            block = app["last_block"]
    return block


def get_confirmations(app, block):
    confirmations = app["last_block"] - block + 1
    if confirmations < 1:
        confirmations = 1
    return confirmations


def block_to_timestamp(app, block):
    try:
        t = app["block_map_time"][block]
    except:
        t = 0
    return t

class Cache():
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

def decode_block_tx(block):
    s = get_stream(block)
    b = dict()
    b["amount"] = 0
    b["size"] = int(len(block)/2)
    b["strippedSize"] = 80
    b["version"] = unpack("<L", s.read(4))[0]
    b["versionHex"] = pack(">L", b["version"]).hex()
    b["previousBlockHash"] = rh2s(s.read(32))
    b["merkleRoot"] = rh2s(s.read(32))
    b["time"] = unpack("<L", s.read(4))[0]
    b["bits"] = s.read(4)
    b["target"] = bits_to_target(unpack("<L", b["bits"])[0])
    b["targetDifficulty"] = target_to_difficulty(b["target"])
    b["target"] = b["target"].to_bytes(32, byteorder="little")
    b["nonce"] = unpack("<L", s.read(4))[0]
    s.seek(-80, 1)
    b["header"] = s.read(80)
    b["bits"] = rh2s(b["bits"])
    b["target"] = rh2s(b["target"])
    b["hash"] = double_sha256(b["header"], hex=0)
    b["hash"] = rh2s(b["hash"])
    b["txList"] = list()
    b["tx"] = list()
    for i in range(var_int_to_int(read_var_int(s))):
        b["tx"].append(Transaction(s, format="decoded", keep_raw_tx=False))
        b["txList"].append(b["tx"][i]["txId"])
        b["tx"][i]["coinbase"] = False if i else True

    return b