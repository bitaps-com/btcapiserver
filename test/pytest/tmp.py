import unittest
import configparser
from pybtc import *
from math import *
import requests
from orderedset import OrderedSet
import zlib
from pprint import pprint
import psycopg2
config_file =   "/config/btcapi-server.conf"
config = configparser.ConfigParser()
config.read(config_file)

postgres_dsn = config["POSTGRESQL"]["dsn"]
option_transaction = True if config["OPTIONS"]["transaction"] == "on" else False
option_merkle_proof = True if config["OPTIONS"]["merkle_proof"] == "on" else False
option_address_state = True if config["OPTIONS"]["address_state"] == "on" else False
option_address_timeline = True if config["OPTIONS"]["address_timeline"] == "on" else False
option_blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False

option_blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
option_transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False
base_url = config["SERVER"]["api_endpoint_test_base_url"]

def encode_dhcs(elements, min_bits_threshold=20):
    # Delta-Hoffman coded set
    data_sequence = bitarray()
    data_sequence_append = data_sequence.append

    deltas_bits = deque()
    deltas_bits_map_freq = dict()
    last = 0
    last2 = 0
    min_v = 9999999999999
    max_v = 0
    for value in   elements:
        if min_v > value:
            min_v = value
        if max_v < value:
            max_v = value


    for value in  sorted(elements):
        # value = max_v - (value - min_v)
        delta =   value - last
        # print(">>", delta)

        bits = delta.bit_length()
        if bits < min_bits_threshold:
            bits =  min_bits_threshold

        deltas_bits.append(bits)

        try:
            deltas_bits_map_freq[bits] += 1
        except:
            deltas_bits_map_freq[bits] = 1

        while bits > 0:
            data_sequence_append(delta & (1 << (bits - 1)))
            bits -= 1
        last = value
    print("min_v", min_v, "max_v", max_v)



    # huffman encode round 1
    # encode bits length sequence to byte string
    codes_round_1 = huffman_code(huffman_tree(deltas_bits_map_freq))
    r = bitarray()
    r.encode(codes_round_1, deltas_bits)
    bits_sequence = r.tobytes()
    bits_sequnce_len_round_1 = r.length()

    # huffman encode round 2
    # encode byte string
    deltas_bits = deque()
    deltas_bits_map_freq = dict()
    for i in bits_sequence:
        b = i >> 4
        c = i & 0b1111
        deltas_bits.append(b)
        try:
            deltas_bits_map_freq[b] += 1
        except:
            deltas_bits_map_freq[b] = 1

        deltas_bits.append(c)
        try:
            deltas_bits_map_freq[c] += 1
        except:
            deltas_bits_map_freq[c] = 1

    codes_round_2 = huffman_code(huffman_tree(deltas_bits_map_freq))
    r = bitarray()
    r.encode(codes_round_2, deltas_bits)
    bits_sequnce_len_round_2 = r.length()
    bits_sequence = r.tobytes()


    code_table_1 = int_to_var_int(len(codes_round_1))
    for code in codes_round_1:
        code_table_1 += int_to_var_int(code)
        code_table_1 += int_to_var_int(codes_round_1[code].length())
        code_table_1 += b"".join([bytes([i]) for i in codes_round_1[code].tolist()])

    code_table_2 = int_to_var_int(len(codes_round_2))
    for code in codes_round_2:
        code_table_2 += int_to_var_int(code)
        code_table_2 += int_to_var_int(codes_round_2[code].length())
        code_table_2 += b"".join([bytes([i]) for i in codes_round_2[code].tolist()])


    d_filter_len = data_sequence.length()
    d_filter_string = data_sequence.tobytes()

    return  b"".join((code_table_1,
                      code_table_2,
                      int_to_var_int(bits_sequnce_len_round_1),
                      int_to_var_int(bits_sequnce_len_round_2),
                      bits_sequence,
                      int_to_var_int(d_filter_len),
                      d_filter_string))

def encode_huffman(elements):
    map_freq = dict()
    for value in  elements:
        try:
            map_freq[value] += 1
        except:
            map_freq[value] = 1
    bitstr = bitarray()
    codes = huffman_code(huffman_tree(map_freq))
    bitstr.encode(codes, elements)

    code_table_1 = int_to_var_int(len(codes))
    for code in codes:
        code_table_1 += int_to_var_int(code)
        code_table_1 += int_to_var_int(codes[code].length())
        code_table_1 += b"".join([bytes([i]) for i in codes[code].tolist()])

    return bitstr.tobytes() + code_table_1

class AddressAPIEndpointsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("\nTesting Address API endpoints:\n")

    # def test_address_state(self):
    #     print("/rest/address/state/{address}:\n")
    #
    #     r = requests.get(base_url + "/rest/address/state/37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7")
    #     self.assertEqual(r.status_code, 200)
    #     d = r.json()["data"]
    #     pprint(d)
    #
    #     print("OK\n")
    #

    # def test_get_address_by_list(self):
    #     print("/rest/addresses/state/by/address/list:\n")
    #     r = requests.post(base_url + "/rest/addresses/state/by/address/list",
    #                       json=["37P8thrtDXb6Di5E7f4FL3bpzum3fhUvT7",
    #                             "17ami8DrFbU625nzButiQrFcqEtcCMtUqH",
    #                             "1G2iR6zmqqBJkcZateaoRAZtgEfETbjzDE"])
    #     self.assertEqual(r.status_code, 200)
    #     pprint(r.json())
    #     print("OK\n")

    def test_raw_block_filters(sels):

        print("Test raw filters:")

        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()
        last = 0



        for h in range(0, 570000):
            cursor.execute("SELECT height, filter  "
                           " FROM raw_block_filters "
                           " WHERE height = %s  LIMIT 1;", (h,) )

            rows = cursor.fetchall()
            r = rows[0][1].tobytes()
            s2 = set([ r[i:i + 13] for i in range(0, len(r), 13)])
            s = set([ bytes_to_int(r[i+5:i + 13], "little") for i in range(0, len(r), 13)])

            # print(h, len(s))
            # print(s2)
            cursor.execute("SELECT   address  "
                           "FROM transaction_map "
                           "where  pointer >= %s and pointer < %s;", (h << 39, (h +1) << 39))
            rows = cursor.fetchall()
            # print(">>", rows)

            for row in rows:
                if row[0][0] == b'\x02':
                    k = parse_script(bytes(row[0][1:]))
                    if  siphash(k["addressHash"]) not in s:
                        # print(row[0][1:].hex())
                        print(siphash(k["addressHash"]).to_bytes(8,byteorder="little"))
                        print(s)
                        print(k)
                        raise Exception()
                elif row[0][0] in (b'\x00',b'\x01', b'\x05', b'\x06'):
                    hk = siphash(bytes(row[0][1:]))
                    if  hk not in s:
                        # print(h.hex())
                        raise Exception()

            print(h, "OK", len(rows))

        return



    def test_block_filters(sels):
        return
        print("Test raw filters:")

        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()
        last = 0
        batch_size = 4032
        a_total = 10000


        for h in range(169344, 200000):
            cursor.execute("SELECT height, filter  "
                           " FROM block_filter "
                           " WHERE height = %s and type = 1  LIMIT 1;", (h,) )

            rows = cursor.fetchall()

            stream = get_stream(rows[0][1].tobytes())
            r = var_int_to_int(read_var_int(stream))

            if r:
                a_total = r
                print("Batch start", a_total)

            c = var_int_to_int(read_var_int(stream))
            l = var_int_to_int(read_var_int(stream))
            f = stream.read(l)
            P = floor(log2(a_total / 1.497137))
            M = ceil(1.49713 * (2 ** P))
            F = c * M
            s = decode_gcs(f, c, P)

            cursor.execute("SELECT height, filter  "
                           " FROM block_filter "
                           " WHERE height = %s and type = 16  LIMIT 1;", (h,) )

            rows = cursor.fetchall()
            batch_block_index = h  % batch_size


            stream = get_stream(rows[0][1].tobytes())
            c2 = var_int_to_int(read_var_int(stream))
            l = var_int_to_int(read_var_int(stream))
            f = stream.read(l)
            P2 = ceil(log2((((batch_block_index) // 144) + 1) * 3 / 1.497137))
            F2 = c2 * ceil(1.49713 * 2 ** P2)
            s2 = decode_gcs(f, c2, P2)



            cursor.execute("SELECT   address  "
                           "FROM transaction_map "
                           "where  pointer >= %s and pointer < %s;", (h << 39, (h +1) << 39))
            rows = cursor.fetchall()
            # print(">>", rows)

            for row in rows:
                if row[0][0] == b'\x02':

                    k = parse_script(bytes(row[0][1:]))

                    if  map_into_range(siphash(k["addressHash"]), F) not in s:
                        if map_into_range(siphash(k["addressHash"]), F2) not in s2:
                            print("len addresses", len(rows))
                            print("len filters", len(s), len(s2))
                            print(s2)
                            print(map_into_range(siphash(k["addressHash"]), F2))
                            for ik in rows:
                                try:
                                    print(bytes_to_int(ik[0][0]), parse_script(bytes(ik[0][1:]))["addressHash"].hex())
                                except:
                                    print(bytes_to_int(ik[0][0]), ik[0][1:].hex())

                            # print(row[0][1:].hex())
                            raise Exception()
                elif row[0][0] in (b'\x00'):
                    hk = map_into_range(siphash(bytes(row[0][1:])), F)
                    if  hk not in s:
                        hk = map_into_range(siphash(bytes(row[0][1:])), F2)
                        if hk not in s2:
                            print(row[0][0])
                            raise Exception()

            print(h, "OK -> ", P, P2, len(rows))

        return

        c = 0
        for h in range(0, 20):
            cursor.execute("SELECT height, filter  "
                           " FROM block_batch_filters_p2pkh_uncompressed "
                           " WHERE height = %s  LIMIT 1;", (h,) )

            rows = cursor.fetchall()
            f = rows[0][1].tobytes()
            hash_list = set([bytes_to_int(f[i:i + 8], byteorder="big") for i in range(0, len(f), 8)])


            cursor.execute("SELECT   address  "
                           "FROM transaction_map "
                           "where  pointer >= %s and pointer < %s;", (h << 39, (h +1) << 39))
            rows = cursor.fetchall()
            # print(">>", rows)
            bc = 0
            for row in rows:
                if row[0][0] == b'\x00':

                    c+= 1
                    bc+= 1
                    hk = bytes(row[0][1:])
                    if  map_into_range(siphash(hk),10 ** 13) not in hash_list:
                        # print(row[0][1:].hex())
                        print(map_into_range(siphash(hk),10 ** 13), hash_list)
                        raise Exception()
                elif row[0][0] == b'\x02':


                    c += 1
                    bc += 1
                    hk = parse_script(bytes(row[0][1:]))["addressHash"]
                    if  map_into_range(siphash(hk),10 ** 13) not in hash_list:
                        # print(h.hex())
                        print(b'\x02', bytes(row[0][1:]).hex(), parse_script(bytes(row[0][1:])))
                        print(map_into_range(siphash(hk), 10 ** 13), hash_list)
                        raise Exception()

            print(h, bc, "OK")
        print(c, "OK")

        c = 0
        for h in range(0, 2200):
            cursor.execute("SELECT height, filter  "
                           " FROM block_batch_filters_p2sh_uncompressed "
                           " WHERE height = %s  LIMIT 1;", (h,) )

            rows = cursor.fetchall()
            f = rows[0][1].tobytes()
            hash_list = set([bytes_to_int(f[i:i + 8], byteorder="big") for i in range(0, len(f), 8)])


            cursor.execute("SELECT   address  "
                           "FROM transaction_map "
                           "where  pointer >= %s and pointer < %s;", (h << 39, (h +1) << 39))
            rows = cursor.fetchall()
            # print(">>", rows)
            bc = 0
            for row in rows:
                if row[0][0] == b'\x01':

                    c+= 1
                    bc+= 1
                    hk = bytes(row[0][1:])
                    if  map_into_range(siphash(hk),10 ** 13) not in hash_list:
                        # print(row[0][1:].hex())
                        print(map_into_range(siphash(hk),10 ** 13), hash_list)
                        raise Exception()


            print(h, bc, "OK")

        c = 0
        for h in range(500000, 520000):
            cursor.execute("SELECT height, filter  "
                           " FROM block_batch_filters_p2wpkh_uncompressed "
                           " WHERE height = %s  LIMIT 1;", (h,) )

            rows = cursor.fetchall()
            f = rows[0][1].tobytes()
            hash_list = set([bytes_to_int(f[i:i + 8], byteorder="big") for i in range(0, len(f), 8)])


            cursor.execute("SELECT   address  "
                           "FROM transaction_map "
                           "where  pointer >= %s and pointer < %s;", (h << 39, (h +1) << 39))
            rows = cursor.fetchall()
            # print(">>", rows)
            bc = 0
            for row in rows:
                if row[0][0] == b'\x05':

                    c+= 1
                    bc+= 1
                    hk = bytes(row[0][1:])
                    if  map_into_range(siphash(hk),10 ** 13) not in hash_list:
                        # print(row[0][1:].hex())
                        print(map_into_range(siphash(hk),10 ** 13), hash_list)
                        raise Exception()


            print(h, bc, "OK")


        print(c, "OK")




        return



        b = bytes(row[1])
        N = c_int_to_int(b)
        i = c_int_len(N)
        f = zlib.decompress(b[i:])
        h = row[0]
        f = set(decode_gcs(f, N, P=block_filter_bits))
        print(">>>",len(f),N,  h)

        cursor.execute("SELECT   address, pointer, amount  "
                                    "FROM transaction_map "
                                    "where  pointer > %s and pointer < %s;" ,  (last, h << 39,) )
        last = (h +1) << 39
        rows = cursor.fetchall()


        print("affected addresses ", len(rows))
        M = block_filter_fps
        N = block_filter_capacity
        elements = [e for e in rows]

        for a in elements:
            if map_into_range(siphash(bytes(a[0])), N * M) not in f:
                print("false negative!",  bytes(a[0]).hex(), a[1]>>39, a[2] )
        print("x", map_into_range(siphash(bytes(a[0])), N * M))
        pk = map_into_range(siphash(bytes(a[0])), N * M)



        print("create monitoring addresses")
        m_addresses = set()
        while len(m_addresses) < 20_000:
            m_addresses.add(sha256(int_to_bytes(random.randint(1, 0xFFFFFFFFFFFFFFFFFFFF)))[:21])
        print("monitoring addresses created ")
        f = create_gcs(m_addresses, N = block_filter_capacity, M=block_filter_fps, P=block_filter_bits)
        l = decode_gcs(f, len(m_addresses),P=block_filter_bits)
        # l.append(pk)
        print("monitoring set created ", len(f), len(l))


        cursor.execute("SELECT height, filter  "
                                      "FROM block_filters_checkpoints2 "
                                   "order by height asc limit 20;" )
        rows = cursor.fetchall()
        for row in rows:
            print("checkpoint %s length compressed filter %s " % (row[0], len(row[1])))
            b = bytes(row[1])
            N = c_int_to_int(b)
            i = c_int_len(N)
            f = zlib.decompress(b[i:])
            print("decompressed size %s" % len(f))
            f = set(decode_gcs(f, N, P=block_filter_bits))
            print("len ", len(f))
            e = 0
            for i in l:
                if i in f:
                    e += 1
            print(i, f.pop())
            print("false positive ", e)




        print("OK\n")









    def test_bloom2(sels):
        return
        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()
        last = 0
        print("create monitoring addresses")
        m_addresses = set()
        while len(m_addresses) < 10_000:
            m_addresses.add(sha256(int_to_bytes(random.randint(1, 0xFFFFFFFFFFFFFFFFFFFF)))[:21])
        print("monitoring addresses created ")
        f = create_gcs(m_addresses, N = block_filter_capacity, M=block_filter_fps, P=block_filter_bits)
        l = decode_gcs(f, len(m_addresses),P=block_filter_bits)
        # l.append(pk)
        print("monitoring set created ", len(f), len(l))


        cursor.execute("SELECT height, filter  "
                                      "FROM block_filters_checkpoints "
                                   "order by height asc ;" )
        rows = cursor.fetchall()
        for row in rows:
            print("checkpoint %s length compressed filter %s " % (row[0], len(row[1])))
            b = bytes(row[1])
            N = c_int_to_int(b)
            i = c_int_len(N)
            f = zlib.decompress(b[i:])
            print("decompressed size %s" % len(f))
            f = set(decode_gcs(f, N, P=block_filter_bits))
            print("len ", len(f))
            e = 0
            for i in l:
                if i in f:
                    e += 1
            print(i, f.pop())
            print("false positive ", e)




        print("OK\n")



"""
P = log2(M / 1.497137)

144*7 = 1008
14.625988960266113 base  12.2702054977417 dublicates  2.324991226196289
bip158 = 17
17.29 19.92
savings - 22%

144*7*2 = 2016
29.00393772125244 base  23.103970527648926 dublicates  5.838413238525391
bip158 = 34
32.69  39.35 17
savings - 24.7%


144*7*3 = 3024
36.56511116027832 base  29.295815467834473 duplicates  7.176980018615723
43.67845344543457 base  32.42870235443115 dublicates  11.157435417175293
bip158 = 51

savings 28.4%


144*7*4 = 4032
47.98566436767578 base  37.96860599517822 duplicates  9.893980979919434
58.13447093963623 base  42.028982162475586 dublicates  15.98241138458252
bip158 = 69

savings 30.4%



144*7*5 = 5040
58.93297290802002 base  46.25423526763916 duplicates  12.524898529052734
bip158 = 86

savings 31.3%


144*7*6 = 6048
70.67300224304199 base  55.19221878051758 duplicates  15.296182632446289
bip158 = 104

savings 32%

144*7*6 = 7056
81.90222549438477 base  63.58853816986084 dublicates  18.0983247756958
bip158 = 122
savings 32.7%

93.32631492614746 base  72.14700412750244 dublicates  20.933186531066895

458065 7545 0 0.0 55997498 21565930
0.39
min_v 2195883309 max_v 9999447028750
30.49609375 30.46484375 0.0
229.48994731903076 base  89.69472408294678 dublicates  139.54909896850586
min_v 846115 max_v 9999999919445
53.14746570587158 55729157 21565930

"""