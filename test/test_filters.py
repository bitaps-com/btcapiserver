import unittest
import configparser
from pybtc import *
import psycopg2
from collections import OrderedDict, deque

config_file =   "/config/btcapi-server.conf"
config = configparser.ConfigParser()
config.read(config_file)
postgres_dsn = config["POSTGRESQL"]["dsn"]
option_block_batch_filters = True if config["OPTIONS"]["block_batch_filters"] == "on" else False
option_transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False


class FiltersTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("\nTesting block batch filters:\n")

    def test_filters(self):
        print("Test filters:")
        if not option_transaction_history:
            print("Skipped ...")
            print("Filter testing is only available if the transaction history option is enabled!\n")
            return
        if not option_block_batch_filters:
            print("Skipped ...")
            print("Block batch filters option is not enabled!\n")
            return
        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()
        h = 0

        batch_size = 144 * 7
        last_batch_height = (h // batch_size - 1) * batch_size
        h = last_batch_height + batch_size
        n_type_map_filter_type = {0: 2, 1: 4, 2: 1, 5: 8, 6: 16}


        batch_map = {1: dict(), 2: dict(), 4: dict(), 8: dict(), 16: dict()}
        batch = {1: set(), 2: set(), 4: set(), 8: set(), 16: set()}
        element_index = {1: 0, 2: 0, 4: 0, 8: 0, 16: 0}
        last_hash = {1: None, 2: None, 4: None, 8: None, 16: None}


        while 1:
            if h % (144 * 7) == 0:
                batch_map = {1: dict(), 2: dict(), 4: dict(), 8: dict(), 16: dict()}
                element_index = {1: 0, 2: 0, 4: 0, 8: 0, 16: 0}
                batch = {1: set(), 2: set(), 4: set(), 8: set(), 16: set()}

            pointers_fingerprint = {1: list(), 2: list(), 4: list(), 8: list(), 16: list()}

            cursor.execute("SELECT type, height, filter, hash  FROM  block_filter WHERE height = %s;", (h,) )

            rowsf = cursor.fetchall()
            if not rowsf:
                break

            v_set = {1: set(), 2: set(), 4: set(), 8: set(), 16: set()}
            cursor.execute("SELECT  address, pointer FROM transaction_map "
                           "where  pointer >= %s and pointer < %s;", (h << 39, (h + 1) << 39))
            rows = cursor.fetchall()

            mp = dict()
            items = {1: set(), 2: set(), 4: set(), 8: set(), 16: set()}
            tx_filters = {1: dict(), 2: dict(), 4: dict(), 8: dict(), 16: dict()}

            for row in rows:
                ti =  (row[1] >> 20) & (2 ** 19 - 1)
                if h == 436462:
                    if row[0][0][0] > 1:
                        print("-", row[0][0][0])
                if row[0][0][0] not in (0,1,2,5,6):
                    continue
                f_type = n_type_map_filter_type[row[0][0][0]]
                if row[0][0][0] == 2:
                    k = parse_script(bytes(row[0][1:]))
                    e = k["addressHash"][:20]
                elif row[0][0][0] in (0, 1, 5, 6):
                    e = bytes(row[0][1:21])
                items[f_type].add(ti.to_bytes(4, "little") + e)
                q = map_into_range(siphash(e), 2**32)
                try:
                    tx_filters[f_type][ti].add(q.to_bytes(4, "little"))
                except:
                    tx_filters[f_type][ti] = {q.to_bytes(4, "little")}

            for f_type in items:
                for e in items[f_type]:
                    k = e[4:]
                    e = map_into_range(siphash(k), 2**32)
                    mp[e] = k
                    v_set[f_type].add(e)

            for f_type in items:
                if v_set[f_type]:
                    d = bytearray()
                    for i in sorted(tx_filters[f_type].keys()):
                        try:
                            d += b"".join(sorted(tx_filters[f_type][i]))
                        except:
                            pass
                    pointers_fingerprint[f_type] = ripemd160(sha256(d))
                else:
                    pointers_fingerprint[f_type] = b""


            for row in rowsf:
                f_type = row[0]
                r = bytes(row[2])
                stream = get_stream(r)
                l = var_int_to_int(read_var_int(stream))
                if l:
                    elements = decode_gcs(stream.read(l))
                else:
                    elements = []

                l = var_int_to_int(read_var_int(stream))
                if l:
                    duplicates = decode_gcs(stream.read(l))
                else:
                    duplicates = []
                fingerprint = stream.read(20)

                f = pointers_fingerprint[f_type]

                if f != fingerprint:
                    print(fingerprint, f)
                    raise Exception("transaction filters fingerprint error")

                for e in elements:
                    batch_map[f_type][element_index[f_type]] = e
                    batch[f_type].add(e)
                    element_index[f_type] += 1

                v = set(elements)
                for d in duplicates:
                    v.add(batch_map[f_type][d])

                delta = v - v_set[f_type]

                if delta:
                    raise Exception("filter content error")

                if last_hash[f_type]:
                    last_hash[f_type] = sha256(last_hash[f_type] + sha256(r))
                elif r:
                    last_hash[f_type] = sha256(b"\00" * 32 + sha256(r))

                if last_hash[f_type]:
                    if  last_hash[f_type] != bytes(row[3]):
                        print("filter type", f_type, "height", h)
                        print(last_hash[f_type], bytes(row[3]))
                        raise Exception("filter header hash error")

                if elements or duplicates:
                    print(h, "[%s]" % f_type, len(v), len(elements), len(duplicates),"OK")
            h+=1





    def test_1k_FPR(self):
        self.false_positive_test(2, 1000)

    def test_10k_FPR(self):
        self.false_positive_test(2, 10000)

    def false_positive_test(self, f_type, address_count, batch_size = 144 * 7 , verbose = 0):

        print("Test filters type %s for %s monitoring addresses set:" % (f_type, address_count))
        if not option_transaction_history:
            print("Skipped ...")
            print("Filter testing is only available if the transaction history option is enabled!\n")
            return
        if not option_block_batch_filters:
            print("Skipped ...")
            print("Block batch filters option is not enabled!\n")
            return



        addresses = dict()
        while len(addresses) < address_count:
            i = sha256(int_to_bytes(random.randint(1, 2**64)))
            addresses[i[:6]] = i[:20]
        # i = parse_script(address_to_script("1AbHNFdKJeVL8FRZyRZoiTzG9VCmzLrtvm"))['addressHash']
        # addresses[i[:6]] = i[:20]


        c_affected, c_affected_duplicates = 0, 0
        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()
        h =0
        total_items = 0

        tts = 0
        etts = 0
        q = time.time()

        ac = 0
        hc = 0
        while 1:
            cursor.execute("SELECT type, "
                           "       block_filter.height, "
                           "       filter,"
                           "       blocks.hash  FROM  block_filter   "
                           "JOIN blocks ON blocks.height = block_filter.height "
                           "WHERE block_filter.height >= %s and type = %s "
                           "ORDER BY height ASC LIMIT  %s;", (h, f_type, batch_size) )


            rows = cursor.fetchall()
            cursor.execute("commit;")
            if not rows:
                break
            v_0, v_1 = hash_to_random_vectors(bytes(rows[0][3]))
            addresses_tmp = dict()
            for key in addresses:
                addresses_tmp[map_into_range(siphash(key, v_0, v_1), 2**32)] = addresses[key]
            addresses_set = set(addresses_tmp.keys())


            affected, affected_duplicates = 0, 0

            batch_offset = 0
            items  = 0
            batch_affected = dict()

            for row in rows:

                r = bytes(row[2])
                if not r:
                    continue

                stream = get_stream(r)
                l = var_int_to_int(read_var_int(stream))
                etts += l
                if l:
                    elements = decode_gcs(stream.read(l))
                else:
                    elements = []

                ac += len(elements)
                # EF = len(elements) * 784931
                # e2 = [map_into_range(siphash(int_to_bytes(qq)), EF) for qq in elements]
                # d = encode_gcs(e2, P=19)
                # tts += len(d)
                hc += 1

                try:
                    l = var_int_to_int(read_var_int(stream))
                    duplicates = decode_gcs(stream.read(l))
                    tts +=l
                    # hf = stream.read()
                    # counters = decode_huffman(hf)
                except:
                    hf = b""
                    duplicates, counters = [], []
                # tts += len(hf)
                etts += l
                # etts += len(hf)

                for a in addresses_set & set(elements):
                    affected += 1
                    batch_affected[elements.index(a) + batch_offset] = a
                    address = hash_to_address(addresses_tmp[a], witness_version=None)
                    if verbose:
                        print("    affected element: ", row[1], address)

                if batch_affected and duplicates:

                    for pointer in batch_affected:
                        if pointer in duplicates:
                            a = hash_to_address(addresses_tmp[batch_affected[pointer]],
                                                witness_version=None)
                            if verbose:
                                print("        affected duplicate  %s " % row[1], a)
                            affected_duplicates += 1
                assert(row[1] == h)
                items += len(elements) + sum(counters)

                h += 1
                batch_offset += len(elements)
            total_items += items
            c_affected += affected
            c_affected_duplicates += affected_duplicates

            print("Batch: %s;" % (h // batch_size),
                  "Start: %s;" % h,
                  "Items: %s;" % items,
                  "Total items: %s;" % total_items,
                  "Batch affected: [%s + %s];" % (affected, affected_duplicates),
                  "Total affected: [%s + %s];" % (c_affected, c_affected_duplicates),
                  "Average block elements: %s" % round(ac / hc))



        print("Total false positives: %s; " % (c_affected + c_affected_duplicates),
              "Block false positive rate: %s %%;" %( round( ((c_affected + c_affected_duplicates)/ h) * 100, 2),),
              "Transactions false positive rate: %s %%;" % (round(((c_affected + c_affected_duplicates)/ total_items) * 100,
                                                                 6),))
        print("Total time:", int(time.time() - q) // 60, "minutes")


    def test_create_bip158_filters(sels):
        print("Create bip 158 filters:")
        if not option_transaction_history:
            print("Skipped ...")
            print("Filter testing is only available if the transaction history option is enabled!\n")
            return

        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()
        cursor.execute("SELECT height  FROM  blocks  "
                       "ORDER BY height DESC LIMIT  1;")
        rows = cursor.fetchall()
        best_h = rows[0][0]

        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS bip158_test_filter (height BIGINT NOT NULL,
                                                                       filter BYTEA,
                                                                       PRIMARY KEY(height));""")
        F_c = 0
        F_m = 0
        ttc = 0
        hc = 0
        records = []
        cursor.execute("SELECT height FROM bip158_test_filter ORDER BY height DESC LIMIT 1;")
        rows = cursor.fetchall()
        if not rows:
            h = 0
        else:
            h = rows[0][0] + 1

        while h <= best_h:


            cursor.execute("SELECT pointer, address FROM transaction_map "
                           "WHERE pointer >= %s and pointer < %s "
                           "ORDER BY pointer;", (h << 39, (h + 1) << 39))
            rows = cursor.fetchall()
            if not rows:
                h += 1
                continue

            cursor.execute("SELECT hash  FROM blocks  WHERE height = %s LIMIT 1;", (h,))
            rows2 = cursor.fetchall()
            block_hash = bytes(rows2[0][0])
            v_0, v_1 = hash_to_random_vectors(block_hash)

            s = set()
            s2 = set()


            for row in rows:
                s.add(siphash(row[1][1:], v_0=v_0, v_1=v_1))


            F = len(s) * 784931
            F_c += F
            hc += 1

            for a in s:
                s2.add(map_into_range(a, F))
                ttc += 1



            d = encode_gcs(s2, P=19)
            f =  b"".join([int_to_var_int(F), int_to_var_int(len(d)), d])

            records.append((h, f))
            cursor.execute("INSERT INTO bip158_test_filter (height, filter) "
                           "VALUES (%s, %s);", (h,psycopg2.Binary(f)))
            if h % 1000 == 0:
                print("Blocks: %s;" % h,
                      "Average count per block: %s;" % round(ttc / hc),
                      "Total addresses: %s;" % ttc)
                cursor.execute("commit;")
            h += 1

        try:
            print("Total blocks: %s;" % h,
                  "Average count per block: %s;" % round(ttc/ hc),
                  "Total addresses: %s;" % ttc)
            cursor.execute("commit;")
        except:
            pass


    def test_bip158_10000_FPR(sels):

        print("Test bip 158 filters false positives for 10 000 monitoring addresses set:")
        if not option_transaction_history:
            print("Skipped ...")
            print("Filter testing is only available if the transaction history option is enabled!\n")
            return

        addresses = set()
        while len(addresses) < 10000:
            i = sha256(int_to_bytes(random.randint(1, 2**64)))
            addresses.add(i[:20])

        affected = 0
        t_affected = 0

        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()
        h = 0

        q = time.time()
        tti = 0
        tts = 0
        ets = 0


        while 1:
            cursor.execute("SELECT bip158_test_filter.height, "
                           "       filter,"
                           "       blocks.hash  FROM  bip158_test_filter  "
                           "JOIN blocks ON blocks.height = bip158_test_filter.height "
                           "WHERE bip158_test_filter.height >= %s  "
                           "ORDER BY height ASC LIMIT  1008;", (h,) )

            rows = cursor.fetchall()
            if not rows:
                break
            affected = 0
            for row in rows:
                r = bytes(row[1])
                block_hash = bytes(row[2])
                v_0, v_1 = hash_to_random_vectors(block_hash)
                if not r:
                    continue

                stream = get_stream(r)
                F = var_int_to_int(read_var_int(stream))
                l = var_int_to_int(read_var_int(stream))
                if l:
                    elements = decode_gcs(stream.read(l))
                    e2 = encode_gcs(elements, sort=False)
                    ets += len(e2)
                    tts += l
                    elements = set(elements)

                else:
                    elements = set()

                hashed_addresses = set()
                for a in addresses:
                    hashed_addresses.add(map_into_range(siphash(a, v_0=v_0, v_1=v_1), F))

                k = len(hashed_addresses & elements)
                t_affected += k
                affected += k

                h = row[0] + 1
                tti += len(elements)


            print("Blocks %s;" % h,
                  "Addresses total %s" % tti,
                  "Affected: %s;" % affected,
                  "Total affected : %s;" % t_affected,
                  "Size  dynamic GCS P: %s;" % ets,
                  "Bip158 size: %s;" % tts,
                  "Size savings: %s %%;" % (round(1 - ets/tts, 2))
                  )


        print("Total false positives: %s; " % affected,
              "Block false positive rate: %s;" %(round( (affected / h) * 100, 2),),
              "Transactions false positive rate: %s;" %( round( (affected/ tti) * 100, 2),))
        print("Total time:", int(time.time() - q) // 60, "minutes")





    """
       Batch [1]: 592 ; start block 596736 ; affected total: 1592 ; affected duplicates: 1337 0.04674863815307617
    affected block  597552 1KCgXjPXapX2SrPDzkVkPsXsTUX2B3yTHC
        affected block  597553 via duplicate 1KCgXjPXapX2SrPDzkVkPsXsTUX2B3yTHC count 1
    affected block  597692 1H3jbhuA6G7ViHji5L4KYGsCVfohCM6Xo8
    affected block  597739 13sA6xtfNwd7EvL6xXi8yBp7FWJYQtxyJJ
Total time: 6396 seconds
.
----------------------------------------------------------------------
Ran 1 test in 6396.217s

        # i = parse_script(address_to_script("1EmHuuAC257q3Pu5M5EfEgw4mU1CYHbLLo"))['addressHash']
        # addresses[bytes_to_int(i[:4])] = i[:20]
        # xx=bytes_to_int(i[:4])
    """

    def test_10k_FPR2(sels):

        print("Test filters for 10 000 monitoring addresses set:")
        if not option_transaction_history:
            print("Skipped ...")
            print("Filter testing is only available if the transaction history option is enabled!\n")
            return
        if not option_block_batch_filters:
            print("Skipped ...")
            print("Block batch filters option is not enabled!\n")
            return

        addresses = dict()
        while len(addresses) < 10000:
            i = sha256(int_to_bytes(random.randint(1, 2**64)))
            addresses[bytes_to_int(i[:5])] = i[:20]
            # addresses[i[:6]] = i[:20]
        # i = parse_script(address_to_script("131gJPiGDSSfV7f6rs6XjTr7K9xh2LfXwB"))['addressHash']
        # addresses[bytes_to_int(i[:5])] = i[:20]
        # addresses[i[:6]] = i[:20]
        #20
        i = parse_script(address_to_script("1FuXfSvK4UHFvjoTJjUxgnj9x2T3DujzjM"))['addressHash']
        addresses[bytes_to_int(i[:5])] = i[:20]
        # addresses[i[:6]] = i[:20]


        affected = 0
        affected_duplicates = 0
        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()
        # h = 72 * 7 * 1 * 900
        h = 0
        f_map = {1: 0, 2: 0, 4: 0, 8: 0}
        accum = {1: deque(), 2: deque(), 4: deque(), 8: deque()}
        duplicates = {1: None, 2: None, 4: None, 8: None}
        counters = {1: None, 2: None, 4: None, 8: None}
        batch_affected = {1: dict(), 2: dict(), 4: dict(), 8: dict()}
        batch_offset = {1: 0, 2: 0, 4: 0, 8: 0}
        batch_size =  144 * 7 * 4
        batch_size =  1000
        q = time.time()
        lb = 0
        lc = 0
        uc = 0
        dc = 0
        qtt = time.time()
        while 1:
            cursor.execute("SELECT type, block_filter.height, filter,"
                           "       blocks.hash  FROM  block_filter  "
                           "JOIN blocks ON blocks.height = block_filter.height "
                           "WHERE block_filter.height >= %s "
                           "ORDER BY height ASC LIMIT  1600;", (h,) )

            rows = cursor.fetchall()
            cursor.execute("commit;")

            if not rows:
                break
            batch_rnd = None
            batch_num = -1

            batch_addresses = set()
            batch_addresses_dict = dict()
            for a in addresses:
                batch_addresses.add(a & (2 ** 32 - 1))
                batch_addresses_dict[a & (2 ** 32 - 1)] = addresses[a]


            while rows:
                for row in rows[:8]:
                    if row[0] < 9:
                        f_type = row[0]
                        r = bytes(row[2])
                        if r:
                            stream = get_stream(r)

                            if row[1] % batch_size == 0:
                                if batch_num != row[1] // batch_size:
                                    batch_rnd = map_into_range(bytes_to_int(row[3][:16]), 2 ** 37)

                                    batch_num = row[1] // batch_size
                                    # batch_addresses = set()
                                    # batch_addresses_dict = dict()
                                    # for a in addresses:
                                    #     batch_addresses.add(map_into_range(a * batch_rnd, 2 ** 37))
                                    #     batch_addresses_dict[map_into_range(a * batch_rnd, 2 ** 37)] = addresses[a]
                                if f_type == 1:
                                    print("Batch [%s]:" % f_type , h // batch_size, "; start block",h,
                                          "; affected total:", affected,
                                          "; affected duplicates:",
                                          affected_duplicates,
                                          time.time() - qtt)

                                lb = 0
                                lc = 0
                                batch_offset[f_type] = 0
                                batch_affected[f_type] = dict()
                                accum[f_type] = False
                                counters[f_type] = None

                            F = var_int_to_int(read_var_int(stream))
                            f_map[f_type] = F
                            l = var_int_to_int(read_var_int(stream))
                            d = stream.read(l)
                            s = decode_gcs(d)
                        else:
                            s = []
                            F = 0
                        accum[f_type] = (F,s)

                    else:
                        f_type = row[0]
                        # v_0, v_1 = hash_to_random_vectors(row[3])
                        r = bytes(row[2])
                        duplicates[f_type] = list()
                        if r:
                            stream = get_stream(r)
                            l = var_int_to_int(read_var_int(stream))
                            for s in decode_gcs(stream.read(l)):
                                duplicates[f_type].append(s)
                            counters[f_type] = decode_huffman(stream.read())
                        else:
                            duplicates[f_type] = None

                for f_type in accum:
                    if f_type != 1:
                        continue
                    if accum[f_type][0]:
                        F = accum[f_type][0]

                        # a = map_into_range(siphash(a, v_0, v_1), F)\
                        s1 = OrderedDict()

                        # for i, x in enumerate(accum[f_type][1]):
                        #     try:
                        #         # print(x >> 24)
                        #         s1[x].append(x, i)
                        #     except:
                        #         s1[x] = [(x, i)]
                        lb += len(s1)
                        lc += len(accum[f_type][1])
                        # la = 0
                        i = 0

                        if batch_addresses & set(accum[f_type][1]):
                            for a in batch_addresses:
                                if a in accum[f_type][1]:
                                    # print("e", e, ">", sl, v_1, e in s)
                                    affected += 1
                                    batch_affected[f_type][accum[f_type][1].index(a) + batch_offset[f_type]] = a
                                    address = hash_to_address(batch_addresses_dict[a], witness_version=None)
                                    print("    affected block ", h, address, f_type)
                                #
                        # if r:
                        #     print(">",len(r), len(s1), len(s2))
                        # for i, q in enumerate(accum[f_type][1]):
                        #     for a in addresses:
                        #         # k = (a + i + h  F
                        #         # k = (a * (i+1) * ((h + 1)<<16) ) % F
                        #         # k = map_into_range(siphash(a, i, h), F)
                        #         k = map_into_range(a * (i + 1) * v_1, F)
                        #         if k == q:
                        #             affected += 1
                        #             batch_affected[f_type][accum[f_type][1].index(k) + batch_offset[f_type]] = a
                        #             address = hash_to_address(addresses[a], witness_version=None)
                        #             print("    affected block ", h, address)
                    if batch_affected[f_type] and duplicates[f_type * 16]:
                        for pointer in batch_affected[f_type]:
                            if pointer in duplicates[f_type * 16]:
                                a = hash_to_address(batch_addresses_dict[batch_affected[f_type][pointer]],
                                                    witness_version=None)
                                print("        affected block  %s via duplicate" % h, a,
                                      "count", counters[f_type * 16][duplicates[f_type * 16].index(pointer)] , f_type)
                                affected_duplicates += 1
                    batch_offset[f_type] += len(accum[f_type][1])
                if h % batch_size == 0:
                    qtt = time.time()
                h += 1
                rows = rows[8:]
        print("Total time:", int(time.time() - q), "seconds")
        return


    def chunks_by_count(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

"""

37 bit 2000 batch


Test filters for 10 000 monitoring addresses set:
Batch [1]: 0 ; start block 0 ; affected total: 0 ; affected duplicates: 0 0.011582612991333008
Batch [1]: 1 ; start block 2000 ; affected total: 0 ; affected duplicates: 0 0.1670393943786621
Batch [1]: 2 ; start block 4000 ; affected total: 0 ; affected duplicates: 0 0.1672196388244629
Batch [1]: 3 ; start block 6000 ; affected total: 0 ; affected duplicates: 0 0.17815303802490234
Batch [1]: 4 ; start block 8000 ; affected total: 0 ; affected duplicates: 0 0.1921682357788086
Batch [1]: 5 ; start block 10000 ; affected total: 0 ; affected duplicates: 0 0.19470691680908203
Batch [1]: 6 ; start block 12000 ; affected total: 0 ; affected duplicates: 0 0.1842358112335205
Batch [1]: 7 ; start block 14000 ; affected total: 0 ; affected duplicates: 0 0.21898198127746582
Batch [1]: 8 ; start block 16000 ; affected total: 0 ; affected duplicates: 0 0.2249152660369873
Batch [1]: 9 ; start block 18000 ; affected total: 0 ; affected duplicates: 0 0.17714476585388184
Batch [1]: 10 ; start block 20000 ; affected total: 0 ; affected duplicates: 0 0.20412635803222656
Batch [1]: 11 ; start block 22000 ; affected total: 0 ; affected duplicates: 0 0.20978879928588867
Batch [1]: 12 ; start block 24000 ; affected total: 0 ; affected duplicates: 0 0.211958646774292
Batch [1]: 13 ; start block 26000 ; affected total: 0 ; affected duplicates: 0 0.2187485694885254
Batch [1]: 14 ; start block 28000 ; affected total: 0 ; affected duplicates: 0 0.20422005653381348
Batch [1]: 15 ; start block 30000 ; affected total: 0 ; affected duplicates: 0 0.20169520378112793
    affected block  30000 1FuXfSvK4UHFvjoTJjUxgnj9x2T3DujzjM 1
Batch [1]: 16 ; start block 32000 ; affected total: 1 ; affected duplicates: 0 0.26360487937927246
Batch [1]: 17 ; start block 34000 ; affected total: 1 ; affected duplicates: 0 0.24228930473327637
Batch [1]: 18 ; start block 36000 ; affected total: 1 ; affected duplicates: 0 0.28116631507873535
Batch [1]: 19 ; start block 38000 ; affected total: 1 ; affected duplicates: 0 0.22296738624572754
Batch [1]: 20 ; start block 40000 ; affected total: 1 ; affected duplicates: 0 0.24347257614135742
Batch [1]: 21 ; start block 42000 ; affected total: 1 ; affected duplicates: 0 0.23287391662597656
Batch [1]: 22 ; start block 44000 ; affected total: 1 ; affected duplicates: 0 0.2654712200164795
Batch [1]: 23 ; start block 46000 ; affected total: 1 ; affected duplicates: 0 0.2442340850830078
Batch [1]: 24 ; start block 48000 ; affected total: 1 ; affected duplicates: 0 0.3962669372558594
Batch [1]: 25 ; start block 50000 ; affected total: 1 ; affected duplicates: 0 0.26896095275878906
Batch [1]: 26 ; start block 52000 ; affected total: 1 ; affected duplicates: 0 0.3063225746154785
Batch [1]: 27 ; start block 54000 ; affected total: 1 ; affected duplicates: 0 0.26677370071411133
Batch [1]: 28 ; start block 56000 ; affected total: 1 ; affected duplicates: 0 0.2733912467956543
Batch [1]: 29 ; start block 58000 ; affected total: 1 ; affected duplicates: 0 0.31076812744140625
Batch [1]: 30 ; start block 60000 ; affected total: 1 ; affected duplicates: 0 0.29433155059814453
Batch [1]: 31 ; start block 62000 ; affected total: 1 ; affected duplicates: 0 0.3420524597167969
Batch [1]: 32 ; start block 64000 ; affected total: 1 ; affected duplicates: 0 0.29282450675964355
Batch [1]: 33 ; start block 66000 ; affected total: 1 ; affected duplicates: 0 0.3443946838378906
Batch [1]: 34 ; start block 68000 ; affected total: 1 ; affected duplicates: 0 0.45645642280578613
Batch [1]: 35 ; start block 70000 ; affected total: 1 ; affected duplicates: 0 0.37703585624694824
Batch [1]: 36 ; start block 72000 ; affected total: 1 ; affected duplicates: 0 0.40244126319885254
Batch [1]: 37 ; start block 74000 ; affected total: 1 ; affected duplicates: 0 0.41446495056152344
Batch [1]: 38 ; start block 76000 ; affected total: 1 ; affected duplicates: 0 0.35643506050109863
Batch [1]: 39 ; start block 78000 ; affected total: 1 ; affected duplicates: 0 0.3523828983306885
Batch [1]: 40 ; start block 80000 ; affected total: 1 ; affected duplicates: 0 0.47875475883483887
Batch [1]: 41 ; start block 82000 ; affected total: 1 ; affected duplicates: 0 0.5400228500366211
Batch [1]: 42 ; start block 84000 ; affected total: 1 ; affected duplicates: 0 0.4137539863586426
Batch [1]: 43 ; start block 86000 ; affected total: 1 ; affected duplicates: 0 0.3648722171783447
Batch [1]: 44 ; start block 88000 ; affected total: 1 ; affected duplicates: 0 0.45053815841674805
Batch [1]: 45 ; start block 90000 ; affected total: 1 ; affected duplicates: 0 0.3901352882385254
Batch [1]: 46 ; start block 92000 ; affected total: 1 ; affected duplicates: 0 0.385847806930542
Batch [1]: 47 ; start block 94000 ; affected total: 1 ; affected duplicates: 0 0.4959897994995117
Batch [1]: 48 ; start block 96000 ; affected total: 1 ; affected duplicates: 0 0.46150851249694824
Batch [1]: 49 ; start block 98000 ; affected total: 1 ; affected duplicates: 0 0.44447898864746094
Batch [1]: 50 ; start block 100000 ; affected total: 1 ; affected duplicates: 0 0.573845624923706
Batch [1]: 51 ; start block 102000 ; affected total: 1 ; affected duplicates: 0 0.6030557155609131
Batch [1]: 52 ; start block 104000 ; affected total: 1 ; affected duplicates: 0 0.5653691291809082
Batch [1]: 53 ; start block 106000 ; affected total: 1 ; affected duplicates: 0 0.8116579055786133
Batch [1]: 54 ; start block 108000 ; affected total: 1 ; affected duplicates: 0 0.6651637554168701
Batch [1]: 55 ; start block 110000 ; affected total: 1 ; affected duplicates: 0 0.9142212867736816
Batch [1]: 56 ; start block 112000 ; affected total: 1 ; affected duplicates: 0 0.8282411098480225
Batch [1]: 57 ; start block 114000 ; affected total: 1 ; affected duplicates: 0 0.9081618785858154
Batch [1]: 58 ; start block 116000 ; affected total: 1 ; affected duplicates: 0 1.085641860961914
Batch [1]: 59 ; start block 118000 ; affected total: 1 ; affected duplicates: 0 0.8431117534637451
Batch [1]: 60 ; start block 120000 ; affected total: 1 ; affected duplicates: 0 1.0680134296417236
Batch [1]: 61 ; start block 122000 ; affected total: 1 ; affected duplicates: 0 1.094954252243042
Batch [1]: 62 ; start block 124000 ; affected total: 1 ; affected duplicates: 0 1.1296067237854004
Batch [1]: 63 ; start block 126000 ; affected total: 1 ; affected duplicates: 0 1.367269515991211
Batch [1]: 64 ; start block 128000 ; affected total: 1 ; affected duplicates: 0 1.5055243968963623
Batch [1]: 65 ; start block 130000 ; affected total: 1 ; affected duplicates: 0 2.37619686126709
Batch [1]: 66 ; start block 132000 ; affected total: 1 ; affected duplicates: 0 2.763242244720459
Batch [1]: 67 ; start block 134000 ; affected total: 1 ; affected duplicates: 0 2.8537793159484863
Batch [1]: 68 ; start block 136000 ; affected total: 1 ; affected duplicates: 0 3.2965545654296875
Batch [1]: 69 ; start block 138000 ; affected total: 1 ; affected duplicates: 0 3.3804678916931152
Batch [1]: 70 ; start block 140000 ; affected total: 1 ; affected duplicates: 0 2.974231243133545
Batch [1]: 71 ; start block 142000 ; affected total: 1 ; affected duplicates: 0 3.228109121322632
Batch [1]: 72 ; start block 144000 ; affected total: 1 ; affected duplicates: 0 2.922028064727783
Batch [1]: 73 ; start block 146000 ; affected total: 1 ; affected duplicates: 0 2.477933168411255
Batch [1]: 74 ; start block 148000 ; affected total: 1 ; affected duplicates: 0 2.6076714992523193
Batch [1]: 75 ; start block 150000 ; affected total: 1 ; affected duplicates: 0 2.3491573333740234
Batch [1]: 76 ; start block 152000 ; affected total: 1 ; affected duplicates: 0 2.3398663997650146
Batch [1]: 77 ; start block 154000 ; affected total: 1 ; affected duplicates: 0 2.2025973796844482
Batch [1]: 78 ; start block 156000 ; affected total: 1 ; affected duplicates: 0 2.0537476539611816
Batch [1]: 79 ; start block 158000 ; affected total: 1 ; affected duplicates: 0 1.8275694847106934
Batch [1]: 80 ; start block 160000 ; affected total: 1 ; affected duplicates: 0 1.7792026996612549
Batch [1]: 81 ; start block 162000 ; affected total: 1 ; affected duplicates: 0 2.096630334854126
Batch [1]: 82 ; start block 164000 ; affected total: 1 ; affected duplicates: 0 2.109501838684082
Batch [1]: 83 ; start block 166000 ; affected total: 1 ; affected duplicates: 0 2.2523725032806396
Batch [1]: 84 ; start block 168000 ; affected total: 1 ; affected duplicates: 0 2.395564317703247
Batch [1]: 85 ; start block 170000 ; affected total: 1 ; affected duplicates: 0 2.2448153495788574
Batch [1]: 86 ; start block 172000 ; affected total: 1 ; affected duplicates: 0 2.1051485538482666
Batch [1]: 87 ; start block 174000 ; affected total: 1 ; affected duplicates: 0 2.3177661895751953
Batch [1]: 88 ; start block 176000 ; affected total: 1 ; affected duplicates: 0 2.622894287109375
Batch [1]: 89 ; start block 178000 ; affected total: 1 ; affected duplicates: 0 2.5405678749084473
Batch [1]: 90 ; start block 180000 ; affected total: 1 ; affected duplicates: 0 3.4793777465820312
Batch [1]: 91 ; start block 182000 ; affected total: 1 ; affected duplicates: 0 5.09630274772644
Batch [1]: 92 ; start block 184000 ; affected total: 1 ; affected duplicates: 0 4.966126441955566
Batch [1]: 93 ; start block 186000 ; affected total: 1 ; affected duplicates: 0 5.655399322509766
Batch [1]: 94 ; start block 188000 ; affected total: 1 ; affected duplicates: 0 4.939436197280884
Batch [1]: 95 ; start block 190000 ; affected total: 1 ; affected duplicates: 0 5.2341296672821045
Batch [1]: 96 ; start block 192000 ; affected total: 1 ; affected duplicates: 0 5.539689064025879
Batch [1]: 97 ; start block 194000 ; affected total: 1 ; affected duplicates: 0 5.7385993003845215
Batch [1]: 98 ; start block 196000 ; affected total: 1 ; affected duplicates: 0 6.39298677444458
Batch [1]: 99 ; start block 198000 ; affected total: 1 ; affected duplicates: 0 5.717353105545044
Batch [1]: 100 ; start block 200000 ; affected total: 1 ; affected duplicates: 0 5.376492023468018
Batch [1]: 101 ; start block 202000 ; affected total: 1 ; affected duplicates: 0 5.868610143661499
Batch [1]: 102 ; start block 204000 ; affected total: 1 ; affected duplicates: 0 6.234995365142822
Batch [1]: 103 ; start block 206000 ; affected total: 1 ; affected duplicates: 0 5.2300121784210205
Batch [1]: 104 ; start block 208000 ; affected total: 1 ; affected duplicates: 0 5.250045537948608
Batch [1]: 105 ; start block 210000 ; affected total: 1 ; affected duplicates: 0 6.047279357910156
Batch [1]: 106 ; start block 212000 ; affected total: 1 ; affected duplicates: 0 5.836318016052246
Batch [1]: 107 ; start block 214000 ; affected total: 1 ; affected duplicates: 0 6.6637489795684814
Batch [1]: 108 ; start block 216000 ; affected total: 1 ; affected duplicates: 0 6.44462776184082
Batch [1]: 109 ; start block 218000 ; affected total: 1 ; affected duplicates: 0 8.31217622756958
Batch [1]: 110 ; start block 220000 ; affected total: 1 ; affected duplicates: 0 7.55508828163147
Batch [1]: 111 ; start block 222000 ; affected total: 1 ; affected duplicates: 0 7.276252746582031
Batch [1]: 112 ; start block 224000 ; affected total: 1 ; affected duplicates: 0 7.382060527801514
Batch [1]: 113 ; start block 226000 ; affected total: 1 ; affected duplicates: 0 7.9110331535339355
Batch [1]: 114 ; start block 228000 ; affected total: 1 ; affected duplicates: 0 7.263080596923828
Batch [1]: 115 ; start block 230000 ; affected total: 1 ; affected duplicates: 0 10.537686824798584
Batch [1]: 116 ; start block 232000 ; affected total: 1 ; affected duplicates: 0 11.94865345954895
Batch [1]: 117 ; start block 234000 ; affected total: 1 ; affected duplicates: 0 11.572701215744019
Batch [1]: 118 ; start block 236000 ; affected total: 1 ; affected duplicates: 0 11.026041030883789
Batch [1]: 119 ; start block 238000 ; affected total: 1 ; affected duplicates: 0 10.24012303352356
Batch [1]: 120 ; start block 240000 ; affected total: 1 ; affected duplicates: 0 8.511935472488403
Batch [1]: 121 ; start block 242000 ; affected total: 1 ; affected duplicates: 0 7.585694789886475
Batch [1]: 122 ; start block 244000 ; affected total: 1 ; affected duplicates: 0 8.361712217330933
Batch [1]: 123 ; start block 246000 ; affected total: 1 ; affected duplicates: 0 7.468443870544434
Batch [1]: 124 ; start block 248000 ; affected total: 1 ; affected duplicates: 0 7.77147364616394
Batch [1]: 125 ; start block 250000 ; affected total: 1 ; affected duplicates: 0 8.733120679855347
Batch [1]: 126 ; start block 252000 ; affected total: 1 ; affected duplicates: 0 8.147195100784302
Batch [1]: 127 ; start block 254000 ; affected total: 1 ; affected duplicates: 0 9.300328254699707
    affected block  255269 1EMxDthCx9DYYcpwnqb9axpLEjY4x9vTRQ 1
        affected block  255269 via duplicate 1EMxDthCx9DYYcpwnqb9axpLEjY4x9vTRQ count 1 1
Batch [1]: 128 ; start block 256000 ; affected total: 2 ; affected duplicates: 1 9.001648187637329
    affected block  256595 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH 1
        affected block  256598 via duplicate 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH count 1 1
Batch [1]: 129 ; start block 258000 ; affected total: 3 ; affected duplicates: 2 8.328612804412842
Batch [1]: 130 ; start block 260000 ; affected total: 3 ; affected duplicates: 2 8.498657464981079
Batch [1]: 131 ; start block 262000 ; affected total: 3 ; affected duplicates: 2 9.237635612487793
Batch [1]: 132 ; start block 264000 ; affected total: 3 ; affected duplicates: 2 8.489577054977417
Batch [1]: 133 ; start block 266000 ; affected total: 3 ; affected duplicates: 2 9.717461585998535
Batch [1]: 134 ; start block 268000 ; affected total: 3 ; affected duplicates: 2 9.220049381256104
Batch [1]: 135 ; start block 270000 ; affected total: 3 ; affected duplicates: 2 14.21752643585205
Batch [1]: 136 ; start block 272000 ; affected total: 3 ; affected duplicates: 2 19.14605188369751
Batch [1]: 137 ; start block 274000 ; affected total: 3 ; affected duplicates: 2 17.14554476737976
Batch [1]: 138 ; start block 276000 ; affected total: 3 ; affected duplicates: 2 15.476715803146362
Batch [1]: 139 ; start block 278000 ; affected total: 3 ; affected duplicates: 2 14.4943265914917
    affected block  279459 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH 1
        affected block  279507 via duplicate 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH count 1 1
Batch [1]: 140 ; start block 280000 ; affected total: 4 ; affected duplicates: 3 14.759339094161987
    affected block  280513 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH 1
Batch [1]: 141 ; start block 282000 ; affected total: 5 ; affected duplicates: 3 14.296149253845215
Batch [1]: 142 ; start block 284000 ; affected total: 5 ; affected duplicates: 3 15.271684169769287
Batch [1]: 143 ; start block 286000 ; affected total: 5 ; affected duplicates: 3 17.7218279838562
Batch [1]: 144 ; start block 288000 ; affected total: 5 ; affected duplicates: 3 18.778400182724
    affected block  288618 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH 1
Batch [1]: 145 ; start block 290000 ; affected total: 6 ; affected duplicates: 3 22.22026014328003
    affected block  290116 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH 1
Batch [1]: 146 ; start block 292000 ; affected total: 7 ; affected duplicates: 3 19.236260175704956
Batch [1]: 147 ; start block 294000 ; affected total: 7 ; affected duplicates: 3 18.41835641860962
Batch [1]: 148 ; start block 296000 ; affected total: 7 ; affected duplicates: 3 18.63091731071472
Batch [1]: 149 ; start block 298000 ; affected total: 7 ; affected duplicates: 3 18.32806897163391
Batch [1]: 150 ; start block 300000 ; affected total: 7 ; affected duplicates: 3 18.431514024734497
Batch [1]: 151 ; start block 302000 ; affected total: 7 ; affected duplicates: 3 18.386115789413452
    affected block  303170 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH 1
Batch [1]: 152 ; start block 304000 ; affected total: 8 ; affected duplicates: 3 19.38502264022827
Batch [1]: 153 ; start block 306000 ; affected total: 8 ; affected duplicates: 3 19.206772565841675
Batch [1]: 154 ; start block 308000 ; affected total: 8 ; affected duplicates: 3 17.799577236175537
Batch [1]: 155 ; start block 310000 ; affected total: 8 ; affected duplicates: 3 20.040149688720703
Batch [1]: 156 ; start block 312000 ; affected total: 8 ; affected duplicates: 3 19.890920400619507
    affected block  313663 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH 1
Batch [1]: 157 ; start block 314000 ; affected total: 9 ; affected duplicates: 3 20.61800742149353
    affected block  314281 1AmCNWv4rjBrvtGuLYQVoe6yK882N9UuUA 1
        affected block  314912 via duplicate 1AmCNWv4rjBrvtGuLYQVoe6yK882N9UuUA count 1 1
        affected block  315266 via duplicate 1AmCNWv4rjBrvtGuLYQVoe6yK882N9UuUA count 1 1
Batch [1]: 158 ; start block 316000 ; affected total: 10 ; affected duplicates: 5 21.292680978775024
Batch [1]: 159 ; start block 318000 ; affected total: 10 ; affected duplicates: 5 20.961485862731934
Batch [1]: 160 ; start block 320000 ; affected total: 10 ; affected duplicates: 5 21.905975341796875
Batch [1]: 161 ; start block 322000 ; affected total: 10 ; affected duplicates: 5 20.357622146606445
Batch [1]: 162 ; start block 324000 ; affected total: 10 ; affected duplicates: 5 24.441078424453735
Batch [1]: 163 ; start block 326000 ; affected total: 10 ; affected duplicates: 5 24.08799910545349
Batch [1]: 164 ; start block 328000 ; affected total: 10 ; affected duplicates: 5 24.774383783340454
Batch [1]: 165 ; start block 330000 ; affected total: 10 ; affected duplicates: 5 26.183144569396973
Batch [1]: 166 ; start block 332000 ; affected total: 10 ; affected duplicates: 5 26.227254629135132
Batch [1]: 167 ; start block 334000 ; affected total: 10 ; affected duplicates: 5 28.288923978805542
    affected block  335204 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH 1
    affected block  335246 1422wUnDQVsNQgn9bWYcVnQ9pMZv1Dab3W 1
Batch [1]: 168 ; start block 336000 ; affected total: 12 ; affected duplicates: 5 29.901915788650513
Batch [1]: 169 ; start block 338000 ; affected total: 12 ; affected duplicates: 5 24.069209575653076
Batch [1]: 170 ; start block 340000 ; affected total: 12 ; affected duplicates: 5 31.342244863510132
Batch [1]: 171 ; start block 342000 ; affected total: 12 ; affected duplicates: 5 29.342833995819092
    affected block  342240 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH 1
        affected block  342258 via duplicate 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH count 1 1
        affected block  343261 via duplicate 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH count 1 1
        affected block  343490 via duplicate 15qnrD5MyGo6FMNhR6ugNDp8MtSVjcvqjH count 1 1
Batch [1]: 172 ; start block 344000 ; affected total: 13 ; affected duplicates: 8 30.845691204071045
Batch [1]: 173 ; start block 346000 ; affected total: 13 ; affected duplicates: 8 29.85030436515808
Batch [1]: 174 ; start block 348000 ; affected total: 13 ; affected duplicates: 8 30.86751079559326
Batch [1]: 175 ; start block 350000 ; affected total: 13 ; affected duplicates: 8 29.33920955657959
Batch [1]: 176 ; start block 352000 ; affected total: 13 ; affected duplicates: 8 32.05176758766174
Batch [1]: 177 ; start block 354000 ; affected total: 13 ; affected duplicates: 8 31.580792665481567
Batch [1]: 178 ; start block 356000 ; affected total: 13 ; affected duplicates: 8 32.522178411483765
Batch [1]: 179 ; start block 358000 ; affected total: 13 ; affected duplicates: 8 31.549909114837646
Batch [1]: 180 ; start block 360000 ; affected total: 13 ; affected duplicates: 8 34.37537384033203
Batch [1]: 181 ; start block 362000 ; affected total: 13 ; affected duplicates: 8 36.597787618637085
Batch [1]: 182 ; start block 364000 ; affected total: 13 ; affected duplicates: 8 36.038891553878784
Batch [1]: 183 ; start block 366000 ; affected total: 13 ; affected duplicates: 8 37.74009847640991
Batch [1]: 184 ; start block 368000 ; affected total: 13 ; affected duplicates: 8 40.85695695877075
Batch [1]: 185 ; start block 370000 ; affected total: 13 ; affected duplicates: 8 38.291247606277466
Batch [1]: 186 ; start block 372000 ; affected total: 13 ; affected duplicates: 8 34.41765642166138
Batch [1]: 187 ; start block 374000 ; affected total: 13 ; affected duplicates: 8 35.58099317550659
    affected block  374939 133tW9HeZYduzLsovUsZdRTeUWShiQVLPn 1
        affected block  374996 via duplicate 133tW9HeZYduzLsovUsZdRTeUWShiQVLPn count 1 1
Batch [1]: 188 ; start block 376000 ; affected total: 14 ; affected duplicates: 9 39.24918746948242
Batch [1]: 189 ; start block 378000 ; affected total: 14 ; affected duplicates: 9 36.9858341217041
Batch [1]: 190 ; start block 380000 ; affected total: 14 ; affected duplicates: 9 40.30838751792908
Batch [1]: 191 ; start block 382000 ; affected total: 14 ; affected duplicates: 9 43.79545760154724
Batch [1]: 192 ; start block 384000 ; affected total: 14 ; affected duplicates: 9 44.635982513427734
Batch [1]: 193 ; start block 386000 ; affected total: 14 ; affected duplicates: 9 42.891412019729614
    affected block  387371 12aTsAsNeKYhfk7nCwnAMpQqwBDpynXrjt 1
Batch [1]: 194 ; start block 388000 ; affected total: 15 ; affected duplicates: 9 48.78173494338989
    affected block  388228 12aTsAsNeKYhfk7nCwnAMpQqwBDpynXrjt 1
        affected block  388262 via duplicate 12aTsAsNeKYhfk7nCwnAMpQqwBDpynXrjt count 1 1
Batch [1]: 195 ; start block 390000 ; affected total: 16 ; affected duplicates: 10 53.60857605934143
Batch [1]: 196 ; start block 392000 ; affected total: 16 ; affected duplicates: 10 44.57618498802185
Batch [1]: 197 ; start block 394000 ; affected total: 16 ; affected duplicates: 10 51.78374361991882
Batch [1]: 198 ; start block 396000 ; affected total: 16 ; affected duplicates: 10 55.43276047706604
Batch [1]: 199 ; start block 398000 ; affected total: 16 ; affected duplicates: 10 55.211480379104614
    affected block  399902 17sD8RXn34stU7djYADRQwqGY6zsyXK65t 1
Batch [1]: 200 ; start block 400000 ; affected total: 17 ; affected duplicates: 10 60.400933504104614
Batch [1]: 201 ; start block 402000 ; affected total: 17 ; affected duplicates: 10 59.96634030342102
Batch [1]: 202 ; start block 404000 ; affected total: 17 ; affected duplicates: 10 52.34324383735657
Batch [1]: 203 ; start block 406000 ; affected total: 17 ; affected duplicates: 10 54.499053716659546
Batch [1]: 204 ; start block 408000 ; affected total: 17 ; affected duplicates: 10 52.41022205352783
Batch [1]: 205 ; start block 410000 ; affected total: 17 ; affected duplicates: 10 56.84523153305054
Batch [1]: 206 ; start block 412000 ; affected total: 17 ; affected duplicates: 10 52.08387327194214
    affected block  412443 1Mb2B562tVqHy8yQArdZ22ZeyCMotLSpbZ 1
Batch [1]: 207 ; start block 414000 ; affected total: 18 ; affected duplicates: 10 57.2943332195282
Batch [1]: 208 ; start block 416000 ; affected total: 18 ; affected duplicates: 10 57.40468978881836
Batch [1]: 209 ; start block 418000 ; affected total: 18 ; affected duplicates: 10 62.586912631988525
Batch [1]: 210 ; start block 420000 ; affected total: 18 ; affected duplicates: 10 55.81053042411804
Batch [1]: 211 ; start block 422000 ; affected total: 18 ; affected duplicates: 10 55.683314085006714
Batch [1]: 212 ; start block 424000 ; affected total: 18 ; affected duplicates: 10 57.780426263809204
Batch [1]: 213 ; start block 426000 ; affected total: 18 ; affected duplicates: 10 57.96705508232117
Batch [1]: 214 ; start block 428000 ; affected total: 18 ; affected duplicates: 10 57.043776750564575
Batch [1]: 215 ; start block 430000 ; affected total: 18 ; affected duplicates: 10 56.22121810913086
Batch [1]: 216 ; start block 432000 ; affected total: 18 ; affected duplicates: 10 56.968305826187134
Batch [1]: 217 ; start block 434000 ; affected total: 18 ; affected duplicates: 10 63.86978316307068
Batch [1]: 218 ; start block 436000 ; affected total: 18 ; affected duplicates: 10 67.91844916343689
Batch [1]: 219 ; start block 438000 ; affected total: 18 ; affected duplicates: 10 67.69226431846619
Batch [1]: 220 ; start block 440000 ; affected total: 18 ; affected duplicates: 10 67.77007555961609
Batch [1]: 221 ; start block 442000 ; affected total: 18 ; affected duplicates: 10 74.57195138931274
Batch [1]: 222 ; start block 444000 ; affected total: 18 ; affected duplicates: 10 71.90946626663208
Batch [1]: 223 ; start block 446000 ; affected total: 18 ; affected duplicates: 10 71.9119942188263
Batch [1]: 224 ; start block 448000 ; affected total: 18 ; affected duplicates: 10 70.91576862335205
Batch [1]: 225 ; start block 450000 ; affected total: 18 ; affected duplicates: 10 66.975839138031
    affected block  450634 1AsDFAagdAD5599hnxPD6G9mXw86TGXWGy 1
        affected block  451121 via duplicate 1AsDFAagdAD5599hnxPD6G9mXw86TGXWGy count 1 1
Batch [1]: 226 ; start block 452000 ; affected total: 19 ; affected duplicates: 11 76.01177525520325
Batch [1]: 227 ; start block 454000 ; affected total: 19 ; affected duplicates: 11 74.2019214630127
Batch [1]: 228 ; start block 456000 ; affected total: 19 ; affected duplicates: 11 76.55940914154053
Batch [1]: 229 ; start block 458000 ; affected total: 19 ; affected duplicates: 11 78.63710284233093
Batch [1]: 230 ; start block 460000 ; affected total: 19 ; affected duplicates: 11 77.03392481803894
Batch [1]: 231 ; start block 462000 ; affected total: 19 ; affected duplicates: 11 78.31902432441711
Batch [1]: 232 ; start block 464000 ; affected total: 19 ; affected duplicates: 11 80.25021076202393
Batch [1]: 233 ; start block 466000 ; affected total: 19 ; affected duplicates: 11 83.35962295532227
    affected block  466665 1NnUQVcnwPfKmnpCvL8vpCPiSUWjQxjBN1 1
        affected block  467391 via duplicate 1NnUQVcnwPfKmnpCvL8vpCPiSUWjQxjBN1 count 2 1
Batch [1]: 234 ; start block 468000 ; affected total: 20 ; affected duplicates: 12 86.838387966156
    affected block  468288 1NnUQVcnwPfKmnpCvL8vpCPiSUWjQxjBN1 1
Batch [1]: 235 ; start block 470000 ; affected total: 21 ; affected duplicates: 12 84.71073722839355
    affected block  470878 1FNSZ5ekRVuoLPAeVEga2qpxm76tAvo6v4 1
Batch [1]: 236 ; start block 472000 ; affected total: 22 ; affected duplicates: 12 83.33139896392822
    affected block  472373 1NnUQVcnwPfKmnpCvL8vpCPiSUWjQxjBN1 1
Batch [1]: 237 ; start block 474000 ; affected total: 23 ; affected duplicates: 12 77.73929381370544
Batch [1]: 238 ; start block 476000 ; affected total: 23 ; affected duplicates: 12 72.4907455444336
Batch [1]: 239 ; start block 478000 ; affected total: 23 ; affected duplicates: 12 73.1862404346466
Batch [1]: 240 ; start block 480000 ; affected total: 23 ; affected duplicates: 12 72.69325542449951
Batch [1]: 241 ; start block 482000 ; affected total: 23 ; affected duplicates: 12 86.68774223327637
Batch [1]: 242 ; start block 484000 ; affected total: 23 ; affected duplicates: 12 78.20340919494629
Batch [1]: 243 ; start block 486000 ; affected total: 23 ; affected duplicates: 12 71.29171371459961
    affected block  486813 1PvmDDJ1oQ7ryRVEkw6RaAPPoAmYgSUHAd 1
        affected block  486901 via duplicate 1PvmDDJ1oQ7ryRVEkw6RaAPPoAmYgSUHAd count 1 1
Batch [1]: 244 ; start block 488000 ; affected total: 24 ; affected duplicates: 13 80.92418575286865
Batch [1]: 245 ; start block 490000 ; affected total: 24 ; affected duplicates: 13 81.61609387397766
Batch [1]: 246 ; start block 492000 ; affected total: 24 ; affected duplicates: 13 84.60064029693604
    affected block  492520 1A86Q4DMPo3soj3HKoU3emjPfLaAk7ZFG1 1
        affected block  493524 via duplicate 1A86Q4DMPo3soj3HKoU3emjPfLaAk7ZFG1 count 1 1
Batch [1]: 247 ; start block 494000 ; affected total: 25 ; affected duplicates: 14 101.05142045021057
Batch [1]: 248 ; start block 496000 ; affected total: 25 ; affected duplicates: 14 99.02784490585327
Batch [1]: 249 ; start block 498000 ; affected total: 25 ; affected duplicates: 14 104.03207969665527
Batch [1]: 250 ; start block 500000 ; affected total: 25 ; affected duplicates: 14 111.93524146080017
Batch [1]: 251 ; start block 502000 ; affected total: 25 ; affected duplicates: 14 112.21568059921265
Batch [1]: 252 ; start block 504000 ; affected total: 25 ; affected duplicates: 14 115.0949354171753
Batch [1]: 253 ; start block 506000 ; affected total: 25 ; affected duplicates: 14 97.06922841072083
    affected block  507305 12hnrsCKneUjJH2mARv5HowPfbib38CmuB 1
Batch [1]: 254 ; start block 508000 ; affected total: 26 ; affected duplicates: 14 85.58324694633484
    affected block  508177 1BkLpSNdyyLVzciz2Frvwn1SDwKppjDwX7 1
Batch [1]: 255 ; start block 510000 ; affected total: 27 ; affected duplicates: 14 90.47018480300903
Batch [1]: 256 ; start block 512000 ; affected total: 27 ; affected duplicates: 14 81.24033856391907
Batch [1]: 257 ; start block 514000 ; affected total: 27 ; affected duplicates: 14 85.587073802948
    affected block  514686 1Q7f2tWSYF6EockPfGJ685DsN4ZAtJvwKV 1
Batch [1]: 258 ; start block 516000 ; affected total: 28 ; affected duplicates: 14 65.52105855941772
    affected block  517669 1Q7f2tWSYF6EockPfGJ685DsN4ZAtJvwKV 1
Batch [1]: 259 ; start block 518000 ; affected total: 29 ; affected duplicates: 14 59.55364727973938
    affected block  518532 1FmJZez9ZVpjLM49WaVHBMmV7AtLDca4Zs 1
Batch [1]: 260 ; start block 520000 ; affected total: 30 ; affected duplicates: 14 72.60275435447693
Batch [1]: 261 ; start block 522000 ; affected total: 30 ; affected duplicates: 14 74.26339173316956
    affected block  522168 1EEEpvsX6fjaL4hUvujnVCCsjDwTJ8YMRB 1
Batch [1]: 262 ; start block 524000 ; affected total: 31 ; affected duplicates: 14 72.6454176902771
Batch [1]: 263 ; start block 526000 ; affected total: 31 ; affected duplicates: 14 62.95396709442139
Batch [1]: 264 ; start block 528000 ; affected total: 31 ; affected duplicates: 14 69.74895477294922
Batch [1]: 265 ; start block 530000 ; affected total: 31 ; affected duplicates: 14 66.23014736175537
Batch [1]: 266 ; start block 532000 ; affected total: 31 ; affected duplicates: 14 79.60727906227112
Batch [1]: 267 ; start block 534000 ; affected total: 31 ; affected duplicates: 14 71.91988372802734
Batch [1]: 268 ; start block 536000 ; affected total: 31 ; affected duplicates: 14 71.12300205230713
Batch [1]: 269 ; start block 538000 ; affected total: 31 ; affected duplicates: 14 68.51632332801819
Batch [1]: 270 ; start block 540000 ; affected total: 31 ; affected duplicates: 14 71.13425469398499
Batch [1]: 271 ; start block 542000 ; affected total: 31 ; affected duplicates: 14 74.1062536239624
    affected block  542881 1Q7f2tWSYF6EockPfGJ685DsN4ZAtJvwKV 1
Batch [1]: 272 ; start block 544000 ; affected total: 32 ; affected duplicates: 14 72.00571179389954
Batch [1]: 273 ; start block 546000 ; affected total: 32 ; affected duplicates: 14 80.02109885215759
    affected block  547382 1Q7f2tWSYF6EockPfGJ685DsN4ZAtJvwKV 1
Batch [1]: 274 ; start block 548000 ; affected total: 33 ; affected duplicates: 14 81.45263171195984
Batch [1]: 275 ; start block 550000 ; affected total: 33 ; affected duplicates: 14 83.93421959877014
    affected block  551205 1NKFfqhPsAs8Uo2YTuUDUaPzMVxi2qCkTP 1
Batch [1]: 276 ; start block 552000 ; affected total: 34 ; affected duplicates: 14 88.79495120048523
Batch [1]: 277 ; start block 554000 ; affected total: 34 ; affected duplicates: 14 85.22072124481201
Batch [1]: 278 ; start block 556000 ; affected total: 34 ; affected duplicates: 14 70.94436764717102
Batch [1]: 279 ; start block 558000 ; affected total: 34 ; affected duplicates: 14 66.90568447113037
Batch [1]: 280 ; start block 560000 ; affected total: 34 ; affected duplicates: 14 74.89673018455505
Batch [1]: 281 ; start block 562000 ; affected total: 34 ; affected duplicates: 14 75.24596238136292
    affected block  563880 12jmonhjMuvaWYSvPXrbSsfrmqzesH4b8Q 1
Batch [1]: 282 ; start block 564000 ; affected total: 35 ; affected duplicates: 14 78.88952493667603
    affected block  564107 12jmonhjMuvaWYSvPXrbSsfrmqzesH4b8Q 1
Batch [1]: 283 ; start block 566000 ; affected total: 36 ; affected duplicates: 14 84.49248361587524
Batch [1]: 284 ; start block 568000 ; affected total: 36 ; affected duplicates: 14 81.86303520202637
Batch [1]: 285 ; start block 570000 ; affected total: 36 ; affected duplicates: 14 87.07524561882019
Batch [1]: 286 ; start block 572000 ; affected total: 36 ; affected duplicates: 14 92.28864026069641
Batch [1]: 287 ; start block 574000 ; affected total: 36 ; affected duplicates: 14 87.25779843330383
Batch [1]: 288 ; start block 576000 ; affected total: 36 ; affected duplicates: 14 90.2243800163269
Batch [1]: 289 ; start block 578000 ; affected total: 36 ; affected duplicates: 14 91.60299301147461
Batch [1]: 290 ; start block 580000 ; affected total: 36 ; affected duplicates: 14 92.89798545837402
Batch [1]: 291 ; start block 582000 ; affected total: 36 ; affected duplicates: 14 98.06562352180481
Batch [1]: 292 ; start block 584000 ; affected total: 36 ; affected duplicates: 14 94.18647861480713
Batch [1]: 293 ; start block 586000 ; affected total: 36 ; affected duplicates: 14 87.06202602386475
Batch [1]: 294 ; start block 588000 ; affected total: 36 ; affected duplicates: 14 78.82413578033447
Batch [1]: 295 ; start block 590000 ; affected total: 36 ; affected duplicates: 14 81.79227066040039
Batch [1]: 296 ; start block 592000 ; affected total: 36 ; affected duplicates: 14 83.30850529670715
Batch [1]: 297 ; start block 594000 ; affected total: 36 ; affected duplicates: 14 77.80669069290161
Batch [1]: 298 ; start block 596000 ; affected total: 36 ; affected duplicates: 14 81.65787029266357
    affected block  596968 1Q7f2tWSYF6EockPfGJ685DsN4ZAtJvwKV 1
Total time: 9938 seconds
.
----------------------------------------------------------------------
Ran 1 test in 9938.482s




"""
