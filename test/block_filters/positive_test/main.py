import unittest
import configparser
from pybtc import *
import psycopg2
from collections import OrderedDict, deque

config_file =   "/config/btcapi-server.conf"
config = configparser.ConfigParser()
config.read(config_file)
postgres_dsn = config["POSTGRESQL"]["dsn"]
option_block_batch_filters = True if config["OPTIONS"]["block_filters"] == "on" else False
option_transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False



def test_filters():
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


    batch_size = 1000
    last_batch_height = (h // batch_size - 1) * batch_size
    h = last_batch_height + batch_size
    n_type_map_filter_type = {0: 2, 1: 4, 2: 1, 5: 8, 6: 16}

    # h = 606962
    batch_map = {1: dict(), 2: dict(), 4: dict(), 8: dict(), 16: dict()}
    batch = {1: set(), 2: set(), 4: set(), 8: set(), 16: set()}
    element_index = {1: 0, 2: 0, 4: 0, 8: 0, 16: 0}
    last_hash = {1: None, 2: None, 4: None, 8: None, 16: None}


    while 1:
        if h % (1000) == 0:
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

            if row[0][0][0] not in (0,1,2,5,6):
                continue
            f_type = n_type_map_filter_type[row[0][0][0]]
            if row[0][0][0] == 2:
                k = parse_script(bytes(row[0][1:]))
                # print(">>>",row[0][0][0])
                # print(k)
                # print(ti)
                # print(row[1] & 0b11111111111111111)
                # print(decode_script(bytes(row[0][1:]), asm=1))
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

            if last_hash[f_type] and v:
                last_hash[f_type] = double_sha256(double_sha256(r) + last_hash[f_type])
            elif v:
                last_hash[f_type] = double_sha256(double_sha256(r) + b"\00" * 32)

            if last_hash[f_type]:
                if f_type == 1:
                    print(last_hash[f_type].hex(), bytes(row[3]).hex())
                if  last_hash[f_type] != bytes(row[3]):
                    print(v)
                    print("filter type", f_type, "height", h)
                    print(last_hash[f_type], bytes(row[3]))
                    raise Exception("filter header hash error")

            if elements or duplicates:
                print(h, "[%s]" % f_type, len(v), len(elements), len(duplicates),"OK")
        h+=1


test_filters()


def test_1k_FPR(self):
    self.false_positive_test(2, 1000)

def test_10k_FPR(self):
    self.false_positive_test(2, 10000)


def false_positive_test(self, f_type, address_count, batch_size = 1000 , verbose = 0):

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
        addresses[map_into_range(siphash(i), 2**32)] = i[:20]
    # i = parse_script(address_to_script("1G4tVHxSEzBhFFzA7XQHtgjHfM1rpAUcV7"))['addressHash']
    # addresses[map_into_range(siphash(i), 2 ** 32)] = i[:20]

    addresses_set = set(addresses.keys())

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
    bt = 0
    while 1:
        cursor.execute("SELECT type, "
                       "       block_filter.height, "
                       "       filter  FROM  block_filter   "
                       "WHERE block_filter.height >= %s and type = %s "
                       "ORDER BY height ASC LIMIT  %s;", (bt * batch_size, f_type, batch_size) )
        rows = cursor.fetchall()
        bt += 1
        cursor.execute("commit;")
        if not rows:
            break

        affected, affected_duplicates = 0, 0

        batch_offset = 0
        items  = 0
        batch_affected = dict()

        for row in rows:
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
                tts += l
            else:
                duplicates = []


            ac += len(elements)
            hc += 1
            etts += l


            for a in addresses_set & set(elements):
                affected += 1
                batch_affected[elements.index(a) + batch_offset] = a
                address = hash_to_address(addresses[a], witness_version=None)
                if verbose:
                    print("    affected element: ", row[1], address)

            if batch_affected and duplicates:

                for pointer in batch_affected:
                    if pointer in duplicates:
                        a = hash_to_address(addresses[batch_affected[pointer]],
                                            witness_version=None)
                        if verbose:
                            print("        affected duplicate  %s " % row[1], a)
                        affected_duplicates += 1
            # print(row[1], h)
            # assert(row[1] == h)
            h = row[1]
            items += len(elements) + len(duplicates)

            # h += 1
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

