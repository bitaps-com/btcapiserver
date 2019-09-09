import unittest
import configparser
from pybtc import *
import requests
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
block_filter_bits = int(config["OPTIONS"]["block_filter_bits"])
block_filter_fps = int(config["OPTIONS"]["block_filter_fps"])
block_filter_capacity = int(config["OPTIONS"]["block_filter_capacity"])
option_blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
option_transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False
base_url = config["SERVER"]["api_endpoint_test_base_url"]

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

    def test_bloom(sels):
        # return

        m_addresses = set()
        while len(m_addresses) < 10_000:
            i = sha256(int_to_bytes(random.randint(1, 0xFFFFFFFFFFFFFFFFFFFF)))[:21]
            m_addresses.add(i)
        print("m addresses created ", len(m_addresses))
        f2 = create_gcs(m_addresses, N = block_filter_capacity, M=block_filter_fps, P=block_filter_bits)
        l2 = set(decode_gcs(f2, len(m_addresses)))
        print("m set created ")

        conn = psycopg2.connect(dsn=postgres_dsn)
        cursor = conn.cursor()
        last = 0



        cursor.execute("SELECT height, filter  "
                       " FROM block_range_filters "
                       " WHERE height > 0 "
                       "ORDER BY height ASC;" )

        rows = cursor.fetchall()

        for row in rows:
            print("checkpoint:" , row[0])
            print("checkpoint hash:" , sha256(row[1], hex=1))
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
        return


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
189647 59f97b8059c60215d8bafdb97f8c7556b88b8f6d4544a1c9f67382cff23d55d3
220607 88395f2c8a0fb4074bca3671d17d0a0ffd1c75ee1e5d48037ff5b013068cb9a8
242207 e08a79648f7cd8cfec751c1800fec4b75720bd5565b4b9bf8f59210d3afae31e
263951 f7a2e771b19eeaf74f796ba7fb17094d910a2ce1e521e77dcdbb0c65f3ab4820
275615 ac6061966dffcfe92643c9b3a541038ad078cd920bcb596310d601a524a88413
286703 4e76c78acc66327d2ed3dbe13392da1e426d59e40e3b765ca10d9c4a903142ae
294767 67a0c5ea270b7dbe76469ec3bb5acc61942f1ca7670e94de49cb0e2cd9986599
303407 6c5a97a9e6983516ccd0a0d54fd9baf5d3b8ebfff88966199d92630d5d4e8a8b
311615 46e176ce199c02f46596248cfad61f6b50b431e77e7af263d3bc033e2b882e4d
319103 4efbbeec20141818c8ea7aaf5e6f1d70eaa07c4b30b0c3102ef09db03465cd0c
325727 8e2436748916c1c20ac9167a43d0a570f4aa4b6ccc3e86ab53323226d827e5f7
331631 cef5a15c325afd8c9939608275fe56b672faa87cddb26128ea3dcf6c5f619c4e
337103 73941facdbdec3f41ccdc2872f0b1a859336477f38e1c204e7ae4f474f1792f5
342287 0fda3b1b3961cd9322bfd3fc32c0edbfe91381b83b455db0cab34dc5a2120180
347327 75dc33a7877673b71c416a5c89d02af16c63a012c8f45d865161b934892351b3
352367 5e2589a2d3c65a9687f50aec48ec34b90e74b516529ffa11873c6902aa851552
357263 e4ef783d087801617e00bd20b718a789ed01d18fd5c6a912637ab2b006d11b99
361583 166b5c6e049c68d19617ac49a83d1830303848a0a1957034d928c23e3553726a
365471 9868c457ede56a0250545e7070f9b52bb017e265233e43568957a25663e09821
369791 7637ddbea8b18a7cd0a548d906d0f35d1854c152f00833585d4229575696b009
373967 873396f290d44eddfb2eeec7e071c2cff0021b9d431e4201e2b2a6ce363657a4
377855 d765d89a50ca39a0c36e3d3c9298bcc8736199aac49e4ea64afff904318b9cd1
381455 985f144df1e1c847225b9694839ca5b00a9726c04aed8e9211c85d2f1bdf3c35
384623 9024532490684c6e77023cf23a60a58076fc02896f35d0c6672ff33dcb23ae4c
387935 6184a47b3079da9bea9af255b901ccc9182d1de6319b9a3224e7b56119313026
390815 b0aec3d011ff20dc8fb83fc35214693fed0e5638aaf783267bc7349e3ff7ff26
393839 c1f93efbea2fb9612e69cd92088ee242fa9378fadde05ff19a635a37470d1e3c
396287 bf708f0d198235da2cf83b1930ac163e1044737138e03bf99335b504f18c83d8
398879 85a0fb4167a75d946f312db5602fd54742c12c776dd599554c3535041f24bd19
401039 43aa2c4b49d239d695f1d7c20a07c1abbba0d1ced5c3a788de9d8d9251ba65e1
403775 148b6861d6fa157434d0179ef1e9bf85789c869f0b94223184b9ae20082ed98e
406223 a1a2fc8a4659e9ace604cff831965cfe41f8580423ff18e70a1c8b2fd0910081
408959 9c390085767a5c84774568ce018654891038eb0d7030d4cb0a1218d401fc9b8e
411695 db074b8567253681745d6a59b2e42eb09e9b0ad828d386c9bc716e57d3c7b322
414287 ff9673cd6b7ba06310c5cebb17b8220cd3032b9f9ec1d2c3cbef15bc6eec081d
416735 348a673c4b5b9b57f2eb8b4d5759badb22679de72cf6502d364d059b6c790e8a
419183 e6736e5cd87370200508ee6f8fd83c433cb9293467ce4b3f74e5d4ca1631332c
421775 4b736d7767178e4f3efa3c46458a25dae97ff1ab1d1b7ab50fac834b9fa45e05
424223 01979befe637b11ecef47d1d3b24445d82e2862cc25707212e8c5c53109a56db
426671 fd0b514702fd74da8af2548cd02657b743621943d4c743775bc8ed36b699e9a0
428975 f848901989236d4da940353c0652892dbb7817b2ae9af03420caf3c90b8538cf
431423 2af9c7c689d75b74276a62b34b385c2e8d50c46f80e8a372a102b721669cdd01
433583 40298a56c5eb695b3d5bd0795c261b3f398a4735ed960e17022421adbfa8de50
435599 1a7c1a96e21c67bf69807ea73600160b0d547cd17a19cfae37ff191f490f8839
437471 9fa67b62349597bfd92265b80109e8bfb1a559beaa7d86481720faf226f3af2d
439487 d6c758ef3d79f641a6d522552d6c633af618fc858ed740d31916178810fd454b
441215 1c0d08e599081edff822e82d7e0abd5ab9d43842d23f5554d32189b9b2788e07
442799 36ad9aec4e5889d85dce3cb7f1689117cd749b90696eaeeb787bdf70882718a4
444527 c3eb948d482657c7830eef7b6c9950cd379ddeb2c2a5161ae6a8425aebd4d5a4
446399 37d7c10511ace456ea1097633bc3cef69f208ac9d20e50a22677aeaada867889
448127 834c056f4b90e5964d8555dfd8740ff4f79b0b823e8a13c6df08572f96e8acce
449999 ca812f5f730110de4a2098fff7ac5295619527b973c12c83a84886c128b49d51
451727 80e3f1c09ecb7f41d3398984079168317cce9b1e26d332d9fa80807822a79efb
453455 7f4c4a5462d2761bbc1567f04ae9502569029d169e827aeb8550be984dd0addf
455183 8f323d069c9b8ac4db90e38fdc7a28b709e93c2cd0a9206f7bedcf688175b66b
456767 b7bc923d3efcbfa695f1b9889d3adfba6efde413d81a6b508287860d5b81ac6d
458351 c8916566f6bbc2d2ca98890a2c454bd7df27c0c9df6393d9e22b67c3adba50b1
460079 77dbbefd4495b75edcece04e2bac28cfd34ab9b1b3a2b959cf7c077595a533ad
461663 59df419fcab901c837d2123d9303f4c8fa25942a2ba4ba075975d48d8618d94f
463247 d77410542131efbd43ff5ea972f8974e95d4167752b4af9810f5b7a1e1255e0d
464687 a9f9e50dd65dbf019f1b239ad778691d23c7d86e5720c1357e8f7afd605e1729
466127 e457f1ba720b600aa654a2914d653fe5c9246a8bb3eb42d43bb9fa4e484cc451
467567 dc42503651f74a99a6406db19f75b217a552e3ce999317c3a16b8b6e07f27c82
469007 d027b668fda99871cc34eb2eb2014a576a6bea9bcb82c437d3e8fdfb39a9b0f7
470447 e199b8e3d1bc223a120feb21701ad05c0d29502b86e6340557c8aa133c826d09
471887 83bf3c49966aaf096530494347ebe0971f8c7ca78a01ba9ca9fef393c357d295
473327 588e9dd8f25c809ec0db7e9430962b5d25abe1bf5ce3c9455fcea622acb8b19a
475055 622858a8fc9ebc54abc752b4b63d7186f44d4cf68a90ea67d3e5cff157b1fc81
476639 5b15f703cd34f7ab57657d4af581141563dfad16db8cc48549c818c0cf3e1950
478367 7268caae1a40518bfdb9e6caa8ba2eeec05135c721b1ddb68ee5e3a718674025
479951 81fc0ec62767dc3e460c10ba9346f545742da0ca584cfe238fc21087afe6f8e8
481391 474036549be37d03b99e17161364461b81823a32b948e0697006eff918172e9d
482687 ee14c6ebdd3efe21f9b45e683fd47efafe2e89ad911329b6745af5f767e38413
484271 6b4b0b348863699b1de21ed7eecf5de28a47bce05f8c4e3b1b6af27464bf0274
485999 d74da09bda4722410971c31b3c55d8e5a4bb744485d990174c9176e4e76c118f
487583 28fcc582e9ebeb4579d30e0939d363eeb01636affba9ec4566f1c0ec231edb56
489167 7bc396a5c630e8bf5cba7c8316e3bc24eb66914b988fab831b30832a29e94e5e
490463 750252ef4aa57469088f21db255fa7bdc051c578c6a704743d02b637c0780244
491759 c0a24de07d5b758f8b1770357395777f84e4436591c65af29884757d0a19fd32
492911 e4d7b77f1e00cbaa75e18b0ec92125e07c9a9268cd2619826157cff06aba3670
494063 1f7d0e5a3b4b035e92458cf187a148112b6113fdbb6ca7d3175b769045b6e291
495215 8f5c6091807e265675807d3772da05f3354bbfd58c53f64ccaaadf228eaefb4e
496367 b8c75472b76104cc63b95c07549b267400d4dde4dd809fc2843190eafe44cdd9
497519 4eb81a8814f72db08616efbee46bb62b13c227b18a7cba205b58a4f794bd4a61
498527 10f448ce280443b186fb43fefa4860ac412a3770f8d4b3b14d197c9c3ec9fc51
499535 559a0a19bf47b9dac1f98da6106c5e35315a1c40af6ea9f30641d852f55637a5
500543 88aa4d2a161e3c8c08747d045489668349f727ed3bb66aca77d6eb310c33eca4
501551 667cb5316ca96eabc49dd196902648c862b7b185c23b069a93f48e754fdb5ab1
502559 7e362a1866200a8ac06303a98573d499bff3f87afb78eaf81a2fe5ad90f44965
503567 f7c1e70e48a1a17a3da1f214eb3136517244737bbf1599f4e691c63a0012a3de
504575 3c457a5e67834c4b5f810c23d0afee3147eebcd51610fbbbf5ab55ac6d68df3c
505727 3e3717f91b747f7047d5915adac9cc43a9d5ca2233be870ad6e72bafcb905848
507023 51f57825d89844b6976f7a1cc7c0e398c38d125147548252945effaa58225139
508319 7cb4b38555462e2a86e8bd3af1a374d9944e4a99f1fcf744ab7408a808617c44
509615 9d465c6512f18dda7db479992df0056f79d6eb4adec4f0a355efad1efb29a78d
511055 83689d17c5840e5ab8d02dbe7ac8bb0b78dbcbbbfc83610877e36574b1707dcd
512351 86fffd8dfc3ce90c7f6fdf05a3da7f9cbf8878c9780c59f557e9571827a693fb
513503 972b1be98a9332cc1068018ba86e85cc3062a66197b2b6d20234919618eb8a00
515375 3880168d44005f14ae24151da53ad742e03df3cec41ca25322535b69f25a8b7e
517535 6d2b10f50b89bc74a38888cd21993bec90bf4e306f537400d3b70ddfb39e5b8a
519407 a9611a1a6c91a34debe9cdf833c423b05d146aee2a92c0a68405e4dec574f9f5
520991 6662a632a2f9e702373d81482fa5a69f58e0e2c0359015d9093a563ffb637944
522719 79ec76f6f872dfcc052f08837116b0bf74fb7974c9590d71cbbd5b9906ef3343
524591 2b605feaa7523b8bdb2e7121ce27b3bd5d2f4dcde293538ce019aff232f49ba4
526463 6f30f57033c47baaf1bfcc15806754dfaa94da741c7a869715fe4b0217054106
528335 add5715d20a344ebecc1255146f029cadf5b2516ff7cb24b97632d87447f5f2c
530351 9828eeb2dd36480acbc4b833fd781bbdbce7c3faac10b3eefc72a7779813ade3
531935 9339d5c66ad9032245209b4357b78443dd83ec3d6aedeb64fa44215acf0c2a88
533663 1336d682c586385ce0b6e447a7d190138174606588063f4666671fcd2c4de7d7
535535 4a6c0f6f379b9049885023a115427cdd250b1fd1ad6a67ce30fccffb077ba190
537263 e9fb0ac85b6806b11427e7a9c179c0d0b20f6be1d8381c02dae1e6b0c5d8a264
539135 461ed8fd09410b912790a6092f0290a3256c82b11c15ace2419a0a1557b27b43
540863 7c2519a73d21678a31019433bbb5f85a3ee70df6b98cff2477d69f4372c48a84
542591 b00d98cd496e2f1a580d0d49b6324e67aec6c7d2dc70f4ab84ac974b63946e1f
544319 557fc768b0f18514c9cd99964cae7adf24046daaf63b3e3fa94d0d3b391720fd
545903 7d882cc16d8b15287404658a8c027a3b52a7a9d79960d58651ac2564076f27b5
547487 81a937fa8ad815ae28240e3881133e8500b4ab779b5d63fc742be2e0187e4108
549071 21cecf7a86fa0f59801c4cf09b6d2aa224a53020d21951f5360d4668a6584a05
550511 878b7c2d0d00a827891115d656ed12bad11d74af786d488c00727832c8186db8
551951 a81103097ed7a2acb59ba1d2fc066c1e8bda3b35f22490254afdde7817217bf8
553535 4974f6339a5263d258756508ef4a83f9e394ff486696274078847883993f2c58
555119 986da18b48b2a6dfd31d31b0bc0521fcd6e9281fe1f35f6b20ba30b5f7effd17
557279 ea2a111c582e916bd1f8e3095763e350f17a79ed17426f6c748ec3f4db172dbc
559007 f28cbe6c4e41f9589391c30a1423949f397b41232743c68c9a383c3edebac19b
560735 3a5bc4b1546180bf197e0d3b6e91a0d097922eaf00cfcd5231989044fc79e1b2
562463 9e6d6e2998dd7a34ba51af0a9aee315a7c2a86b5da985e235cbd8cc87bec61e0
564047 60adc8372d185c6d9758e0038cad8307c259a1ec2e9dbb41b6a2b0f1de7b96a5
565631 204ca2052964d00f6c3d45a440be34b3c69699b0935a9ce1d4e84c0c67accd8
567071 baeeb064afbf9568f0be8a3641af7ef38e54a1a0310a6d307c1723f7787ee169
568799 3f10336587446e3f20cf8cb2a66489d0bcb2c76e67d23a1359c54bb3738e226a
570239 395c8568499167a0d4c1356dc093dd8a9facf5127c89fc02311ed6f604d7f8a3
571679 a0676e5cb1471db90ede64f7c3e682782265927079ad6a4d9e84d0ce8e9658db
573119 e5d4ee65d92cf7e158a2cc8f7a3f0e6b5a2393941f58dcb2baf482a9d34b9455
574703 f0be43d67dccf88718f745edf7cabf349acfb8c912b3199f46e672b8af715cce
575999 ca8ce0bfc54363b5167689315e6847e4c7ed51e3da084f6e0025429a004e6905
577295 0da1c934d90e679728bb58c9385a7f493b0e15c5b19c9547a63b9e67194f46b6
578735 62bbf648a6cea715ea30b6020b2438e5f3cd52ae227ea383b1a2ca4b964eedf7
580031 e31668cabc8f7323725e9c044ff7c2e4dbf1a9c39ef3a42a8e957bc1c9a51f1d
581183 3b13cab8d0b30cd79c1416e4a8e5dfcabfa48774b8edea6bdc76d40f9c56e434
582479 7f0e68cc49723050461d197d8727b50fee1daccfd2a6b1490848e923479a3b36
583775 f4c4b223507c3cc008e2bf668bb6497bfe6baf04fd7d940fd87cac785e7bc2fb
585215 7f6f82e91e9f10dc24192b62fbef568148a97dd2b1996df75a66bee6d771f59d
586655 615b142ed6bcb285c0206ae7355509805173755d749813fb9d6bdd78a7825ff2
588239 81cbefd2231c508c218b8e9257b484b4da56bf3d7101cb31ced45a995804c355
589823 803ffae7f0f8961adc1648a0dedd42e09c4df63e00e5a243b150f934391dd558
591263 6f5c71385eecb81997dcffb2a7d5f4a91b652d0e01cdc69c72aaf615fb60ac7c
592847 a359c36e385990c762046738af6c21fe68978037a33f52569166bf464d625aca


6 306 Mb  block filters

2 133 Mb  block range filters


"""