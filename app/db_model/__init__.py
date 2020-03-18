from pybtc import rh2s
import json

async def create_db_model(app, conn):

    # check db isolation level

    level = await conn.fetchval("SHOW TRANSACTION ISOLATION LEVEL;")
    if level != "repeatable read":
        raise Exception("Postgres repeatable read isolation level required! current isolation level is %s" % level)
    await conn.execute(open("./db_model/sql/schema.sql", "r", encoding='utf-8').read().replace("\n",""))


    # blocks data

    if app.blocks_data:
        await conn.execute(open("./db_model/sql/block.sql", "r", encoding='utf-8').read().replace("\n", ""))
        await conn.execute("INSERT INTO service (name,value) VALUES ('blocks_data','1') ON CONFLICT(name) DO NOTHING;")
    else:
        await conn.execute("""
                               INSERT INTO service (name, value) VALUES ('blocks_data', '0') 
                               ON CONFLICT(name) DO NOTHING;
                               """)

    m = await conn.fetchval("SELECT value FROM service WHERE name ='blocks_data' LIMIT 1;")
    app.log.info("Option blocks_data = %s" % m)

    if bool(int(m)) !=  app.blocks_data:
        app.log.critical("blocks_data option not match db structure; you should drop db and recreate it")
        raise Exception("DB structure invalid")

    # transaction

    if app.transaction:
        await conn.execute(open("./db_model/sql/transaction.sql",
                                "r", encoding='utf-8').read().replace("\n", ""))
        await conn.execute("INSERT INTO service (name,value) VALUES ('transaction','1') ON CONFLICT(name) DO NOTHING;")
    else:
        await conn.execute("INSERT INTO service (name,value) VALUES ('transaction','0') ON CONFLICT(name) DO NOTHING;")

    m = await conn.fetchval("SELECT value FROM service WHERE name ='transaction' LIMIT 1;")
    app.log.info("Option transaction = %s" % m)

    if bool(int(m)) !=  app.transaction:
        app.log.critical("transaction option not match db structure; you should drop db and recreate it")
        raise Exception("DB structure invalid")


    # merkle proof module

    if app.merkle_proof:
        await conn.execute(open("./db_model/sql/merkle_proof.sql",
                                "r", encoding='utf-8').read().replace("\n", ""))
        await conn.execute("""
                           INSERT INTO service (name, value) VALUES ('merkle_proof', '1')  
                           ON CONFLICT(name) DO NOTHING;
                           """)
    else:
        await conn.execute("""
                           INSERT INTO service (name, value) VALUES ('merkle_proof', '0') 
                           ON CONFLICT(name) DO NOTHING;
                           """)

    m = await conn.fetchval("SELECT service.value FROM service WHERE service.name ='merkle_proof' LIMIT 1;")
    app.log.info("Option merkle_proof = %s" % m)

    if int(m) == 1 and not app.merkle_proof or app.merkle_proof and int(m) == 0:
        app.log.critical("merkle_proof config option not match db structure; you should drop db and recreate it.")
        raise Exception("DB structure invalid")


    if app.block_filters:
        await conn.execute(open("./db_model/sql/filters.sql",
                                "r", encoding='utf-8').read().replace("\n", ""))
        await conn.execute("""
                           INSERT INTO service (name, value) VALUES ('block_filters', '1')  
                           ON CONFLICT(name) DO NOTHING;
                           """)
    else:
        await conn.execute("""
                           INSERT INTO service (name, value) VALUES ('block_filters', '0') 
                           ON CONFLICT(name) DO NOTHING;
                           """)

    m = await conn.fetchval("SELECT service.value FROM service WHERE service.name ='block_filters' LIMIT 1;")
    app.log.info("Option block_filters = %s" % m)

    if int(m) == 1 and not app.block_filters or app.block_filters and int(m) == 0:
        app.log.critical("block_filters config option not match db structure; you should drop db and recreate it.")
        raise Exception("DB structure invalid")



    # transaction history

    if app.transaction_history:
        if not app.transaction:
            app.log.critical("transaction_history option required transaction option enabled")
            raise Exception("configuration invalid")

        await conn.execute(open("./db_model/sql/transaction_history.sql",
                                "r", encoding='utf-8').read().replace("\n", ""))
        await conn.execute("""
                           INSERT INTO service (name, value) VALUES ('transaction_history', '1')  
                           ON CONFLICT(name) DO NOTHING;
                           """)
        app.transaction_map_start_block = await conn.fetchval("SELECT pointer FROM transaction_map "
                                                              "ORDER BY pointer DESC LIMIT 1;")
        if app.transaction_map_start_block is None:
            app.transaction_map_start_block = 0
        else:
            app.transaction_map_start_block = app.transaction_map_start_block >> 39

    else:
        await conn.execute("""
                           INSERT INTO service (name, value) VALUES ('transaction_history', '0') 
                           ON CONFLICT(name) DO NOTHING;
                           """)

    m = await conn.fetchval("SELECT value FROM service WHERE name ='transaction_history' LIMIT 1;")
    app.log.info("Option transaction_history = %s" % m)

    if bool(int(m)) !=  app.transaction_history:
        app.log.critical("transaction_history option not match db structure; "
                         "you should drop db and recreate it")
        raise Exception("DB structure invalid")



    # address_state module

    if app.address_state:
        if not app.transaction_history:
            app.log.critical("address_state option required transaction_history option enabled")
            raise Exception("configuration invalid")


        await conn.execute(open("./db_model/sql/address_state.sql",
                                "r", encoding='utf-8').read().replace("\n", ""))
        await conn.execute("""
                               INSERT INTO service (name, value) VALUES ('address_state', '1')  
                               ON CONFLICT(name) DO NOTHING;
                               """)

    else:

        await conn.execute("""
                               INSERT INTO service (name, value) VALUES ('address_state', '0') 
                               ON CONFLICT(name) DO NOTHING;
                               """)

    m = await conn.fetchval("SELECT service.value FROM service WHERE service.name ='address_state' LIMIT 1;")
    app.log.info("Option address_state = %s" % m)

    if int(m) == 1 and not app.address_state or app.address_state and int(m) == 0:
        app.log.critical("address_state config option not match db structure; you should drop db and recreate it.")
        raise Exception("DB structure invalid")

    # address_timeline module

    if app.address_timeline:
        if not app.transaction_history:
            app.log.critical("address_timeline option required transaction_history option enabled")
            raise Exception("configuration invalid")
        if not app.address_state:
            app.log.critical("address_timeline option required address_state option enabled")
            raise Exception("configuration invalid")


        await conn.execute(open("./db_model/sql/address_timeline.sql",
                                "r", encoding='utf-8').read().replace("\n", ""))
        await conn.execute("""
                               INSERT INTO service (name, value) VALUES ('address_timeline', '1')
                               ON CONFLICT(name) DO NOTHING;
                               """)

    else:

        await conn.execute("""
                               INSERT INTO service (name, value) VALUES ('address_timeline', '0')
                               ON CONFLICT(name) DO NOTHING;
                               """)

    m = await conn.fetchval("SELECT service.value FROM service WHERE service.name ='address_timeline' LIMIT 1;")
    app.log.info("Option address_timeline = %s" % m)

    if int(m) == 1 and not app.address_timeline or app.address_timeline and int(m) == 0:
        app.log.critical("address_timeline config option not match db structure; you should drop db and recreate it.")
        raise Exception("DB structure invalid")


    # blockchain_analytica module

    if app.blockchain_analytica:
        await conn.execute(open("./db_model/sql/blockchain_analytica.sql",
                                "r", encoding='utf-8').read().replace("\n", ""))
        await conn.execute("""
                               INSERT INTO service (name, value) VALUES ('blockchain_analytica', '1')
                               ON CONFLICT(name) DO NOTHING;
                               """)

        # b = await conn.fetchval("SELECT blockchain FROM blocks_stat ORDER BY height DESC LIMIT 1;")
        # if b is not None:
        #     app.blockchain_stat = json.loads(b)
        # else:
        #     app.blockchain_stat = {
        #         "outputs": {"count": {"total": 0},                                  # total outputs count
        #                                                                             # What is the total quantity of
        #                                                                             # coins in bitcoin blockchain?
        #
        #                     "amount": {"min": {"pointer": 0,                        # output with minimal amount
        #                                        "value": 0},                         # What is the minimum amount of a coins?
        #
        #                                "max": {"pointer": 0,                        # output with maximal amount
        #                                        "value": 0},                         # What is the maximal amount of a coins?
        #
        #                                "total": 0,                                  # total amount of all outputs
        #
        #                                "map": {"count": dict(),                     # quantity distribution by amount
        #                                                                             # How many coins exceed 1 BTC?
        #
        #                                        "amount": dict()}                    # amounts distribution by amount
        #                                                                             # What is the total amount of all coins
        #                                                                             # exceeding 10 BTC?
        #                                },
        #
        #                     "type": {"map": {"count": dict(),                       # quantity distribution by type
        #                                                                             # How many P2SH coins?
        #
        #                                      "amount": dict(),                      # amounts distribution by type
        #                                                                             # What is the total amount of
        #                                                                             # all P2PKH coins?
        #
        #                                      "size": dict()}},                      # sizes distribution by type
        #                                                                             # What is the total size
        #                                                                             # of all P2PKH coins?
        #
        #                     "age": {"map": {"count": dict(),                        # distribution of counts by age
        #                                                                             # How many coins older then 1 year?
        #
        #                                     "amount": dict(),                       # distribution of amount by age
        #                                                                             # What amount of coins older then 1 month?
        #
        #                                     "type": dict()                          # distribution of counts by type
        #                                                                             # How many P2SH coins older then 1 year?
        #                                     }}
        #
        #                     },
        #
        #         "inputs": {"count": {"total": 0},                                   # total inputs count
        #                                                                             # What is the total quantity of
        #                                                                             # spent coins in bitcoin blockchain?
        #
        #                    "amount": {"min": {"pointer": 0,                         # input with minimal amount
        #                                       "value": 0},                          # What is the smallest coin spent?
        #
        #                               "max": {"pointer": 0,                         # input with maximal amount
        #                                       "value": 0},                          # what is the greatest coin spent?
        #
        #                               "total": 0,                                   # total amount of all inputs
        #                                                                             # What is the total amount of
        #                                                                             # all spent coins?
        #
        #                               "map": {"count": dict(),                      # quantity distribution by amount
        #                                                                             # How many spent coins exceed 1 BTC?
        #
        #                                       "amount": dict()}                     # amounts distribution by amount
        #                                                                             # What is the total amount of
        #                                                                             #  all spent coins exceeding 10 BTC?
        #                               },
        #                    "type": {
        #                        "map": {"count": dict(),                             # quantity distribution by type
        #                                                                             # How many P2SH  spent coins?
        #
        #                                "amount": dict(),                            # amounts distribution by type
        #                                                                             # What is the total amount
        #                                                                             # of all P2PKH spent?
        #
        #                                "size": dict()                               # sizes distribution by type
        #                                                                             # What is the total
        #                                                                             # size of all P2PKH spent?
        #
        #                                }},
        #
        #
        #                    # P2SH redeem script statistics
        #                    "P2SH": {
        #                        "type": {"map": {"count": dict(),
        #                                         "amount": dict(),
        #                                         "size": dict()}
        #                                 }
        #                    },
        #
        #                    # P2WSH redeem script statistics
        #                    "P2WSH": {
        #                        "type": {"map": {"count": dict(),
        #                                         "amount": dict(),
        #                                         "size": dict()}
        #                                 }
        #                    }
        #                    },
        #
        #         "transactions": {"count": {"total": 0},
        #
        #                          "amount": {"min": {"pointer": 0,
        #                                             "value": 0},
        #
        #                                     "max": {"pointer": 0,
        #                                             "value": 0},
        #
        #                                     "map": {"count": dict(),
        #                                             "amount": dict(),
        #                                             "size": dict()},
        #                                     "total": 0},
        #                          "size": {"min": {"pointer": 0, "value": 0},
        #                                   "max": {"pointer": 0, "value": 0},
        #                                   "total": {"size": 0, "bSize": 0, "vSize": 0},
        #                                   "map": {"count": dict(), "amount": dict()}},
        #
        #                          "type": {"map": {"count": dict(), "size": dict(),
        #                                           "amount": dict()}},
        #
        #                          "fee": {"min": {"pointer": 0, "value": 0},
        #                                  "max": {"pointer": 0, "value": 0},
        #                                  "total": 0}
        #                          }
        #          }

        if app.address_state:
            app.blockchain_address_sate = {

            }



    else:

        await conn.execute("""
                               INSERT INTO service (name, value) VALUES ('blockchain_analytica', '0')
                               ON CONFLICT(name) DO NOTHING;
                               """)

    m = await conn.fetchval("SELECT service.value FROM service WHERE service.name ='blockchain_analytica' LIMIT 1;")
    app.log.info("Option blockchain_analytica = %s" % m)

    if int(m) == 1 and not app.blockchain_analytica or app.blockchain_analytica and int(m) == 0:
        app.log.critical("blockchain_analytica config option not match db structure; you should drop db and recreate it.")
        raise Exception("DB structure invalid")




    # mempool_analytica

    app.log.info("Option mempool_analytica = 0")

    # transaction_fee_analytica

    app.log.info("Option transaction_fee_analytica = 0")

    # nodes_discovery

    app.log.info("Option nodes_discovery = 0")

    # market_data

    app.log.info("Option market_data = 0")


    # deterministic_wallet

    app.log.info("Option deterministic_wallet = 0")


    # payment_forwarding

    app.log.info("Option payment_forwarding = 0")


    # hot_wallet

    app.log.info("Option hot_wallet = 0")

    start_block = await conn.fetchval("SELECT height FROM blocks ORDER BY height DESC LIMIT 1;")
    app.start_checkpoint = -1 if start_block is None else start_block


    if "flush_mempool_on_start" in app.config["OPTIONS"]:
        if app.config["OPTIONS"]["flush_mempool_on_start"] == "on":
            app.log.info("Option flush_mempool_on_start = 1")
            app.log.info("Flush mempool ...")
            if app.transaction:
                await conn.execute("TRUNCATE TABLE unconfirmed_transaction;")
                await conn.execute("TRUNCATE TABLE invalid_transaction;")
            if app.transaction_history:
                await conn.execute("TRUNCATE TABLE unconfirmed_transaction_map;")
                await conn.execute("TRUNCATE TABLE invalid_transaction_map;")
            await conn.execute("TRUNCATE TABLE connector_unconfirmed_utxo;")
            await conn.execute("TRUNCATE TABLE connector_unconfirmed_stxo;")





    rows = await conn.fetch("SELECT hash from blocks order by height desc limit 100;")
    for row in rows:
        app.chain_tail.append(rh2s(row["hash"]))


