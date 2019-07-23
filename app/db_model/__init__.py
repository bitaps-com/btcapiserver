from pybtc import rh2s

async def create_db_model(app, conn):

    # check db isolation level

    level = await conn.fetchval("SHOW TRANSACTION ISOLATION LEVEL;")
    if level != "repeatable read":
        raise Exception("Postgres repeatable read isolation "
                        "level required! current isolation level is %s" % level)
    await conn.execute(open("./db_model/sql/schema.sql", "r", encoding='utf-8').read().replace("\n",""))



    # transaction

    if app.transaction:
        await conn.execute(open("./db_model/sql/transaction.sql", "r", encoding='utf-8').read().replace("\n", ""))
        await conn.execute("""
                           INSERT INTO service (name, value) VALUES ('transaction', '1')  
                           ON CONFLICT(name) DO NOTHING;
                           """)
        app.transaction_start_block = await conn.fetchval("SELECT pointer "
                                                          "FROM transaction ORDER BY pointer DESC LIMIT 1;")
        app.transaction_start_block = 0 if app.transaction_start_block is None else app.transaction_start_block >> 39
    else:
        await conn.execute("""
                           INSERT INTO service (name, value) VALUES ('transaction', '0') 
                           ON CONFLICT(name) DO NOTHING;
                           """)

    m = await conn.fetchval("SELECT value FROM service WHERE name ='transaction' LIMIT 1;")
    app.log.info("Option transaction = %s" % m)

    if bool(int(m)) !=  app.transaction:
        app.log.critical("transaction option not match db structure; "
                         "you should drop db and recreate it")
        raise Exception("DB structure invalid")




    # merkle proof module

    if app.merkle_proof:
        if not app.transaction:
            app.log.critical("merkle_proof option required transaction option enabled")
            raise Exception("configuration invalid")

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

    app.block_start_block = await conn.fetchval("SELECT height FROM blocks ORDER BY height DESC LIMIT 1;")

    app.block_start_block = 0 if app.block_start_block is None else app.block_start_block

    app.start_checkpoint = min(app.transaction_map_start_block,
                               app.transaction_start_block,
                               app.block_start_block)


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

