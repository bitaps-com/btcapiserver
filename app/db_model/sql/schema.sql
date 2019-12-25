CREATE TABLE IF NOT EXISTS blocks (height BIGINT NOT NULL,
                                   hash BYTEA,
                                   adjusted_timestamp INT,
                                   timestamp_received INT,
                                   header BYTEA,
                                   PRIMARY KEY(height));

CREATE TABLE IF NOT EXISTS transaction (pointer BIGINT NOT NULL,
                                        tx_id BYTEA,
                                        timestamp INT,
                                        raw_transaction BYTEA,
                                        PRIMARY KEY(pointer));


CREATE TABLE IF NOT EXISTS unconfirmed_transaction (tx_id BYTEA,
                                                    raw_transaction BYTEA,
                                                    timestamp INT,
                                                    PRIMARY KEY(tx_id));

CREATE TABLE IF NOT EXISTS invalid_transaction (tx_id BYTEA,
                                                raw_transaction BYTEA,
                                                timestamp INT,
                                                PRIMARY KEY(tx_id));

CREATE TABLE  IF NOT EXISTS service(name VARCHAR PRIMARY KEY, value VARCHAR );

INSERT INTO service (name, value) VALUES ('bootstrap_completed', '0') ON CONFLICT(name) DO NOTHING;




/*
CREATE INDEX IF NOT EXISTS txmap_address_map_amount
  ON transaction_map USING BTREE (address, pointer);

CREATE INDEX IF NOT EXISTS tx_map_tx_id
  ON transaction USING BTREE (tx_id);



CREATE INDEX IF NOT EXISTS utxo_address_map_amount
 ON connector_utxo USING BTREE (address, amount);
 */



create extension if not exists plpython3u;

























