CREATE TABLE IF NOT EXISTS transaction_map (pointer BIGINT,
                                            address BYTEA NOT NULL,
                                            amount BIGINT,
                                            PRIMARY KEY (pointer));


CREATE TABLE IF NOT EXISTS unconfirmed_transaction_map (tx_id BYTEA,
                                                        pointer BIGINT,
                                                        address BYTEA NOT NULL,
                                                        amount BIGINT,
                                                        PRIMARY KEY (tx_id, pointer));

CREATE TABLE IF NOT EXISTS invalid_transaction_map (tx_id BYTEA,
                                                        pointer BIGINT,
                                                        address BYTEA NOT NULL,
                                                        amount BIGINT,
                                                        PRIMARY KEY (tx_id, pointer));

CREATE TABLE IF NOT EXISTS stxo (pointer BIGINT NOT NULL PRIMARY KEY,
                                 s_pointer BIGINT);


CREATE INDEX IF NOT EXISTS utxmap_address_map_amount ON unconfirmed_transaction_map USING BTREE (address, pointer);
CREATE INDEX IF NOT EXISTS itxmap_address_map_amount ON invalid_transaction_map USING BTREE (address, pointer);
