CREATE TABLE IF NOT EXISTS stxo (pointer BIGINT NOT NULL PRIMARY KEY,
                                 s_pointer BIGINT,
                                 address BYTEA NOT NULL,
                                 amount BIGINT);



CREATE TABLE IF NOT EXISTS transaction_map (address BYTEA NOT NULL,
                                            pointer BIGINT NOT NULL)
                                            PARTITION BY HASH (address);

CREATE TABLE IF NOT EXISTS unconfirmed_transaction_map (tx_id BYTEA NOT NULL, address BYTEA NOT NULL);

CREATE TABLE IF NOT EXISTS invalid_transaction_map (tx_id BYTEA, address BYTEA NOT NULL);


CREATE INDEX IF NOT EXISTS unconfirmed_transaction_map_tx_id ON unconfirmed_transaction_map USING HASH (tx_id);