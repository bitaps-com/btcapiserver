CREATE TABLE IF NOT EXISTS stxo (pointer BIGINT NOT NULL PRIMARY KEY,
                                 s_pointer BIGINT);

/*CREATE INDEX IF NOT EXISTS transactions_map_tx_id ON transaction USING BTREE (tx_id);*/