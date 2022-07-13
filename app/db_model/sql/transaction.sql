CREATE TABLE IF NOT EXISTS transaction (pointer BIGINT NOT NULL,
                                        tx_id BYTEA,
                                        timestamp INT,
                                        raw_transaction BYTEA,
                                        PRIMARY KEY(pointer));


CREATE TABLE IF NOT EXISTS unconfirmed_transaction (tx_id BYTEA,
                                                    raw_transaction BYTEA,
                                                    timestamp INT,
                                                    id BIGSERIAL,
                                                    PRIMARY KEY(tx_id));

CREATE TABLE IF NOT EXISTS invalid_transaction (tx_id BYTEA,
                                                raw_transaction BYTEA,
                                                timestamp INT,
                                                invalidation_timestamp INT,
                                                PRIMARY KEY(tx_id));

CREATE TABLE IF NOT EXISTS  invalid_utxo (outpoint BYTEA,
                                          out_tx_id BYTEA,
                                          address BYTEA,
                                          amount  BIGINT,
                                          PRIMARY KEY (outpoint));

CREATE TABLE IF NOT EXISTS  invalid_stxo (outpoint BYTEA,
                                          sequence  INT,
                                          out_tx_id BYTEA,
                                          tx_id BYTEA,
                                          input_index INT,
                                          address BYTEA,
                                          amount BIGINT,
                                          pointer BIGINT,
                                          PRIMARY KEY(outpoint, sequence));

CREATE INDEX IF NOT EXISTS unconfirmed_transaction_time ON unconfirmed_transaction USING BTREE (timestamp);
CREATE INDEX IF NOT EXISTS unconfirmed_transaction_id ON unconfirmed_transaction USING BTREE (id);

CREATE INDEX IF NOT EXISTS invalid_utxo_out_tx_id ON invalid_utxo USING BTREE (out_tx_id);
CREATE INDEX IF NOT EXISTS invalid_utxo_address ON invalid_utxo USING BTREE (address);

CREATE INDEX IF NOT EXISTS invalid_stxo_out_tx_id ON invalid_stxo USING BTREE (out_tx_id);
CREATE INDEX IF NOT EXISTS invalid_stxo_address ON invalid_stxo USING BTREE (address);
CREATE INDEX IF NOT EXISTS invalid_stxo_tx_id ON invalid_stxo USING BTREE (tx_id);
CREATE INDEX IF NOT EXISTS invalid_transaction_timestamp ON invalid_transaction USING BTREE (invalidation_timestamp);

CREATE OR REPLACE FUNCTION set_invalidation_timestamp()
                            RETURNS TRIGGER AS $$
                            BEGIN
                                SELECT extract(epoch FROM now()) INTO NEW.invalidation_timestamp;
                                RETURN NEW;
                            END;
                            $$ LANGUAGE 'plpgsql';
DROP TRIGGER IF EXISTS before_insert_invalid_transaction ON invalid_transaction;
CREATE TRIGGER before_insert_invalid_transaction BEFORE INSERT ON invalid_transaction FOR EACH ROW EXECUTE PROCEDURE  set_invalidation_timestamp();