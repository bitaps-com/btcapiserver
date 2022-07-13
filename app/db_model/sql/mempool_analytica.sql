ALTER TABLE unconfirmed_transaction ADD COLUMN IF NOT EXISTS size INT4;
ALTER TABLE unconfirmed_transaction ADD COLUMN IF NOT EXISTS b_size INT4;
ALTER TABLE unconfirmed_transaction ADD COLUMN IF NOT EXISTS rbf SMALLINT;
ALTER TABLE unconfirmed_transaction ADD COLUMN IF NOT EXISTS fee BIGINT;
ALTER TABLE unconfirmed_transaction ADD COLUMN IF NOT EXISTS feeRate FLOAT;
ALTER TABLE unconfirmed_transaction ADD COLUMN IF NOT EXISTS amount BIGINT;
ALTER TABLE unconfirmed_transaction ADD COLUMN IF NOT EXISTS segwit SMALLINT;

ALTER TABLE invalid_transaction ADD COLUMN IF NOT EXISTS size INT4;
ALTER TABLE invalid_transaction ADD COLUMN IF NOT EXISTS b_size INT4;
ALTER TABLE invalid_transaction ADD COLUMN IF NOT EXISTS rbf SMALLINT;
ALTER TABLE invalid_transaction ADD COLUMN IF NOT EXISTS fee BIGINT;
ALTER TABLE invalid_transaction ADD COLUMN IF NOT EXISTS feeRate FLOAT;
ALTER TABLE invalid_transaction ADD COLUMN IF NOT EXISTS amount BIGINT;
ALTER TABLE invalid_transaction ADD COLUMN IF NOT EXISTS segwit SMALLINT;

CREATE TABLE IF NOT EXISTS  mempool_analytica(minute INT4 PRIMARY KEY,
                                              hour INT4,
                                              day SMALLINT,
                                              inputs JSONB,
                                              outputs JSONB,
                                              transactions JSONB);


CREATE TABLE IF NOT EXISTS  mempool_dbs(tx_id BYTEA PRIMARY KEY, timestamp INT4, child SMALLINT, id BIGSERIAL);


CREATE INDEX IF NOT EXISTS mempool_dbs_timestamp ON mempool_dbs USING hash (timestamp);
CREATE INDEX IF NOT EXISTS mempool_dbs_id ON mempool_dbs USING hash (id);
CREATE INDEX IF NOT EXISTS mempool_dbs_childs_timestamp ON mempool_dbs USING BTREE (child, timestamp);

CREATE OR REPLACE FUNCTION set_fee_rate_column()
                            RETURNS TRIGGER AS $$
                            BEGIN
                                NEW.feeRate = NEW.fee / (((NEW.b_size * 3 + NEW.size)/4) ) :: float;
                                RETURN NEW;
                            END;
                            $$ LANGUAGE 'plpgsql';

DROP TRIGGER IF EXISTS before_insert_unconfirmed_transaction ON unconfirmed_transaction;

CREATE TRIGGER before_insert_unconfirmed_transaction BEFORE INSERT ON unconfirmed_transaction FOR EACH ROW EXECUTE PROCEDURE  set_fee_rate_column();

CREATE INDEX IF NOT EXISTS unconfirmed_transaction_fee_queue ON unconfirmed_transaction (feeRate DESC) ;
