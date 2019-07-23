CREATE TABLE IF NOT EXISTS transaction (pointer BIGINT NOT NULL,
                                        tx_id BYTEA,
                                        timestamp INT,
                                        raw_transaction BYTEA,
                                        inputs_data BYTEA,
                                        PRIMARY KEY(pointer));

CREATE TABLE IF NOT EXISTS unconfirmed_transaction (tx_id BYTEA,
                                                    raw_transaction BYTEA,
                                                    timestamp INT,
                                                    inputs_data BYTEA,
                                                    PRIMARY KEY(tx_id));

CREATE TABLE IF NOT EXISTS invalid_transaction (tx_id BYTEA,
                                                raw_transaction BYTEA,
                                                timestamp INT,
                                                inputs_data BYTEA,
                                                PRIMARY KEY(tx_id));
