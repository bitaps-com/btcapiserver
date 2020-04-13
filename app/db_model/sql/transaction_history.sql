CREATE TABLE IF NOT EXISTS stxo (pointer BIGINT NOT NULL PRIMARY KEY,
                                 s_pointer BIGINT,
                                 address BYTEA NOT NULL,
                                 amount BIGINT);



CREATE TABLE IF NOT EXISTS transaction_map (address BYTEA NOT NULL,
                                            pointer BIGINT,
                                            amount BIGINT);


CREATE TABLE IF NOT EXISTS unconfirmed_transaction_map (tx_id BYTEA,
                                                        address BYTEA NOT NULL,
                                                        amount BIGINT,
                                                        PRIMARY KEY(address, tx_id));

CREATE TABLE IF NOT EXISTS invalid_transaction_map (tx_id BYTEA,
                                                    address BYTEA NOT NULL,
                                                    amount BIGINT,
                                                    PRIMARY KEY(address, tx_id));


