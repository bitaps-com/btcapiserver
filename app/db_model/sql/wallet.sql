
CREATE TABLE IF NOT EXISTS
          wallet_utxo (pointer BIGINT,
                       wallet BYTEA,
                       path BYTEA,
                       amount  BIGINT,
                       PRIMARY KEY(pointer));


CREATE TABLE IF NOT EXISTS
          wallet (wallet BYTEA PRIMARY KEY,
                  wallet_id BIGINT,
                  sequence INT,

                  received_count  BIGINT NOT NULL DEFAULT 0,
                  received_amount  BIGINT NOT NULL DEFAULT 0,
                  coins  BIGINT NOT NULL DEFAULT 0,

                  sent_count  BIGINT NOT NULL DEFAULT 0,
                  sent_amount  BIGINT NOT NULL DEFAULT 0,
                  coins_destroyed  BIGINT NOT NULL DEFAULT 0

                                     );

CREATE TABLE IF NOT EXISTS
          wallet_address (wallet_id BIGINT,
                          path BYTEA,
                          address BYTEA,
                          PRIMARY KEY(wallet_id));


CREATE TABLE IF NOT EXISTS wallet_transaction_map (pointer BIGINT,
                                                   wallet_id BIGINT NOT NULL,
                                                   address BYTEA,
                                                   amount BIGINT,
                                                   data BYTEA,
                                                   PRIMARY KEY (pointer));



CREATE TABLE IF NOT EXISTS
          wallet_daily (wallet_id BIGINT, PRIMARY KEY,
                        sequence INT,

                        received_count  BIGINT NOT NULL DEFAULT 0,
                        received_amount  BIGINT NOT NULL DEFAULT 0,
                        coins  BIGINT NOT NULL DEFAULT 0,

                        sent_count  BIGINT NOT NULL DEFAULT 0,
                        sent_amount  BIGINT NOT NULL DEFAULT 0,
                        coins_destroyed  BIGINT NOT NULL DEFAULT 0

                                     );


CREATE TABLE IF NOT EXISTS
          wallet_monthly (wallet_id BIGINT PRIMARY KEY,
                          sequence INT,

                          received_count  BIGINT NOT NULL DEFAULT 0,
                          received_amount  BIGINT NOT NULL DEFAULT 0,
                          coins  BIGINT NOT NULL DEFAULT 0,

                          sent_count  BIGINT NOT NULL DEFAULT 0,
                          sent_amount  BIGINT NOT NULL DEFAULT 0,
                          coins_destroyed  BIGINT NOT NULL DEFAULT 0

                                     );






