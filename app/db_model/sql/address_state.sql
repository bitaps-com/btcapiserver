

CREATE TABLE IF NOT EXISTS  address (address BYTEA PRIMARY KEY,
                                     balance BIGINT,
                                     data  bytea);

CREATE TABLE IF NOT EXISTS address_payments (pointer BIGINT,
                                             data BYTEA,
                                             PRIMARY KEY (pointer));

CREATE TABLE IF NOT EXISTS block_address_stat(height BIGINT NOT NULL PRIMARY KEY, addresses JSONB);
CREATE TABLE IF NOT EXISTS blockchian_address_stat(height BIGINT NOT NULL PRIMARY KEY, addresses JSONB);

INSERT INTO service (name, value) VALUES ('address_last_block', -1) ON CONFLICT(name) DO NOTHING;


