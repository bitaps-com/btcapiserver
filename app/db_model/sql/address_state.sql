CREATE TABLE IF NOT EXISTS  address (address BYTEA PRIMARY KEY,
                                     data  bytea);

CREATE TABLE IF NOT EXISTS address_payments (pointer BIGINT,
                                             data BYTEA,
                                             PRIMARY KEY (pointer));

INSERT INTO service (name, value) VALUES ('address_last_block', NULL) ON CONFLICT(name) DO NOTHING;
