CREATE TABLE IF NOT EXISTS address_payments (pointer BIGINT,
                                             data BYTEA,
                                             PRIMARY KEY (pointer));

CREATE TABLE IF NOT EXISTS  address_daily(address BYTEA,
                                          day SMALLINT NOT NULL,
                                          data BYTEA,
                                          PRIMARY KEY (address, day));

CREATE TABLE IF NOT EXISTS  address_monthly(address BYTEA,
                                            month SMALLINT NOT NULL,
                                            data BYTEA,
                                            PRIMARY KEY (address, month));


ALTER TABLE blocks ADD COLUMN IF NOT EXISTS timestamp_received INT4 DEFAULT NULL;
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS adjusted_timestamp INT4 DEFAULT NULL;
