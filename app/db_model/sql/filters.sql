
CREATE TABLE IF NOT EXISTS block_filter (height BIGINT NOT NULL,
                                         type SMALLINT,
                                         hash BYTEA,
                                         filter BYTEA,
                                         PRIMARY KEY(height, type));
CREATE TABLE IF NOT EXISTS block_filters_batch (height BIGINT NOT NULL PRIMARY KEY, data BYTEA);


CREATE TABLE IF NOT EXISTS raw_block_filters (height BIGINT NOT NULL, filter BYTEA, PRIMARY KEY(height));

