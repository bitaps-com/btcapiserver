
CREATE TABLE IF NOT EXISTS block_filters (height BIGINT NOT NULL,
                                          hash BYTEA,
                                          filter BYTEA,
                                          PRIMARY KEY(height));

CREATE TABLE IF NOT EXISTS block_range_filters (height BIGINT NOT NULL,
                                                hash BYTEA,
                                                filter BYTEA,
                                                PRIMARY KEY(height));


CREATE TABLE IF NOT EXISTS block_filters_uncompressed (height BIGINT NOT NULL,
                                                       filter BYTEA,
                                                       PRIMARY KEY(height));
