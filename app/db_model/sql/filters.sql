
CREATE TABLE IF NOT EXISTS block_filters (height BIGINT NOT NULL,
                                          filter BYTEA,
                                          PRIMARY KEY(height));

CREATE TABLE IF NOT EXISTS block_filters_checkpoints (height BIGINT NOT NULL,
                                                      filter BYTEA,
                                          PRIMARY KEY(height));

CREATE TABLE IF NOT EXISTS block_filters_checkpoints2 (height BIGINT NOT NULL,
                                                      filter BYTEA,
                                          PRIMARY KEY(height));

CREATE TABLE IF NOT EXISTS block_filters_uncompressed (height BIGINT NOT NULL,
                                                       filter BYTEA,
                                                       PRIMARY KEY(height));
