CREATE TABLE IF NOT EXISTS blocks_stat (height BIGINT NOT NULL,
                                        block JSONB,
                                        blockchain JSONB,
                                        blockchain_addresses JSONB,
                                        PRIMARY KEY(height));


CREATE TABLE IF NOT EXISTS blocks_daily_stat (day INT4 NOT NULL,
                                              block JSONB,
                                              blockchain JSONB,
                                              blockchain_addresses JSONB,
                                              PRIMARY KEY(day));

CREATE TABLE IF NOT EXISTS blocks_weekly_stat (week INT4 NOT NULL,
                                               block JSONB,
                                               blockchain JSONB,
                                               blockchain_addresses JSONB,
                                               PRIMARY KEY(week));


CREATE TABLE IF NOT EXISTS blocks_monthly_stat (month INT4 NOT NULL,
                                                block JSONB,
                                                blockchain JSONB,
                                                blockchain_addresses JSONB,
                                                PRIMARY KEY(month));


INSERT INTO service (name, value) VALUES ('blocks_address_stat_last_block', '0') ON CONFLICT(name) DO NOTHING;


/*

    block_data = {"merkleRoot": bytes,
                  "medianBlockTime": int,
                  "strippedSize":int,
                  "weight":int,
                  "bits": int,
                  "nonce": int,
                  "version": int,
                  "difficulty": int,
                  "size": int
                  }
    block_stat = {"outputs": {"count": 0,
                              "amount": {"min": {"pointer": None, "value": None},
                                         "max": {"pointer": None, "value": None},
                                        "total": 0},
                             "typeMap": dict(),
                             "amountMap": dict()},
                  "inputs": {"count": 0,
                             "amount": {"min": {"pointer": None, "value": None},
                                        "max": {"pointer": None, "value": None},
                                        "total": 0},
                             "typeMap": dict(),
                             "amountMap": dict()},
                  "transactions": {"block": {"count": 0,
                                   "amount": {"min": {"ipointerd": None, "value": None},
                                              "max": {"pointer": None, "value": None},
                                              "total": 0},
                                   "size": {"min": {"pointer": None, "value": None},
                                            "max": {"pointer": None, "value": None},
                                            "total": 0},
                                   "fee": {"min": {"pointer": None, "value": None},
                                           "max": {"pointer": None, "value": None},
                                           "total": 0},
                                   "feeRate": {"min": {"pointer": None, "value": None},
                                           "max": {"pointer": None, "value": None},
                                           "total": 0},
                                   "amountMap": dict(),
                                   "flagMap": dict(),
                                   "sizeMap": dict(),
                                   "typeMap": dict(),
                                   "feeMap": dict()
                                   "feeRateMap": dict()
                                   },
                  "addresses": {"count": {"total": int},
                                "amount": {"min": {"address": None, "value": None},
                                           "max": {"pointer": None, "value": None},
                                           "total": 0,
                                           "receivedMap": dict(),
                                           "sentMap": dict()},
                                "typeMap": dict()}

blockchain_stat = {"outputs": {"count": {"total": int},            # total outputs count
                                                                   # What is the total quantity of
                                                                   # coins in bitcoin blockchain?

                               "amount": {"min": {"pointer": int,  # output with minimal amount
                                                  "value": int},   # What is the minimum amount of a coins?

                                          "max": {"pointer": int,  # output with maximal amount
                                                  "value": int},   # What is the maximal amount of a coins?

                                          "total": int,            # total amount of all outputs
                                                                   # What is the total amount of all coins?

                                          "map": {"count": dict(), # quantity distribution by amount
                                                                   # How many coins exceed 1 BTC?

                                                  "amount": dict(),# amounts distribution by amount
                                                                   # What is the total amount of all coins
                                                                   # exceeding 10 BTC?
                                                  },

                               "type": {
                                        "map": {"count": dict(),   # quantity distribution by type
                                                                   # How many P2SH coins?

                                                "amount": dict(),  # amounts distribution by type
                                                                   # What is the total amount of all P2PKH coins?

                                                "size": dict()     # sizes distribution by type
                                                                   # What is the total size of all P2PKH coins?
                                                }

                                        }


                               # coin age only for unspent coins
                               "age": { "map": {"count": dict(),  # distribution of counts by age
                                                                  # How many coins older then 1 year?

                                                "amount": dict(), # distribution of amount by age
                                                                  # What amount of coins older then 1 month?

                                                "type": dict()    # distribution of counts by type
                                                                  # How many P2SH coins older then 1 year?
                                                }
                               }

                   "inputs": {"count": {"total": int},            # total inputs count
                                                                  # What is the total quantity of
                                                                  # spent coins in bitcoin blockchain?

                              "amount": {
                                         "min": {"pointer": int,  # input with minimal amount
                                                 "value": int},   # What is the smallest coin spent?

                                         "max": {"pointer": int,  # input with maximal amount
                                                 "value": int},   # what is the greatest coin spent?

                                         "total": int,            # total amount of all inputs
                                                                  # What is the total amount of all spent coins?

                                         "map": {"count": dict(), # quantity distribution by amount
                                                                  # How many spent coins exceed 1 BTC?

                                                 "amount": dict(),# amounts distribution by amount
                                                                  # What is the total amount of all spent coins
                                                                  # exceeding 10 BTC?
                                                 },
                                       },

                              "type": {
                                       "map": {"count": dict(),   # quantity distribution by type
                                                                  # How many P2SH  spent coins?

                                               "amount": dict(),  # amounts distribution by type
                                                                  # What is the total amount of all P2PKH spent?

                                               "size": dict()     # sizes distribution by type
                                                                  # What is the total size of all P2PKH spent?

                                        }

                              # coin days destroyed
                              "destroyed": { "map": {"count": dict(),  # quantity distribution by destroyed days
                                                                       # How many coins destroyed
                                                                       # with coin days greater then 1000000000?

                                            "amount": dict(), # amount distribution by destroyed days


                                            "type": dict()    # coin days destroyed distribution by type

                                                }
                               }

                               # P2SH redeem script statistics
                               "P2SH": {
                                        "type": {"map": {"count": dict(), # quantity distribution by type
                                                                          # How many 2of3 Multisig coins?

                                                         "amount": dict(),# amount distribution by type
                                                                          # What is the total amount of
                                                                          # all spent 2of3 Multisig?

                                                         "size": dict()   # sizes distribution by type
                                                                          # What is the total size of
                                                                          # redeem scripts for
                                                                          # all spent 2of3 Multisig?
                                                          }
                                                   }
                                        }

                               # P2WSH redeem script statistics
                               "P2WSH": {
                                        "type": {"map": {"count": dict(), # quantity distribution by type
                                                                          # How many 2of3 Multisig coins?

                                                         "amount": dict(),# amount distribution by type
                                                                          # What is the total amount of
                                                                          # all spent 2of3 Multisig?

                                                         "size": dict()   # sizes distribution by type
                                                                          # What is the total size of
                                                                          # redeem scripts for
                                                                          # all spent 2of3 Multisig?
                                                          }
                                                   }
                                        }



                        "addresses": {"count": {"total": int},
                                      "amount": {"total": int,
                                                 "min": {"address": None, "value": None},
                                                 "max": {"address": None, "value": None},
                                                 "map": {"count": dict(),
                                                         "amount": {"received": dict(),
                                                                    "sent": dict()}}
                                     "reused": {"total": int,
                                                "amount": int,
                                                "map": {"type": dict(), # How many segwit addresses reused?
                                                        "amount": dict() # How many addresses greater then 10 btc
                                                                         # reused

                                                        }

                                    "coin": "map": "count"
                                    "coinDays": "map": "count"
                                    "transactions" "map": "count"

"MTO" stands for many-to-one. It refers to transactions that have many inputs (at least 3) and exactly one output. This is a stricter definition of consolidation.


                        "transactions":  {"count": {"total": int,


                                                },
                                          "io": {

                                             "outputs": {"count": {"map": "outCount": dict()}
                                                                   "reduce": int}
                                             "inputs": {"count": "map": "inputCount": dict()}}

                                          "amount": {"min": {"pointer": None, "value": None},
                                                     "max": {"pointer": None, "value": None},
                                                     "map" {"count": dict(),
                                                            "amount": dict(),
                                                            "size": dict(),
                                                            "feeRate: dict(),
                                                            "outFeeRate": dict()
                                                     "total": 0},
                                   "size": {"min": {"pointer": None, "value": None},
                                            "max": {"pointer": None, "value": None},
                                            "total": {
                                                      "size": int,
                                                      "bSize": int,
                                                      "vSize": int},
                                            "map": {"count": dict(),
                                                    "amount": dict()}
                                   "type": {"map": "count": dict(),
                                                   "size" dict(),
                                                   "amount": dict()

                                                         "bip69" "segwit","rbf", "coinbase: int

                                   "fee": {"min": {"pointer": None, "value": None},
                                           "max": {"pointer": None, "value": None},
                                           "total": 0},
                                   "feeRate": {"min": {"pointer": None, "value": None},
                                           "max": {"pointer": None, "value": None},
                                           "average": 0,
                                           "map": {"count":
                                                   "amount":
                                                   "size"},
                                   "outFeeRate": { "average": 0
                                           "map": {"count":
                                                   "amount":
                                                   "size"},
                                   },



 */