import pytest
import configparser

config_file =   "../config/btcapi-server.conf"
config = configparser.ConfigParser()
config.read(config_file)

option_transaction = True if config["OPTIONS"]["transaction"] == "on" else False
option_merkle_proof = True if config["OPTIONS"]["merkle_proof"] == "on" else False
option_address_state = True if config["OPTIONS"]["address_state"] == "on" else False
option_address_timeline = True if config["OPTIONS"]["address_timeline"] == "on" else False
option_blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
option_transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False
testnet = int(config["CONNECTOR"]["testnet"])
base_url = config["SERVER"]["api_endpoint_test_base_url"]


@pytest.fixture
def conf():
    config_file = "../config/btcapi-server.conf"
    config = configparser.ConfigParser()
    config.read(config_file)
    option_transaction = True if config["OPTIONS"]["transaction"] == "on" else False
    option_merkle_proof = True if config["OPTIONS"]["merkle_proof"] == "on" else False
    option_address_state = True if config["OPTIONS"]["address_state"] == "on" else False
    option_address_timeline = True if config["OPTIONS"]["address_timeline"] == "on" else False
    option_mempool_analytica = True if config["OPTIONS"]["mempool_analytica"] == "on" else False
    option_blockchain_analytica = True if config["OPTIONS"]["blockchain_analytica"] == "on" else False
    option_transaction_history = True if config["OPTIONS"]["transaction_history"] == "on" else False
    base_url = config["SERVER"]["api_endpoint_test_base_url"]
    return {"option_transaction": option_transaction,
            "option_merkle_proof": option_merkle_proof,
            "option_address_state": option_address_state,
            "option_address_timeline": option_address_timeline,
            "option_blockchain_analytica": option_blockchain_analytica,
            "option_mempool_analytica": option_mempool_analytica,
            "option_transaction_history": option_transaction_history,
            "base_url": base_url,
            "testnet": testnet}


