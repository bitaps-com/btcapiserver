import pathlib

from views import *

PROJECT_ROOT = pathlib.Path(__file__).parent


def setup_routes(app):
    # Blocks
    #     Base methods
    app.router.add_route('GET', '/rest/block/last', get_block_last)
    app.router.add_route('GET', '/rest/block/{block_pointer}', get_block_by_pointer)
    app.router.add_route('GET', '/rest/block/headers/{block_pointer}/{count}', get_block_headers)
    app.router.add_route('GET', '/rest/block/headers/{block_pointer}', get_block_headers)
    app.router.add_route('GET', '/rest/block/utxo/{block_pointer}', get_block_utxo) # test after sync completed
    app.router.add_route('GET', '/rest/block/transactions/{block_pointer}', get_block_transactions)


    # Transactions
    #     Base methods
    app.router.add_route('GET', '/rest/transaction/{tx_pointer}', get_transaction_by_pointer)

    # app.router.add_route('GET', '/rest/transaction/merkle_proof/{tx_hash}', get_transaction_merkle_proof)
    # app.router.add_route('GET', '/rest/transaction/utxo/{tx_pointer}', get_transaction_by_hash)
    # app.router.add_route('POST', '/rest/transaction/broadcast}', broadcast_transaction)
    #
    # # Address
    # #     Base methods
    app.router.add_route('GET', '/rest/address/{address}', get_address_state)
    app.router.add_route('GET', '/rest/address/confirmed/utxo/{address}', get_address_confirmed_utxo)
    # app.router.add_route('GET', '/rest/address/unconfirmed/utxo/{address}', get_transaction_by_hash)

    app.router.add_route('GET', '/{tail:.*}', about)