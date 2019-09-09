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
    #
    app.router.add_route('GET', '/rest/block/utxo/{block_pointer}', get_block_utxo) # test after sync completed
    app.router.add_route('GET', '/rest/block/transactions/{block_pointer}', get_block_transactions)

    if app["block_filters"]:
        # block range filters

        app.router.add_route('GET', '/rest/block/range/filter/headers/{from_filter_header}',
                             get_block_range_filter_headers)

        app.router.add_route('GET', '/rest/block/range/filter/headers', get_block_range_filter_headers)

        app.router.add_route('GET', '/rest/block/range/filter/{filter_header}', get_block_range_filter)

        # block filters
        app.router.add_route('GET', '/rest/block/filter/headers/{block_pointer}/{count}', get_block_filter_headers)
        app.router.add_route('GET', '/rest/block/filter/headers/{block_pointer}', get_block_filter_headers)

        app.router.add_route('GET', '/rest/block/filters/{block_pointer}', get_block_filters)
        app.router.add_route('GET', '/rest/block/filters/{block_pointer}/{count}', get_block_filters)

        app.router.add_route('GET', '/rest/block/filter/{filter_header}', get_block_filter)



    # block

    # app.router.add_route('POST', '/rest/blocks/transactions/by/address/list', get_block_transactions)
    # app.router.add_route('POST', '/rest/blocks/transactions/by/filter', get_block_transactions)


    # Transactions
    #     Base methods
    app.router.add_route('GET', '/rest/transaction/{tx_pointer}', get_transaction_by_pointer)
    app.router.add_route('POST', '/rest/transactions/by/pointer/list', get_transaction_by_pointer_list)

    app.router.add_route('GET', '/rest/transaction/hash/by/blockchain/pointer/{tx_blockchain_pointer}',
                         get_transaction_hash_by_pointer)
    app.router.add_route('POST', '/rest/transactions/hash/by/blockchain/pointer/list', get_transactions_hash_by_pointer)

    if app["merkle_proof"]:
        app.router.add_route('GET', '/rest/transaction/merkle_proof/{tx_pointer}', get_transaction_merkle_proof)

    # app.router.add_route('GET', '/rest/transaction/utxo/{tx_pointer}', get_transaction_by_hash)
    # app.router.add_route('POST', '/rest/transaction/broadcast}', broadcast_transaction)
    #
    # # Address
    # #     Base methods
    app.router.add_route('GET', '/rest/address/state/{address}', get_address_state)
    app.router.add_route('POST', '/rest/addresses/state/by/address/list', get_address_state_by_list)

    app.router.add_route('GET', '/rest/address/utxo/{address}', get_address_confirmed_utxo)
    app.router.add_route('GET', '/rest/address/confirmed/utxo/{address}', get_address_confirmed_utxo)
    app.router.add_route('GET', '/rest/address/unconfirmed/utxo/{address}', get_address_unconfirmed_utxo)

    app.router.add_route('GET', '/rest/addresses/utxo/by/address/list', get_address_confirmed_utxo)
    app.router.add_route('GET', '/rest/addresses/confirmed/utxo/by/address/list', get_address_confirmed_utxo)
    app.router.add_route('GET', '/rest/address/unconfirmed/utxo/by/address/list', get_address_unconfirmed_utxo)


    app.router.add_route('GET', '/test/filter', test_filter)
    app.router.add_route('GET', '/{tail:.*}', about)