import pathlib

from views import *

PROJECT_ROOT = pathlib.Path(__file__).parent



def setup_routes(app):

    # Blocks
    # Base methods
    app.router.add_route('GET', '/rest/block/last', get_block_last)
    app.router.add_route('GET', '/rest/block/{block_pointer}', get_block_by_pointer)
    app.router.add_route('GET', '/rest/block/headers/{block_pointer}/{count}', get_block_headers)
    app.router.add_route('GET', '/rest/block/headers/{block_pointer}', get_block_headers)

    app.router.add_route('GET', '/rest/block/utxo/{block_pointer}', get_block_utxo) # test after sync completed

    app.router.add_route('GET', '/rest/address/state/{address}', get_address_state)
    app.router.add_route('POST', '/rest/addresses/state/by/address/list', get_address_state_by_list)

    app.router.add_route('GET', '/rest/address/utxo/{address}', get_address_confirmed_utxo)
    app.router.add_route('GET', '/rest/address/unconfirmed/utxo/{address}', get_address_unconfirmed_utxo)


    if app["transaction"]:
        app.router.add_route('GET', '/rest/block/transactions/{block_pointer}', get_block_transactions)
        app.router.add_route('GET', '/rest/block/transaction/id/list/{block_pointer}', get_block_transactions_list)

        app.router.add_route('GET', '/rest/transaction/{tx_pointer}', get_transaction_by_pointer)
        app.router.add_route('POST', '/rest/transactions/by/pointer/list', get_transaction_by_pointer_list)
        app.router.add_route('GET', '/rest/transaction/hash/by/pointer/{tx_blockchain_pointer}',
                             get_transaction_hash_by_pointer)
        app.router.add_route('POST', '/rest/transactions/hash/by/blockchain/pointer/list',
                             get_transactions_hash_by_pointer)

        if app["merkle_proof"]:
            app.router.add_route('GET', '/rest/transaction/merkle_proof/{tx_pointer}', get_transaction_merkle_proof)
        else:
            app.router.add_route('GET', '/rest/transaction/merkle_proof/{tx_pointer}',
                                 calculate_transaction_merkle_proof)

        app.router.add_route('GET', '/rest/transaction/calculate/merkle_proof/{tx_pointer}',
                             calculate_transaction_merkle_proof)

    if app["block_filters"]:
        # block filters

        # getcfheaders
        app.router.add_route('GET', '/rest/block/filters/headers/{filter_type}/{start_height}/{stop_hash}',
                             get_block_filters_headers)

        # getcfbheaders
        app.router.add_route('GET', '/rest/block/filters/batch/headers/{filter_type}/{start_height}/{stop_hash}',
                             get_block_filters_batch_headers)


        # getcfilters
        app.router.add_route('GET', '/rest/block/filters/{filter_type}/{start_height}/{stop_hash}', get_block_filters)

    # block
    # app.router.add_route('POST', '/rest/blocks/transactions/by/address/list', get_block_transactions)
    # app.router.add_route('POST', '/rest/blocks/transactions/by/filter', get_block_transactions)




    # Transactions
    #     Base methods
    # app.router.add_route('GET', '/rest/transaction/utxo/{tx_pointer}', get_transaction_by_hash)



    # app.router.add_route('POST', '/rest/transaction/broadcast}', broadcast_transaction)
    #
    # Address
    #     Base methods


    # app.router.add_route('GET', '/rest/addresses/utxo/by/address/list', get_address_confirmed_utxo)
    # app.router.add_route('GET', '/rest/address/unconfirmed/utxo/by/address/list', get_address_unconfirmed_utxo)


    app.router.add_route('GET', '/{tail:.*}', about)