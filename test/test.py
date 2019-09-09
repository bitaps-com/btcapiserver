import block_endpoints
import transaction_endpoints
import address_endpoints

import unittest


testLoad = unittest.TestLoader()
block = testLoad.loadTestsFromModule(block_endpoints)
tx = testLoad.loadTestsFromModule(transaction_endpoints)
address = testLoad.loadTestsFromModule(address_endpoints)

runner = unittest.TextTestRunner(verbosity=1)
runner.run(block)
# runner.run(tx)
# runner.run(address)