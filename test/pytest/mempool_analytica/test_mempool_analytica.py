import requests
from pybtc import *
import base64


def test_get_mempool_analytica(conf):
    if not conf["option_mempool_analytica"]:
        return
    r = requests.get(conf["base_url"] + "/rest/mempool/state")
    assert r.status_code == 200
    d = r.json()["data"]