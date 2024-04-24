from functools import lru_cache
from urllib.request import urlopen
import json


@lru_cache(maxsize=None)
def call_api(url: str):
    response = urlopen(url=url)
    return json.loads(response.read().decode("utf-8"))
