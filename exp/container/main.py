import time
from Store import Store
import container_config
import redis

redis_server = redis.StrictRedis(host=container_config.REDIS_HOST,
                                 port=container_config.REDIS_PORT, db=container_config.REDIS_DB)

def main():
    store = Store('wordcount-multistage-16', 'function', 'request_id', {},
                  output, to, keys, runtime, db_server, redis_server)
    input_res = store.fetch(store.input.keys())
    for k in input_res.keys():
        print(k)
    output_res = {}
    for (k, v) in store.output.items():
        result = 'a' * v['size']
        output_res[k] = result
    time.sleep(store.runtime)
    store.put(output_res, {})
