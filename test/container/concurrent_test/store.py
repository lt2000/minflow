import zipfile
import shutil
import os
import time
import re
import base64
import couchdb
import json
import threading
from flask import Flask, request
from gevent.pywsgi import WSGIServer

result_dir = "."
'''
couchdb_url = 'http://openwhisk:openwhisk@10.2.64.8:5984/'
db_server = couchdb.Server(couchdb_url)
if 'results' in db_server:
    db_server.delete('results')
db_server.create('results')
'''

class Store:
    def __init__(self):
        couchdb_url = 'http://openwhisk:openwhisk@10.2.64.8:5984/'
        db_server = couchdb.Server(couchdb_url)
        self.db = db_server['results']
        self.fetch_dict = {}
        self.put_dict = {}
    
    def init(self, function):
        self.function = function

    def fetch_from_mem(self, path, key):
        with open(path, "r") as f:
            json_file = json.load(f)
            self.fetch_dict[key] = json_file['value']

    def fetch_from_db(self, doc, key):
        self.fetch_dict[key] = self.db[doc]['value']

    def fetch(self, request_id, input):
        self.fetch_dict = {}
        threads = []
        for (k, v) in input.items():
            if v['type'] == 'DB': #DB
                doc = request_id + "_" + k
                thread_ = threading.Thread(target=self.fetch_from_db, args=(doc, k,))
                threads.append(thread_)
            else: # MEM
                path = os.path.join(result_dir, request_id + "_" + k + ".json")
                thread_ = threading.Thread(target=self.fetch_from_mem, args=(path, k,))
                threads.append(thread_)
        for thread_ in threads:
            thread_.start()
        for thread_ in threads:
            thread_.join()
        return self.fetch_dict

    def put_to_mem(self, path, key):
        with open(path, "w") as f:
            json_file = {"key": key, "value": self.put_dict[key]}
            json.dump(json_file, f)
    
    def put_to_db(self, doc, key):
        self.db[doc] = {"key": key, "value":  self.put_dict[key]}

    def put(self, request_id, output, output_res):
        self.put_dict = output_res 
        threads = []
        for (k, v) in output_res.items():
            if 'DB' in output[k]['type']:
                doc = request_id + "_" + k
                thread_ = threading.Thread(target=self.put_to_db, args=(doc, k,))
                threads.append(thread_)
            if 'MEM' in output[k]['type']:
                path = os.path.join(result_dir, request_id + "_" + k + ".json")
                thread_ = threading.Thread(target=self.put_to_mem, args=(path, k,))
                threads.append(thread_)
        for thread_ in threads:
            thread_.start()
        for thread_ in threads:
            thread_.join()
    
    def fetch2(self, request_id, input):
        input_res = {}
        for (k, v) in input.items():
            if v['type'] == 'DB':
                doc = request_id + "_" + k
                input_res[k] = self.db[doc]['value']
            else: # MEM
                path = request_id + "_" + k + ".json"
                with open(os.path.join(result_dir, path), "r") as f:
                    json_file = json.load(f)
                    input_res[k] = json_file['value']
        return input_res
        
    def put2(self, request_id, output, output_res):
        for (k, v) in output_res.items():
            if 'DB' in output[k]['type']:
                doc = request_id + "_" + k
                self.db[doc] = {"key": k, "value": v}
            if 'MEM' in output[k]['type']:
                path = request_id + "_" + k + ".json"
                json_file = {"key": k, "value": v}
                with open(os.path.join(result_dir, path), "w") as f:
                    json.dump(json_file, f)

store = Store()
files = ['file1', 'file2', 'file3', 'file4', 'file5', 'file6', 'file7', 'file8', 'file9', 'file10', 'file1_', 'file2_', 'file3_', 'file4_', 'file5_', 'file6_', 'file7_', 'file8_', 'file9_', 'file10_']
sizes = [100000, 100199, 109919, 119091, 99099, 100000, 100199, 190091, 110990, 99099, 43433, 43434, 112333, 32421, 32424, 55555, 66666, 77777, 22222, 111111]
output = {}
output_res = {}
for i in range(20):
    output_res[files[i]] = 'a' * sizes[i]
    if i & 1 == 1:
        output[files[i]] = {"type" : "DB"}
    else:
        output[files[i]] = {"type": "MEM"}

start_time = time.time()
#store.put('123', output, output_res)
res = store.fetch('123', output)
duration = time.time() - start_time
for v in res.values():
    print(len(v))
print("duration:", duration)