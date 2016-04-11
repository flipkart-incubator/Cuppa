#
# Copyright 2012-2016, the original author or authors.

# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance
# with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
import time
import json
import os

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

#Path hack so the functions here can be run from fabric
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from caas.caffe_worker import Worker
from caas.caffe_worker.ttypes import BaseOutput, PredictionOutput, CommissionInput, PredictionInput

def init(config_path, worker_id):
    with open(config_path) as f:
        import yaml
        config = yaml.load(f)
    trans = TSocket.TSocket(config["worker"]["host"], int(config["port"]["start"]) + int(worker_id))
    socket = TTransport.TFramedTransport(trans)
    proto = TBinaryProtocol.TBinaryProtocol(socket)
    client = Worker.Client(proto)
    return client, trans

def status(config_path, worker_id):
    client, trans = init(config_path, worker_id)
    trans.open()
    bo = client.status()
    print bo.status
    trans.close()

# def fetch(config_path, worker_id, model_id):
#     client, trans = init(config_path, worker_id)
#     trans.open()
#     fi = FetchInput()
#     # fi.prototxt = "cp-0001"
#     # fi.blob = "cm-0001"
#     fi.prototxt = model_id + "_proto"
#     fi.blob = model_id + "_blob"
#     bo = client.fetch(fi)
#     print bo.status
#     trans.close()

def commission(config_path, worker_id, model_id):
    client, trans = init(config_path, worker_id)
    trans.open()
    ci = CommissionInput()

    model_paths = {}
    model_paths["blob"] = model_id + "_blob"
    model_paths["prototxt"] = model_id + "_proto"
    ci.modelId = model_id
    ci.modelPathsJson = json.dumps(model_paths)
    bo = client.commission(ci)
    print bo.status
    trans.close()

def predict(config_path, worker_id):
    client, trans = init(config_path, worker_id)
    trans.open()
    pi = PredictionInput()
    pi.url = "http://localhost:8001/image/t-shirt/z/c/w/au0042-austin-wood-l-275x340-imaeebbnmprhjvch.jpeg"
    po = client.predict(pi)
    print po.values
    trans.close()

def benchmark_predict(config_path, worker_id):
    iters = 100
    client, trans = init(config_path, worker_id)
    trans.open()
    pi = PredictionInput()
    pi.url = "http://localhost:8001/image0001.jpeg"
    start = time.time()
    for x in range(0, iters - 1):
        po = client.predict(pi)
    end = time.time()

    avg_time = (end - start) / iters

    print "Average time in ms: " + str(avg_time)
    trans.close()
