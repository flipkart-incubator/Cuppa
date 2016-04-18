import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))

from knnaas.router.src.knn_worker_client import KnnWorkerClient
from knnaas.knnThrift.ttypes import *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated

model_to_worker_config_path = './model_to_worker_list.yaml'

kwc = KnnWorkerClient(model_to_worker_config_path)
model_id = 'model-1'
appender = 'kwc_'
tags = ['a', 'b', 'c']
add = 0.01
for i in range(100000):
	by = 1
        resp =  kwc._predict(model_id, None, tags, str(i), by)
	if isinstance(resp, KNNLocalResult):
		print resp
