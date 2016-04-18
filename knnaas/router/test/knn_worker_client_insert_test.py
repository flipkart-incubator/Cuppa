import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))

from knnaas.router.knn_worker_client import KnnWorkerClient

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated

model_to_worker_config_path = './model_to_worker_list.yaml'

kwc = KnnWorkerClient(model_to_worker_config_path)
dp_id = 1
model_id = 'model-1'
tags = ['a', 'b', 'c']
for i in range(1, 8):
    if i % 2 == 0:
        embd = [3*dp_id + 1, 3*dp_id + 2, 3*dp_id + 3, 4*dp_id+4]
	print embd
	print kwc._redis_insert(model_id, str(i), embd, tags)
	dp_id += 1
    else:
	embd = [3*dp_id + 1, 3*dp_id + 2, 3*dp_id + 3]
	print embd
	print kwc._redis_insert(model_id, str(i), embd, tags)
	dp_id += 1
