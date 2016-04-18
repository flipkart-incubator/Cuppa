import sys
import os


from knnaas.router.src.knn_worker_client import KnnWorkerClient

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated

model_to_worker_config_path = '/vagrant_data_home/karan/ml-engines/knnaas/config/development/model_to_worker_list.yaml'
knn_model_resource_path = '/vagrant_data_home/karan/ml-engines/knnaas/config/development/knn_model_resource.yaml'

kwc = KnnWorkerClient(model_to_worker_config_path, knn_model_resource_path)


model_id = 'model-1'
appender = 'kwc_'
for i in range(15000, 17421):
    data_point_id = appender + str(i)
    print kwc._remove(model_id, data_point_id)
    print i

