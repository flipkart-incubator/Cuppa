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

import os
import sys
import time
import logging

from commons.src.config import config_loader
from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer

from caas.caffe_worker import Worker
from caas.caffe_worker.ttypes import BaseOutput, PredictionOutput, CommissionInput, PredictionInput

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def get_thrift_client(worker):
    """
    Returns a thrift client for the given worker
    :param self:
    :param worker:
    :return:
    """
    socket = TSocket.TSocket(worker.host, worker.port)
    trans = TTransport.TFramedTransport(socket)
    proto = TBinaryProtocol.TBinaryProtocol(trans)
    client = Worker.Client(proto)
    return client, trans

class CaffeWorkerClient:
    def __init__(self, load_balancer):
        """
        Constructor
        Args:
            worker_config_path: Worker configuration
            load_balancer: Load balancer at worker end

        Returns:

        """
        self.logger = logging.getLogger(__name__)
        self.load_balancer = load_balancer

        # self.load_balancer = WorkerLoadBalancer(model_to_worker_config_path)
        self.worker_to_caffe_thrift_client_map = {}

        worker_id_to_worker_map = self.load_balancer.get_all_workers('Caffe')

        # Build model to thrift client map
        if worker_id_to_worker_map:
            for worker_id, worker in worker_id_to_worker_map.iteritems():
                self.worker_to_caffe_thrift_client_map[worker_id] = get_thrift_client(worker)

    def predict(self, prediction_input):
        """
        Caffe prediction
        Args:
            prediction_input: PredictionInput

        Returns: PredictionOutput

        """
        self.logger.info("Caffe predict has been called..")
        worker = self.load_balancer.choose_worker('Caffe', prediction_input.model_id)

        self.logger.debug('Chosen worker_id: %s for predict ', str(worker.global_worker_id))

        if worker.global_worker_id in self.worker_to_caffe_thrift_client_map:
            client, trans = self.worker_to_caffe_thrift_client_map[worker.global_worker_id]
        else:
            client, trans = get_thrift_client(worker)
            self.worker_to_caffe_thrift_client_map[worker.global_worker_id] = client, trans

        po = None
        retry_count = 3
        trial_number = 0
        while trial_number < retry_count:
            trial_number += 1
            try:
                trans.open()
                po = client.predict(prediction_input)
            except Exception as e:
                po = PredictionOutput()
                po.bo.status = 'Failure'
                self.logger.error('Predict has failed.. ', exc_info=True)
            else:
                break
            finally:
                if trans.isOpen():
                    trans.close()

        return po


