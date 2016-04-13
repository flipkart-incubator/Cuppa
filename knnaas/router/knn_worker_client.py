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
import os

from commons.src.logging.setup_logging import setup_logging
import logging

from knnaas.knnThrift import KnnThriftService
from knnaas.knnThrift.ttypes import *
from knnaas.knnThrift.constants import *

from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
from commons.src.load_balancer.worker_load_balancer import WorkerInfo
from commons.src.config import config_loader
from commons.src.load_balancer.worker_info import WorkerInfo

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
import random


INSERT = 'insert'
DELETE = 'delete'
SearchById = 'SearchByID'
SearchByVector = 'SearchByVector'
THRIFT = 'thrift'
PATH = 'path'

class KnnWorkerClient:
    def __init__(self, load_balancer):
        setup_logging()
        self.logger = logging.getLogger(__name__)
        self.load_balancer = load_balancer
        self.logger.info("load balancer inited...")
#        self.worker_id_to_worker = self.load_balancer.get_all_workers('KNN')
#        self.model_to_worker_id_list = self.load_balancer.get_model_to_workers_list('KNN')
        self.worker_connections = {}
        self.logger.info("KnnWorkerClient Inited...")

    def make_connections(self, model_id,  worker):
        """

        Args:
            model_id:
            worker:

        Returns:

        """
        try:
            self.worker_connections[worker.global_worker_id] = [self.make_clients(worker.host, worker.port) for i in range(10)]
        except Thrift.TException as e:
            self.logger.error("[MAKE CONNECTION FAILED][HOST: %s]" % worker.global_worker_id, exc_info=True)
            self.logger.error("[TException %s]" % str(e), exc_info=True)
            self.logger.debug("Removing worker %s" % worker)
            self._remove_workers(model_id, [worker])
            raise e

    def make_clients(self, host, port):
        """

        Args:
            host:
            port:

        Returns: thrift client for the worker at the given host and port

        """
        try:
            trans = TSocket.TSocket(host, port)
            trans = TTransport.TBufferedTransport(trans)
            proto = TBinaryProtocol.TBinaryProtocolAccelerated(trans)
            client = KnnThriftService.Client(proto)
            trans.open()
            return client
        except Thrift.TException as e:
            raise e

    def _remove_workers(self, model_id, workers_to_remove):
        """

        Args:
            model_id:
            workers_to_remove:

        Returns:

        """
        self.logger.info("_removing workers......")
        self.load_balancer.remove_workers('KNN', model_id, workers_to_remove)
        for worker in workers_to_remove:
            if worker.global_worker_id in self.worker_connections.keys():
                self.worker_connections.pop(worker.global_worker_id)

    def _insert(self, model_id, data_point_id, embedding, tags):
        """

        Args:
            model_id:
            data_point_id:
            embedding:
            tags:

        Returns: Either UpdateOutput or False alongside a message, as a python tuple

        """
        self.logger.info('[KNN_MODEL_WORKER] [_INSERT] [STARTED]')
        self.logger.debug('model_id %s data_point_id %s embd %s tags %s' % (model_id, data_point_id, embedding, tags))
        uo = UpdateOutput()
        if model_id in self.load_balancer.get_model_to_workers_list('KNN').keys():
            worker_ids = self.load_balancer.get_model_to_workers_list('KNN')[model_id]
            self.logger.debug("[worker_ids retrived] %s" % worker_ids)
        else:
            self.logger.debug("[MODEL ID NOT IN MAP] %s" % model_id)
            return False, "MODEL ID NOT IN MAP"
        responses = list()
        workers_to_remove = []
        for worker_id in worker_ids:
            if worker_id in self.worker_connections.keys():
                client = random.choice(self.worker_connections[worker_id])
            else:
                worker = self.load_balancer.get_all_workers('KNN')[worker_id]
                try:
                    self.make_connections(model_id, worker)
                except Exception as e:
                    continue
                client = random.choice(self.worker_connections[worker_id])
                self.logger.debug("[self.worker_connections] %s", self.worker_connections)
            try:
                uo = client.insert(model_id, data_point_id, embedding, tags)
                responses.append(uo)
            except Thrift.TException as tx:
                self.logger.error("[KnnWorkerClient][_insert Worker Exception][Removing Worker] [worker: %s]" % worker_id ,exc_info=True)
                workers_to_remove.append(self.load_balancer.get_all_workers('KNN')[worker_id])
                continue

        if len(workers_to_remove) > 0 :
            self._remove_workers(model_id, workers_to_remove)

        if len(responses) == 0:
            self.logger.debug("[KnnWorkerClient][resp len zero][No worker Left][What has admin been upto?...]")
            return False, 'All workers are non responsive.'

        self.logger.info("[KNN_MODEL_WORKER] [_INSERT] [STOP]")
        return responses, 'OK'

    def _remove(self, model_id, data_point_id):
        """

        Args:
            model_id:
            data_point_id:

        Returns: Either UpdateOutput or False alongside a message, as a python tuple

        """
        self.logger.info('[KNN_MODEL_WORKER] [_REMOVE] [STARTED]')
        self.logger.debug('model_id %s data_point_id %s' % (model_id, data_point_id))
        uo = UpdateOutput()
        if model_id in self.load_balancer.get_model_to_workers_list('KNN').keys():
            worker_ids = self.load_balancer.get_model_to_workers_list('KNN')[model_id]
            self.logger.debug("[worker_ids] %s" % worker_ids)
        else:
            self.logger.debug("[MODEL ID NOT IN MAP] %s" % model_id)
            return False, "MODEL ID NOT IN MAP"
        responses = list()
        self.logger.debug('worker_ids collected for the given model_id %s' % worker_ids)
        workers_to_remove = []
        for worker_id in worker_ids:
            if worker_id in self.worker_connections.keys():
                client = random.choice(self.worker_connections[worker_id])
            else:
                worker = self.load_balancer.get_all_workers('KNN')[worker_id]
                try:
                    self.make_connections(model_id, worker)
                except Exception as e:
                    continue
                client = random.choice(self.worker_connections[worker_id])
                self.logger.debug('chose client %s for worker_id %s' % (client, worker_id))
            try:
                uo  = client.remove(model_id, data_point_id)
                responses.append(uo)
            except Thrift.TException as tx:
                self.logger.debug("[KnnWorkerClient][_remove Worker Exception][worker : %s]" % worker_id, exc_info=True)
                workers_to_remove.append(self.load_balancer.get_all_workers('KNN')[worker_id])
                continue
        self.logger.debug('resp after thrift remove %s' % responses)
        if len(workers_to_remove) > 0:
            self._remove_workers(model_id, workers_to_remove)

        if len(responses) == 0:
            self.logger.debug("[KnnWorkerClient][resp len zero][No worker Left][What has admin been upto?...]")
            return False, 'All workers are non responsive.'
        return responses, 'OK'

    def _predict(self, model_id, embd, tags, data_point_id, by):
        """

        Args:
            model_id:
            embd:
            tags:
            data_point_id:
            by:

        Returns: Either KNNLocalResult or False alongside a message, as a python tuple

        """
        self.logger.info('_predicting...')
        self.logger.debug('model_id %s data_point_id %s embd %s tags %s by %s' % (model_id, data_point_id, embd, tags, by))
        klr = KNNLocalResult()
        tries = 5
        while tries > 0:
            try:
                worker = self.load_balancer.choose_worker('KNN', model_id)
            except Exception as e:
                self.logger.error("[No worker for the given model] %s" % model_id, exc_info=True)
                return False, 'No worker for this model, update router map'
            self.logger.info('random worker %s' % worker.global_worker_id)
            try:
                if worker.global_worker_id in self.worker_connections:
                    klr = random.choice(self.worker_connections[worker.global_worker_id]).predict(model_id, embd, tags, data_point_id, by)
                else:
                    self.make_connections(model_id, worker)
                    klr = random.choice(self.worker_connections[worker.global_worker_id]).predict(model_id, embd, tags, data_point_id, by)
                if klr.values == None:
                    klr.values = []
                    return klr, 'Worker miscalculated, check worker logs'
                else:
                    return klr, 'OK'
            except Exception as e:
                self.logger.debug("[Predict Excepted] [worker: %s] %s" % (worker.global_worker_id, str(e)) , exc_info=True)
                tries -= 1
                self._remove_workers(model_id, [worker])
                self.logger.debug("[PREDICT EXCEPTION tries left %s]" % tries)
                continue
        self.logger.debug("[Workers Down]")
        return False, 'Workers Down'

    def _redis_insert(self, model_id, data_point_id, embd, tags):
        """

        Args:
            model_id:
            data_point_id:
            embd:
            tags:

        Returns: Either UpdateOutput or False alongside a message, as a python tuple

        """
        self.logger.info("[REDIS INSERT][STARTED]")
        uo = UpdateOutput()
        tries = 5
        while tries > 0:
            try:
                worker = self.load_balancer.choose_worker('KNN', model_id)
                self.logger.info("Trying redis insert on worker %s" % worker.global_worker_id)
            except Exception as e:
                self.logger.error("[No worker for the given model] %s" % model_id, exc_info=True)
                return False, 'No worker for this model, update router map'
            try:
                if worker.global_worker_id in self.worker_connections:
                    uo = random.choice(self.worker_connections[worker.global_worker_id]).redis_insert(model_id, data_point_id, embd, tags)
                    self.logger.info("[REDIS DELETE DONE]")
                    return uo, 'OK'
                else:
                    self.make_connections(model_id, worker)
                    uo = random.choice(self.worker_connections[worker.global_worker_id]).redis_insert(model_id, data_point_id, embd, tags)
                    return uo, 'OK'
            except Exception as e:
                tries -= 1
                self.logger.error("[REDIS INSERTION EXCEP] [worker: %s] %s" % (worker.global_worker_id, str(e)), exc_info=True)
                self.logger.debug("[REDIS INSERT tries left %s]" % tries)
                self._remove_workers(model_id, [worker])
                continue
        self.logger.debug("REDIS DOWN / Worker Cluster Down?")
        return False, 'REDIS DOWN or Worker Cluster Down ?'

    def _redis_delete(self, model_id, data_point_id):
        """
        Args:
            model_id:
            data_point_id:

        Returns: Either UpdateOutput or False alongside a message, as a python tuple

        """
        self.logger.info("[REDIS INSERT][STARTED]")
        uo = UpdateOutput()
        tries = 5
        while tries > 0:
            try:
                worker = self.load_balancer.choose_worker('KNN', model_id)
                self.logger.info("Trying redis delete on worker %s" % worker.global_worker_id)
            except Exception as e:
                self.logger.error("[No worker for the given model] %s" % model_id, exc_info=True)
                return False, 'No worker for this model, update router map'
            try:
                if worker.global_worker_id in self.worker_connections:
                    uo = random.choice(self.worker_connections[worker.global_worker_id]).redis_delete(model_id, data_point_id)
                    self.logger.info("[REDIS DELETE DONE]")
                    return uo, 'OK'
                else:
                    self.make_connections(model_id, worker)
                    uo = random.choice(self.worker_connections[worker.global_worker_id]).redis_delete(model_id, data_point_id)
                    return uo, 'OK'
            except Exception as e:
                tries -= 1
                self.logger.error("[REDIS DELETE EXCEP] [worker: %s] %s" % (worker.global_worker_id, str(e)), exc_info=True)
                self.logger.debug("REDIS DELETE tries left %s" % tries)
                self._remove_workers(model_id, [worker])
                continue
        self.logger.debug("REDIS DOWN / Worker Cluster Down?")
        return False,  'REDIS DOWN or Worker Cluster Down ?'

