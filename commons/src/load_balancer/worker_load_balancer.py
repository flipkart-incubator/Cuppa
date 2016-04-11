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

import logging
import random

from commons.src.config import config_loader
from commons.src.load_balancer.worker_info import WorkerInfo
from random import randint

class WorkerLoadBalancer:
    """
    A generic worker load balancer which helps in choosing a worker randomly.
    """

    def __init__(self):
        """
        Constructor
        Returns: None

        """

        #: dict model type to model to worker list map.
        self.model_type_to_model_to_worker_list = {}

        #: dict model type to worker id to worker info map
        self.model_type_to_worker_id_to_worker = {}

        #: dict Model type to model to workers map
        self.model_type_to_model_to_workers_map = {}

        # Logger instance
        self.logger = logging.getLogger(__name__)

    def update_workers_list(self, model_type_to_model_to_workers_map):
        """
        Updates model type to model to workers list map
        :param model_type_to_model_to_workers_map:
        :return:
        """
        if model_type_to_model_to_workers_map:
            model_type_to_model_to_worker_list = {}
            model_type_to_worker_id_to_worker = {}
            for model_type in model_type_to_model_to_workers_map.keys():
                model_to_worker_list = {}
                worker_id_to_worker = {}
                for model_id in model_type_to_model_to_workers_map[model_type].keys():
                    workers = []
                    worker_ids = []

                    for worker in model_type_to_model_to_workers_map[model_type][model_id]:
                        worker_info = WorkerInfo(worker["host"], worker["port"], worker["local_worker_id"])
                        workers.append(worker_info)
                        worker_ids.append(worker_info.global_worker_id)
                        worker_id_to_worker[worker_info.global_worker_id] = worker_info

                    if model_id in model_to_worker_list.keys():
                        model_to_worker_list[model_id] = list(set(model_to_worker_list[model_id])
                                                            | set(worker_ids))
                    else:
                        model_to_worker_list[model_id] = worker_ids

                model_type_to_model_to_worker_list[model_type] = model_to_worker_list
                model_type_to_worker_id_to_worker[model_type] = worker_id_to_worker

            self.model_type_to_model_to_worker_list = model_type_to_model_to_worker_list
            self.model_type_to_worker_id_to_worker = model_type_to_worker_id_to_worker

    def remove_workers(self, model_type, model_id, workers):
        """
         Used to remove workers to a model
         :param model_type
         :param model_id:
         :param workers:
         :return: None
         """
        for worker in workers:
            self.model_type_to_worker_id_to_worker[model_type].pop(worker.global_worker_id, None)

        if model_id in self.model_type_to_model_to_worker_list[model_type].keys():
            for worker in workers:
                if worker.global_worker_id in self.model_type_to_model_to_worker_list[model_type][model_id]:
                    self.model_type_to_model_to_worker_list[model_type][model_id].remove(worker.global_worker_id)
                    if not self.model_type_to_model_to_worker_list[model_type][model_id]:
                        self.model_type_to_model_to_worker_list[model_type].pop(model_id, None)

    def choose_worker(self, model_type, model_id):
        """
        Picks a worker(random) from available workers

        :param model_type
        :param model_id:
        :return: a randomly chosen worker
        """
        if self.model_type_to_model_to_worker_list.get(model_type):
            if model_id in self.model_type_to_model_to_worker_list[model_type].keys():
                worker_id_list = self.model_type_to_model_to_worker_list[model_type][model_id]
                worker_id = random.choice(worker_id_list)
                return self.model_type_to_worker_id_to_worker[model_type][worker_id]
            else:
                raise Exception("No worker available for the given model! ")
        else:
            raise Exception("No worker available for the given model type! ")

    def get_all_workers(self, model_type):
        """
        Returns all the workers available for the given model type
        Args:
            model_type: Type of the model

        Returns: all the workers available for the given model type

        """
        return self.model_type_to_worker_id_to_worker.get(model_type)

    def get_model_to_workers_list(self, model_type):
        """
        Returns the list of workers for a given model_id
        Args:
            model_type: Type of the model
        Returns: List of workers for the given model type

        """
        return self.model_type_to_model_to_worker_list.get(model_type)


    def get_worker_info(self, worker_id, model_type):
        """
        Returns worker information of a given worker
        Args:
            worker_id: Global worker id
            model_type: Type of the model

        Returns: Worker information of the given worker

        """
        if self.model_type_to_worker_id_to_worker.get(model_type):
            return self.model_type_to_worker_id_to_worker.get(model_type).get(worker_id)
        else:
            return None

    def check_if_model_to_workers_map_is_empty(self):
        """
        Checks if model to workers map is empty
        Returns: False if model to workers map is empty else True

        """
        return False if self.model_type_to_model_to_worker_list else True

