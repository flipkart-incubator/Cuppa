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
import os
import sys

from commons.src.logging.setup_logging import setup_logging

import datetime
import time
import json
import random
import ast
from knn_worker_client import KnnWorkerClient
import falcon


VECTOR = 'vector'
TAGS = 'tags'
OPERATION = 'operation'
MODEL_ID = 'modelId'
DATA_POINT_ID = 'dataPointId'
VECTOR = 'vector'
SEARCH_BY_VECTOR = 'SearchByVector'
SEARCH_BY_ID = 'SearchById'

# TODO Add code documentation
class Predict:
    def __init__(self, knn_worker_client):
        self.knn_worker_client = knn_worker_client
        self.logger = logging.getLogger(__name__)
        self.logger.info("Predict Inited...")

    def on_post(self, req, resp):
        """

        Args:
            req:
            resp:

        Returns: falcon http response object

        """
        self.logger.info('[ON_POST][PREDICT][STARTED]')

        body = json.loads(req.stream.read())

        operation = body[OPERATION]

        if not operation == SEARCH_BY_VECTOR and not operation == SEARCH_BY_ID:
                resp.status = falcon.HTTP_400
                resp.body = json.dumps({"TypeMismatch": "operation can only be SearchByVector or SearchById. Case sensitivity."})
                return

        model_id = body[MODEL_ID]

        if not isinstance(model_id, str) and not isinstance(model_id, unicode):
            resp.status = falcon.HTTP_400
            resp.body = json.dumps({"TypeMismatch": "model id should be string"})
            return

        tags = body[TAGS]

        for _tag in tags:
            if not isinstance(_tag, str) and not isinstance(_tag, unicode):
                resp.status = falcon.HTTP_400
                resp.body = json.dumps({"TypeMismatch": "tags should be strings"})
                return

        responses = list()
        knn_local_result = None

        if operation == SEARCH_BY_VECTOR:

            self.logger.info('[SEARCH_BY_VECTOR_STARTED]')
            embedding = body[VECTOR]

            for _float in embedding:
                if not isinstance(_float, float):
                    resp.status = falcon.HTTP_400
                    resp.body = json.dumps({"TypeMismatch": "embedding has non float entry"})
                    return

            data_point_id = None
            search_by_id = 0

            self.logger.debug('calling knn local result with model_id %s embd %s tags %s data_point_id %s by %s' % (model_id, embedding, tags, data_point_id, search_by_id))
            knn_local_result, message  = self.knn_worker_client._predict(model_id, embedding, tags, data_point_id, search_by_id)

            if knn_local_result == False:
                resp.status = falcon.HTTP_500
                resp.body = json.dumps({"message": message})
                return

            self.logger.info('[SEARCH_BY_VECTOR_DONE]')

        if operation == SEARCH_BY_ID:
            self.logger.info('[SEARCH_BY_ID_STARTED]')
            data_point_id = body[DATA_POINT_ID]

            if not isinstance(data_point_id, str) and not isinstance(data_point_id, unicode):
                resp.status = falcon.HTTP_400
                resp.body = json.dumps({"TypeMismatch": "data point id should be string"})
                return

            embedding = None
            search_by_id  = 1

            self.logger.debug('calling knn local result with model_id %s embd %s tags %s data_point_id %s by %s' % (model_id, embedding, tags, data_point_id, search_by_id))

            knn_local_result, message = self.knn_worker_client._predict(model_id, embedding, tags, data_point_id, search_by_id)

            if knn_local_result == False:
                resp.status = falcon.HTTP_500
                resp.body = json.dumps({"message": message})
                return

            self.logger.info('[SEARCH_BY_ID_DONE]')
        # Use full names instead of shortcuts for variable names
        list_result_records = knn_local_result.values

        if len(list_result_records) == 0:
            self.logger.debug("[len(list_rr) == 0] [TRUE]")
            resp.status = falcon.status_codes.HTTP_500
            resp.body = json.dumps({"message": knn_local_result.message})
            self.logger.info('[ON_POST][PREDICT][DONE]')
            return

        self.logger.info("[len(list_rr) > 0]")

        for result_record in list_result_records:
            tpl = tuple()
            tpl = (result_record.id, result_record.distance)
            responses.append(tpl)
        resp.status = falcon.status_codes.HTTP_200
        resp.body = json.dumps({"result": responses})
        self.logger.info('[ON_POST][PREDICT][DONE]')

