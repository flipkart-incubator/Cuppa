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

import falcon
import os
import sys

from commons.src.logging.setup_logging import setup_logging
import datetime
import time
import logging
import json

VECTOR = 'vector'
TAGS = 'tags'
DELETE = 'delete'
INSERT = 'insert'
OPERATION = 'operation'
MODEL_ID = 'modelId'
DATA_POINT_ID = 'dataPointId'

# TODO add code documentation for Class and methods
class Record:

    def __init__(self, knn_worker_client):
        self.knn_worker_client = knn_worker_client
        self.logger = logging.getLogger(__name__)
        self.logger.info("Record Inited...")

    def on_post(self, req, resp):
        """

        Args:
            req:
            resp:

        Returns: falcon http response

        """
        self.logger.info('[ONPOST][STARTED]')
        stream_read = req.stream.read()
        body = json.loads(stream_read)
        self.logger.info('Start on_post with body = %s and type = %s' % (body, type(body)))

        operation = body[OPERATION]

        if not operation == INSERT and not operation == DELETE:
            resp.status = falcon.HTTP_400
            resp.body = json.dumps({"TypeMismatch": "operation can only be insert or delete. Case sensitivity."})
            return

        model_id = body[MODEL_ID]
        data_point_id = body[DATA_POINT_ID]

        if not isinstance(model_id, str) and not isinstance(model_id, unicode):
            resp.status = falcon.HTTP_400
            resp.body = json.dumps({"TypeMismatch": "model id should be string"})
            return

        if not isinstance(data_point_id, str) and not isinstance(data_point_id, unicode):
            resp.status = falcon.HTTP_400
            resp.body = json.dumps({"TypeMismatch": "data point id should be string"})
            return

        if operation == INSERT:

            self.logger.info('[OPERATION][INSERT][STARTED]')
            self.logger.debug('Operation %s with model_id %s and data_point_id %s ' %  (operation, model_id, data_point_id))

            embedding = body[VECTOR]

            for _float in embedding:
                if not isinstance(_float, float):
                    resp.status = falcon.HTTP_400
                    resp.body = json.dumps({"TypeMismatch": "embedding has non float entry"})
                    return

            tags = body[TAGS]

            for _tag in tags:
                if not isinstance(_tag, str) and not isinstance(_tag, unicode):
                    resp.status = falcon.HTTP_400
                    resp.body = json.dumps({"TypeMismatch": "tags should be strings"})
                    return

            self.logger.debug('embd %s and tags %s ' % (embedding, tags))

            timestamp = time.mktime(datetime.datetime.now().timetuple())

            redis_status, redis_message = self.knn_worker_client._redis_insert(model_id, data_point_id, embedding, tags)

            self.logger.debug("[redis return] %s" % redis_status)

            if redis_status == False:
                resp.status = falcon.HTTP_500
                resp.body = json.dumps({"message": redis_message})
                return

            list_of_responses, worker_message = self.knn_worker_client._insert(model_id, data_point_id, embedding, tags)

            self.logger.debug('list of responses are %s' % list_of_responses)

            if list_of_responses == False:
                resp.status = falcon.HTTP_500
                resp.body = json.dumps({"message": worker_message})
                return

            self.logger.info('[OPERATION][INSERT][STOPPED]')

        elif operation == DELETE:

            self.logger.info('[OPERATION][DELETE][STARTED]')

            redis_status, redis_message = self.knn_worker_client._redis_delete(model_id, data_point_id)

            if redis_status == False:
                resp.status = falcon.HTTP_500
                resp.body = json.dumps({"message": redis_message})
                return

            self.logger.debug("[redis return] %s" % redis_status)

            list_of_responses, worker_message = self.knn_worker_client._remove(model_id, data_point_id)

            self.logger.debug('Operation %s with model_id %s and data_point_id %s ' % (operation, model_id, data_point_id))

            if list_of_responses == False:
                resp.status = falcon.HTTP_500
                resp.body = json.dumps({"message": worker_message})
                return

            self.logger.info('[OPERATION][INSERT][STOPPED]')

        for uo in list_of_responses:
            if uo.status == False:
                resp.status = falcon.HTTP_409
                resp.body = json.dumps({"status": uo.message})
                return

        resp.status = falcon.HTTP_200
        resp.body = json.dumps({"status": 'Success'})
        return
