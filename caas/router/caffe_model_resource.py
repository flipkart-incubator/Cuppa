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
import json
from commons.src.config import config_loader
import sys
import logging

from caas.caffe_worker import Worker
from caas.caffe_worker.ttypes import BaseOutput, PredictionOutput, CommissionInput, PredictionInput

from caffe_worker_client import CaffeWorkerClient

class CaffeModelResource(object):
    def __init__(self, load_balancer):
        self.caffe_worker_client = CaffeWorkerClient(load_balancer)
        self.load_balancer = load_balancer
        self.logger = logging.getLogger(__name__)


    def on_get(self, req, resp):
        """Handles GET requests"""
        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = ('\nFalcon is awesome! \n')

    def on_post(self, req, resp):
        payload = json.loads(req.stream.read())
        pi = PredictionInput()
        if ('url' in payload.keys()) and ('modelId' in payload.keys()):
            pi.url = payload['url']
            pi.model_id = payload['modelId']
        else:
            resp.status = falcon.HTTP_400
            raise falcon.HTTPBadRequest("Bad Request", "Url and(or) modelId missing in the payload")

        po = self.caffe_worker_client.predict(pi)

        if po.bo.status == 'Success':
            resp.status = falcon.HTTP_200
            resp.body = (str(po.values))
        elif po.bo.status == 'Failure':
            resp.body = json.dumps({'status': 'Failure', 'message' : 'Error occurred'})
            resp.status = falcon.HTTP_500
            raise falcon.HTTPInternalServerError('Internal Server Error', 'Predict failed! ')
