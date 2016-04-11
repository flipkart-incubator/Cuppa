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
import sys
import time
import os

class ElbResource:
    """
    ELB Healthcheck resource
    """
    def __init__(self, start_time, load_balancer):
        self.start_time = start_time
        self.load_balancer = load_balancer

    def on_get(self, req, resp):
        """
        Handles GET requests for ELB HealthCheck Resource
        :param req:
        :param resp:
        :return:
        """
        uptime = int(time.time()) - self.start_time

        if self.load_balancer.check_if_model_to_workers_map_is_empty():
            resp.status = falcon.HTTP_500
            resp.body = "Model To Workers Map is Empty"
            raise falcon.HTTPInternalServerError('Internal Server Error', 'Model To Workers Map is Empty! ')

        resp.status = falcon.HTTP_200

        # TODO requests and capacity have to be calculated. They are hardcoded for now

        resp.body = json.dumps({'uptime':uptime, 'requests': 1, 'capacity': 100})


