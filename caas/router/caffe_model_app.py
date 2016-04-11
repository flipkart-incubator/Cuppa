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
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))

from commons.src.config import config_loader

from caffe_model_resource import CaffeModelResource

from commons.src.elb.elb_resource import ElbResource

def loader(model_to_worker_config_path, worker_config_path):
    caffe_model_reource = CaffeModelResource(model_to_worker_config_path, worker_config_path)

    start_time = int(time.time())

    elb_resource = ElbResource(start_time)
    # falcon.API instances are callable WSGI apps
    app = falcon.API()
    # caffe_model will handle all requests to the '/caffe_model' URL path
    app.add_route('/caffe_model/predict', caffe_model_reource)
    app.add_route('/elb-healthcheck', elb_resource)
    return app

