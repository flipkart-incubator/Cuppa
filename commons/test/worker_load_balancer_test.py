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

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
from commons.src.load_balancer.worker_load_balancer import WorkerInfo

from commons.src.config import config_loader

if __name__ == "__main__":
    load_balancer = WorkerLoadBalancer(sys.argv[1])
    worker = load_balancer.choose_worker('model-1')
    print worker.host
    print worker.port
    print worker.local_worker_id
    print worker.global_worker_id

    print load_balancer.get_all_workers()

    print load_balancer.get_worker_info('localhost-0').host
    print load_balancer.get_worker_info('localhost-0').port
    print load_balancer.get_worker_info('localhost-0').local_worker_id

    print load_balancer.get_workers('model-1')

