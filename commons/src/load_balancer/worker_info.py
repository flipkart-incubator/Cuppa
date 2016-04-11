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

class WorkerInfo:
    """
    Contains Worker Thrift Service details and worker id
    """
    def __init__(self, host, port, local_worker_id, global_worker_id = None):
        """
        Constructor
        Args:
            host: worker host
            port: worker port number
            local_worker_id: Local worker id
            global_worker_id: Global worker id

        Returns:

        """
        self.host = host
        self.port = port
        self.local_worker_id = local_worker_id
        if global_worker_id is None:
            # Generate global worker id
            self.global_worker_id = host + "-" + str(local_worker_id)
