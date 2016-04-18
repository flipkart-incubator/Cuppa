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
import unittest

from commons.src.config import config_loader
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated

from knnaas.knnThrift import KnnThriftService
from knnaas.knnThrift.ttypes import *
from knnaas.knnThrift.constants import *
worker_misc_config_path = "conf/knnaas_config.yaml"
misc_config = config_loader.get(worker_misc_config_path)


class SimplePredictTestCase(unittest.TestCase):

    def get_client(self, host, port):
        trans = TSocket.TSocket(host, port)
        trans = TTransport.TBufferedTransport(trans)
        proto = TBinaryProtocol.TBinaryProtocolAccelerated(trans)
        client = KnnThriftService.Client(proto)
        return client, trans

    def _predict(self, model_id, embd, tags, data_point_id, by):
        client, trans = self.get_client(host='localhost', port=9090)
        trans.open()
        resp = client.predict(model_id, embd, tags, data_point_id, by)
        trans.close()
        return resp

    def test_predict_machine(self):
        model_id = 'model-1'
        embd = [2.11, 3.122, 4.212, 2.113]
        tags = ["fdp-ml-test-tags"]
        data_point_id = 1
        for i in range(200):
            if i % 2 == 0:
                by = 1
                dpid = str(i)
                po = self._predict(model_id, None, tags, dpid, by)
                assert(len(po.values) == misc_config['k'])
                assert(po.message == 'OK')
            else:
                by = 0
                embd = [i**2, i**2-i, 14*i]
                po = self._predict(model_id, embd, tags, None, by)
                assert(len(po.values) == misc_config['k'])
                assert(po.message == 'OK')

if __name__ == "__main__":
    unittest.main()
