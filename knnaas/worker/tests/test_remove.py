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
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated

from knnaas.knnThrift import KnnThriftService
from knnaas.knnThrift.ttypes import *
from knnaas.knnThrift.constants import *

class SimpleRemoveTestCase(unittest.TestCase):

    def get_client(self, host, port):
        trans = TSocket.TSocket(host, port)
        trans = TTransport.TBufferedTransport(trans)
        proto = TBinaryProtocol.TBinaryProtocolAccelerated(trans)
        client = KnnThriftService.Client(proto)
        return client, trans

    def remove(self, model_id, data_point_id):
        client1, trans = self.get_client(host='localhost', port=9090)
        trans.open()
        resp1 = client1.remove(model_id, data_point_id)
        trans.close()
        return resp1


    def test_rem_machine(self):
        model_id = 'model-1'
        for i in range(9000):
            uo = self.remove(model_id, str(i))
            assert(uo.status == True)
            assert(uo.message == 'OK')

if __name__ == "__main__":
    unittest.main()

