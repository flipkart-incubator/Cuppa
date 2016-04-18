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

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated

from knnaas.knnThrift import KnnThriftService
from knnaas.knnThrift.ttypes import *
from knnaas.knnThrift.constants import *

def get_client(host, port):
    trans = TSocket.TSocket(host, port)
    trans = TTransport.TBufferedTransport(trans)
    proto = TBinaryProtocol.TBinaryProtocolAccelerated(trans)
    client = KnnThriftService.Client(proto)
    return client, trans


def _status():
    client1, trans = get_client(host='localhost', port=9090)
    trans.open()
    resp1 = client1.status()
    trans.close()
    assert(resp1.status == 'Success')

_status()


