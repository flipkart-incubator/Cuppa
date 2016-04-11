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

import os
import sys
import unittest
from unittest import TestCase


class S3LoaderTest(TestCase):
    def test_fetch(self):
        pass

if __name__ == '__main__':
    sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
    unittest.main()
