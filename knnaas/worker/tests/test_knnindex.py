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

import logging
import unittest
import sys
import os
import time
import cPickle as pickle

sys.path.append(os.path.join(os.path.dirname(__file__), "../build"))
import KNNINDEX
from generatedata import GenerateData

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def timer(func):
    def wrapper(*arg):
        t1 = time.time()
        x = func(*arg)
        t2 = time.time()
        print "%s took %0.3f ms" % (func.func_name, (t2 - t1) * 1000.0)
        return x

    return wrapper


class TestKNNIndex(unittest.TestCase):
    # def test_createGoldenData(self):
    def createGoldenData(self):
        generateData = GenerateData()
        data = generateData.generateData(20000)
        with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               os.environ['KNN_HOME'] + '/tests/golden_data/all_data.dmp.gold')),
                  mode='wb') as f:
            pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
            f.flush()
        generateData.printData(1)

    @timer
    def squared_distance_list(self, fv, tv, tc, tn):
        return self.knnIndex.calculateDistanceMultiThreaded(fv, tv, tc, tn)

    def setUp(self):
        print "inside setUp"
        with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               os.environ['KNN_HOME'] + '/tests/golden_data/all_data.dmp.gold')),
                  'r') as f:
            self.data = pickle.load(f)
        self.knnIndex = KNNINDEX.KNNIndex()
        self.index = KNNINDEX.Index()
        for id in self.data.keys():
            tagVector = KNNINDEX.LongVector()
            tagVector.extend(self.data[id][0])
            tagVector.append(99999)  # add dummy integer
            featureVector = KNNINDEX.FloatVector()
            featureVector.extend(self.data[id][1])
            record = KNNINDEX.Record()
            record.setKey(id)
            record.setFeatureVector(featureVector)
            record.setTagVector(tagVector)
            self.knnIndex.insert(record)
            self.index.append(record)
        # print dir(knnIndex)
        self.threadCount = 15
        self.topN = 1000

    def test_euclidian_distance(self):
        print "inside test_euclidian_distance"
        tagVector = KNNINDEX.LongVector()
        tagVector.append(99999)
        results = {}
        for record in self.index:
            x = self.squared_distance_list(record.getFeatureVector(), tagVector, self.threadCount, self.topN);
            results[record.getKey()] = [(i.key, i.distance) for i in x]
        with open(os.path.abspath(
                os.path.join(os.path.dirname(__file__), os.environ['KNN_HOME'] + '/tests/golden_data/results.dmp')),
                  mode='wb') as f:
            pickle.dump(results, f, pickle.HIGHEST_PROTOCOL)
            f.flush()
        with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               os.environ['KNN_HOME'] + '/tests/golden_data/results.dmp.gold')),
                  'r') as f:
            goldenResults = pickle.load(f)
        # results[9][0] = (1000, .09)
        for key in goldenResults.keys():
            self.assertEqual(goldenResults[key], results[key])


if __name__ == "__main__":
    unittest.main()
