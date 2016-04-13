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
from threading import Lock
import csv
import time
import cProfile
import os
import array
import random
import logging

class GenerateData():
    def __init__(self):
        self.data = {}
        self.idMappingDict = {}
        self.tagToIDs = {}
        self.currentID = 0
        self.currentTagID = 0
        self.logger = logging.getLogger(__name__)

    def getNextID(self):
        """

        Returns: increments and return a long type Id for a new string type data point id.

        """
        self.currentID += 1
        return self.currentID

    def getNextTagID(self):
        """

        Returns: increments and return a long type Id for a new string type tag.

        """
        self.currentTagID += 1
        return self.currentTagID

    def getTagIDs(self, l):
        """

        Args:
            l: list of tags

        Returns: a long type id for a new string tag

        """
        lock = Lock()
        ret = []
        for item in l:
            tagID = self.tagToIDs.get(item)
            if tagID is None:
                lock.acquire()
                tagID = self.tagToIDs.get(item)
                if tagID is None:
                    tagID = self.getNextTagID()
                    self.tagToIDs[item] = tagID
                lock.release()
            ret.append(self.tagToIDs[item])
        return ret

    def getData(self):
        return self.data

    def set_ldpid(self, data_point_id):
        """

        Args:
            data_point_id:

        Returns: True/False, depending on whether the data_point_id has already been set or not.

        """
        self.logger.info('[GENERDATE DATA] [SET_LDPID] [STARTED]')
        if data_point_id in self.idMappingDict:
            self.logger.info('[GENERDATE DATA] [SET_LDPID] [DONE]')
            return False
        cr_id = self.getNextID()
        self.idMappingDict[data_point_id] = cr_id
        self.idMappingDict[cr_id] = data_point_id
        self.logger.info('[GENERDATE DATA] [SET_LDPID] [DONE]')
        return True

    def get_ldpid(self, data_point_id):
        """

        Args:
            data_point_id:

        Returns: long type id for a data_point_id

        """
        self.logger.info('[GENERDATE DATA] [GET_LONG_DPID] [STARTED]')
        if data_point_id in self.idMappingDict:
            resp = long(self.idMappingDict[data_point_id])
            return resp
        else:
            return None

    def get_dpid(self, ldpid):
        """

        Args:
            ldpid:

        Returns: 

        """
        self.logger.info('[GENERDATE DATA] [GET_STRING_DPID] [STARTED]')
        if ldpid in self.idMappingDict:
            resp = self.idMappingDict[ldpid]
            return resp
        else:
            return None

    def rem_from_local_map(self, ldpid, data_point_id):
        self.logger.info('[GENERDATE DATA] [REMOVE_FROM_LOCAL_MAP] [STARTED]')
        if ldpid in self.idMappingDict:
            self.idMappingDict.pop(ldpid, None)
        if data_point_id in self.idMappingDict:
            self.idMappingDict.pop(data_point_id, None)
        self.logger.info('[GENERDATE DATA] [REMOVE_FROM_LOCAL_MAP] [DONE]')

    def getIDMapping(self):
        return self.idMappingDict

    def generateData(self, numrows):
        maxRows = numrows
        currentRow = 0
        rd = csv.reader(open(os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/output.csv')), mode='r'),
                        delimiter="\t")
        embeddings = csv.reader(
            open(os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/embeddings.csv')), mode='r'),
            delimiter=",")
        embedding_data = []
        for embedding in embeddings:
            embedding_data.append([float(x) for x in embedding[1:]])
            if currentRow == maxRows:
                break
            currentRow += 1
        currentRow = 0
        embeddingDatacounter = 0
        totalEmbeddings = len(embedding_data)
        for row in rd:
            id = self.getNextID()
            try:
                self.idMappingDict[id] = row[0]
                self.data[id] = []
                self.data[id].append(self.getTagIDs([row[5], row[7], row[16], row[48], row[83], row[86], row[93]]))
                self.data[id].append(embedding_data[embeddingDatacounter])
                # self.data[id] = [row[5], row[7], row[16], row[48], row[83], row[86], row[93], embedding_data[counter]]
                embeddingDatacounter += 1
                if (embeddingDatacounter == totalEmbeddings - 1):
                    embeddingDatacounter = 0
                # print self.data[row[0]]
                if currentRow == maxRows:
                    break;
                currentRow += 1
            except Exception as e:
                print "unable to process " + row[0] + "  " + str(id) + " " + str(e)
        return self.data

    def printData(self, rows):
        for key in self.data.keys()[:rows]:
            print key, self.data[key]

"""
if __name__ == "__main__":
    generateData = GenerateData()
    startTime = time.time()
    cProfile.run('generateData.generateData(10)')
    # generateData.generateData(10)
   generateData.printData(10)
"""
