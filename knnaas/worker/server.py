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
import datetime
import util
import time
import os
import redis_wrapper

from commons.src.logging.setup_logging import setup_logging
from commons.src.config import config_loader
from knnaas.knnThrift import KnnThriftService
from knnaas.knnThrift.ttypes import *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from commons.src.system.daemon import Daemon
import logging
import update
sys.path.append(os.path.join(os.path.dirname(__file__), "./build"))
import KNNINDEX
import generatedata
import logging
misc_config = None

class KnnServiceHandler:
    def __init__(self):
        self.knn_index = KNNINDEX.KNNIndex()
        self.g_d_obj = generatedata.GenerateData()
        self.redis_client = redis_wrapper.RedisModelDataPoints(misc_config['redis']['host'], misc_config['redis']['port'], misc_config['redis']['database'], misc_config['redis']['password'])
        setup_logging()
        self.logger = logging.getLogger(__name__)
        self.logger.info("[Worker Inited]")

    def status(self):

        """
        Returns: Success
        """

        bo = BaseOutput()
        bo.status = "Success"
        return bo

    def insert(self, model_id, data_point_id, vector, tags):
        self.logger.info('[KnnServiceHandler] [INSERT] [STARTED]')
        self.logger.debug('[KnnServiceHandler] [insert] model_id %s data_point_id %s vector %s tags %s' % (model_id, data_point_id, vector, tags))
        uo = UpdateOutput()
        long_tags = self.g_d_obj.getTagIDs(tags)
        self.logger.debug('[KnnServiceHandler] [insert] Received long_tags %s' % long_tags)
        resp_from_set_ldpid = self.g_d_obj.set_ldpid(data_point_id)
        if not resp_from_set_ldpid:
            self.logger.debug('[KnnServiceHandler] [insert] Received [FALSE] resp_from_set_ldpid %s' % resp_from_set_ldpid)
            self.logger.debug('[KnnServiceHandler] [INSERT] [DONE]')
            uo.status = False
            uo.message = 'Data Point Already Exists'
            return uo
        self.logger.debug('[KnnServiceHandler] [insert] Received [True] resp_from_set_ldpid %s' % resp_from_set_ldpid)
        ldpid = self.g_d_obj.get_ldpid(data_point_id)
        self.logger.debug('[KnnServiceHandler] [insert] Recieved ldpid %s' % ldpid)
        response, message = update.insert(ldpid, vector, long_tags, self.knn_index)
        self.logger.debug('[KnnServiceHandler] [insert] Recieved resp from update.insert %s %s' % (response, message))
        self.logger.info('[KnnServiceHandler] [INSERT] [DONE]')
        uo.status = response
        uo.message = message
        return uo

    def redis_insert(self, model_id, data_point_id, vector, tags):
        self.logger.info('[KnnServiceHandler] [REDIS INSERT] [STARTED]')
        uo = UpdateOutput()
        timestamp = time.mktime(datetime.datetime.now().timetuple())
        try:
            response, message = util.redis_insert_data_point(model_id, data_point_id, vector, tags, timestamp, self.redis_client)
            self.logger.debug("[response] %s %s" % (response, message))
            uo.status = response
            uo.message = message
            self.logger.debug("ou" % uo)
            return uo
        except Exception as e:
            self.logger.error("[EXCEPTION IN REDIS][%s]" % str(e),exc_info=True )
            uo.status = False
            uo.message = e.message
            self.logger.debug("ou" % str(uo))
            return uo

    def redis_delete(self, model_id, data_point_id):
        self.logger.info("[KnnServiceHandler][REDIS DELETE] [STARTED]")
        uo = UpdateOutput()
        try:
            response, message = util.redis_delete_data_point(model_id, data_point_id, self.redis_client)
            self.logger.debug("[REDIS DELETE RESP] %s %s" % (response, message))
            uo.status = response
            uo.message = message
            return uo
        except Exception as e:
            self.logger.error("[REDIS DELETE EXP] %s" % str(e),exc_info=True)
            uo.status = False
            uo.message = e.message
            return uo

    def remove(self, model_id, data_point_id):
        self.logger.info('[KnnServiceHandler] [REMOVE] [STARTED]')
        self.logger.debug('[KnnServiceHandler] [remove] model_id %s data_point_id %s' % (model_id, data_point_id))
        uo = UpdateOutput()
        ldpid = self.g_d_obj.get_ldpid(data_point_id)
        if not ldpid:
            self.logger.debug('[KnnServiceHandler] [remove] ldpid does not exists %s' % ldpid)
            self.logger.info('[KnnServiceHandler] [REMOVE] [DONE]')
            uo.status = False
            uo.message = 'Data Point does not exists'
            return uo
        self.logger.debug('[KnnServiceHandler] [remove] Recieved ldpid %s' % ldpid)
        response, message = update.delete(ldpid, self.knn_index)
        self.logger.debug('[KnnServiceHandler] [remove] Recieved resp from update.remove %s %s' % (response, message))
        self.logger.debug('[KnnServiceHandler] [remove] Removing ldpid %s and corresponding dpid %s from local map...' % (ldpid, data_point_id))
        self.g_d_obj.rem_from_local_map(ldpid, data_point_id)
        self.logger.info('[KnnServiceHandler] [REMOVE] [DONE]')
        uo.status = response
        uo.message = message
        return uo

    def predict(self, model_id, fvec, tags, data_point_id, by):
        self.logger.info('[KnnServiceHandler] [PREDICT] [STARTED]')
        if by == 1:
            self.logger.debug('[KnnServiceHandler] [PREDICT_BY_ID] [STARTED]')
            self.logger.debug('[KnnServiceHandler] [predict] [BY ID] model_id %s fvec %s tags %s data_point_id %s by %s' % (model_id, fvec, tags, data_point_id, by))
            fvec = self.get_fv(data_point_id)
            self.logger.debug('[KnnServiceHandler] [predict] [BY ID] fvec return : %s' % fvec)
            if not fvec:
                self.logger.debug('[KnnServiceHandler] [predict] [BY ID] not of fvec returned  : %s' % fvec)
                logging.info('[KnnServiceHandler] [PREDICT] [BY ID] [DONE]')
                knn_local_result = KNNLocalResult()
                knn_local_result.values = []
                knn_local_result.message = "Data Point does not exists"
                return knn_local_result
            rl = list()
            knn_local_result = KNNLocalResult()
            fv = KNNINDEX.FloatVector()
            fv.extend(fvec)
            self.logger.debug('[KnnServiceHandler] [predict] [BY ID] fv extended %s' % list(fv))
            long_tags = self.g_d_obj.getTagIDs(tags)
            self.logger.debug('[KnnServiceHandler] [predict] [BY ID] long tags %s' % long_tags)
            tv = KNNINDEX.LongVector()
            tv.extend(long_tags)
            self.logger.debug('[KnnServiceHandler] [predict] [BY ID] tv extended %s' % tv)
            try:
                result_records = self.knn_index.calculate_distance_multithreaded(fv, tv, misc_config['num_threads'], misc_config['k'])
                self.logger.debug('[KnnServiceHandler] [predict] [BY ID] result record returned by cDM %s' % result_records)
            except Exception as e:
                self.logger.error('[distance calculation exception] [by id] %s %s' % (data_point_id, e.message))
                knn_local_result.values = []
                knn_local_result.message = e.message
                return knn_local_result
            for rr in result_records:
                result_record = ResultRecord()
                dpid = self.g_d_obj.get_dpid(rr.key)
                result_record.id = dpid
                result_record.distance = rr.distance
                rl.append(result_record)
            knn_local_result.values = rl
            knn_local_result.message = 'OK'
        elif by == 0:
            self.logger.info('[KnnServiceHandler] [PREDICT_BY_VECTOR] [STARTED]')
            self.logger.debug('[KnnServiceHandler] [predict] [BY VECTOR] model_id %s fvec %s tags %s data_point_id %s by %s' % (model_id, fvec, tags, data_point_id, by))
            rl = list()
            knn_local_result = KNNLocalResult()
            fv = KNNINDEX.FloatVector()
            fv.extend(fvec)
            self.logger.debug('[KnnServiceHandler] [predict] [BY VECTOR] fv extended %s' % fv)
            long_tags = self.g_d_obj.getTagIDs(tags)
            self.logger.debug('[KnnServiceHandler] [predict] [BY ID] long tags %s' % long_tags)
            tv = KNNINDEX.LongVector()
            tv.extend(long_tags)
            self.logger.debug('[KnnServiceHandler] [predict] [BY VECTOR] tv extended %s' % tv)
            logging.debug('[KnnServiceHandler] [predict] [BY VECTOR] self knn_index %s' % self.knn_index)
            try:
                result_records = self.knn_index.calculate_distance_multithreaded(fv, tv, misc_config['num_threads'], misc_config['k'])
            except Exception as e:
                self.logger.error("[Exception in dist cal by vector] %s %s" % (fvec, e.message))
                knn_local_result.values = []
                knn_local_result.message = e.message
                return knn_local_message
            for rr in result_records:
                result_record = ResultRecord()
                dpid = self.g_d_obj.get_dpid(rr.key)
                self.logger.debug('[KnnServiceHandler] [predict] [BY VECTOR] got dpid %s' % dpid)
                result_record.distance = rr.distance
                result_record.id = dpid
                rl.append(result_record)
            knn_local_result.values = rl
            knn_local_result.message = 'OK'
        self.logger.info('[KnnServiceHandler] [PREDICT] [BY ID] [DONE]')
        return knn_local_result


    def get_fv(self, data_point_id):
        self.logger.info('[KnnServiceHandler] [GET_FV] [STARTED]')
        ldpid = self.g_d_obj.get_ldpid(data_point_id)
        self.logger.debug('[KnnServiceHandler] [get_fv] in get_fv ldpid returned is %s' % ldpid)
        if not ldpid:
            self.logger.debug('[KnnServiceHandler] [get_fv] [LDPID NOT IN idMappingDict]')
            self.logger.info('[KnnServiceHandler] [GET_FV] [DONE]')
            return None
        try:
            fv = self.knn_index.get_feature_vektor(ldpid)
            self.logger.debug('[KnnServiceHandler] [get_fv] fv returned is %s' % fv)
            self.logger.info('[KnnServiceHandler] [GET_FV] [DONE]')
            return fv
        except Exception as e:
            self.logger.error('[KnnServiceHandler] [get_fv] [Exception occured] %s' % e.message, exc_info=True)
            self.logger.debug('[KnnServiceHandler] [GET_FV] [DONE]')
            return None

    def commission(self, ci):
        self.logger.info('[COMMISSION][STARTED]')
        model_id = ci.modelId
        booleans = list()
        bo = BaseOutput()
        try:
            data, message = util.redis_get_all_data_points(model_id, self.redis_client)
            if data == False:
                bo.status = 'Failure'
                bo.message = message
                return bo
            self.logger.debug("[len data : %s]" % len(data))
            for data_point_id, datum in data.iteritems():
                v = datum['vector']
                t = datum['tags']
                uo = self.insert(model_id, data_point_id, v, t)
                self.logger.debug("[resp: %s] [model_id: %s] [dpid: %s]" % (uo.status, model_id, data_point_id))
                if not uo.status:
                    self.logger.debug("[COMMISSION] [FAILED FOR %s]" % data_point_id)
                booleans.append(uo.status)
        except Exception as e:
            bo.status = 'Failure'
            bo.message = str(e)
            self.logger.debug('[COMMISSION] [EXCEPTION] %s' % str(e), exc_info=True)
            return bo
        ground = True
        for boolean in booleans:
            self.logger.debug
            ground &= boolean
        if ground:
            bo.status = 'Success'
            bo.message = ' '
        else:
            bo.status = 'Failure'
            bo.message = 'Logged the datapoints for which insertions returned  false.'
        return bo

    def decommission(self):
        self.logger.info('[DECOMMISSION][STARTED]')
        try:
            bo = BaseOutput()
            self.g_d_obj.idMappingDict.clear()
            self.g_d_obj.tagToIDs.clear()
            self.g_d_obj.currentID = 0
            self.g_d_obj.currentTagID = 0
            self.knn_index.destruct()
            bo.status = 'Success'
            bo.message = ' '
            return bo
        except Exception as e:
            bo.status = 'Failure'
            bo.message = str(e)
            self.logger.error('[DECOMMISSION] [EXCEPTION] %s' % str(e), exc_info=True)
            return bo




"""
handler = KnnServiceHandler()
processor = KnnThriftService.Processor(handler)
transport = TSocket.TServerSocket(port=9090)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()
server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
print "Starting python server..."
server.serve()
"""
class ServerDeamon(Daemon):
     def run(self):
         handler = KnnServiceHandler()
         processor = KnnThriftService.Processor(handler)
         transport = TSocket.TServerSocket(port=misc_config['port'])
         tfactory = TTransport.TBufferedTransportFactory()
         pfactory = TBinaryProtocol.TBinaryProtocolFactory()
         server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
         print "Starting python server..."
         server.serve()

if __name__ == "__main__":
    if len(sys.argv) == 4:
        misc_config = config_loader.get(sys.argv[2])
        worker_name = "knn-worker-" + sys.argv[3]
        pid_file = '/tmp/' + worker_name + '.pid'
        daemon = ServerDeamon(pid_file, stdout="/dev/null", stderr=worker_name + ".stderr")
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        elif 'status' == sys.argv[1]:
            status = daemon.status()
            print(status)
            sys.exit(0)
        else:
            print "Unknown command"
            sys.exit(2)
            sys.exit(0)
    else:
        print "usage: %s start|stop|restart|status worker-config-file worker-id" % sys.argv[0]
        sys.exit(2)
