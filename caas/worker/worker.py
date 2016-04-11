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
import os
import sys
import json

import cv2
import numpy
import requests
from scipy.misc import imresize
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport

from commons.src.system.daemon import Daemon
from commons.src.logging.setup_logging import setup_logging
from commons.src.config import config_loader

from caas.caffe_worker import Worker 
from caas.caffe_worker.ttypes import BaseOutput, PredictionOutput

class WorkerHandler:
    def __init__(self, config_file_path, rundir_path):
        """
        Constructor for Worker Thrift Handler
        Args:
            config_file_path:

        Returns:

        """
        config = config_loader.get(config_file_path)
        self.rundir_path = rundir_path
        self.blob_dir = config["local-store"]["blob"]
        if not os.path.isabs(self.blob_dir):
            self.blob_dir = self.rundir_path + '/' + self.blob_dir

        self.prototxt_dir = config["local-store"]["prototxt"]
        if not os.path.isabs(self.prototxt_dir):
            self.prototxt_dir = self.rundir_path + '/' + self.prototxt_dir

        self.logger = logging.getLogger(__name__)
        self.logger.info("WorkerHandler is being initialized")

    def status(self):
        """
        Returns: Success

        """
        bo = BaseOutput()
        bo.status = "Success"
        return bo

    def __load_caffe_model(self, blob_path, prototxt_path):
        """
        Load caffe model to memory
        Args:
            blob_path: Model in HDF5 format
            prototxt_path: Prototxt file. Contains Network implementation

        Returns:

        """
        net = caffe.Net(prototxt_path, blob_path, caffe.TEST)
        input_layer = net.inputs[0]
        output_layer = net.outputs[0]
        #height = net.blobs["data_q"].data.shape[2]
        #width = net.blobs["data_q"].data.shape[3]
        height = net.blobs[input_layer].data.shape[2]
        width = net.blobs[input_layer].data.shape[3]
        self.logger.info("Model has been successfully loaded from Blob:" + blob_path + " , Prototxt:" + prototxt_path)
        return net, height, width

    def __caffe_predict(self, net, height, width, url):
        # logger = logging.getLogger(__name__)
        #
        # logger.info("caffe_predict has been called")

        input_layer = net.inputs[0]
        output_layer = net.outputs[0]
        r = requests.get(url, allow_redirects=False)
        arr = numpy.asarray(bytearray(r.content), dtype=numpy.uint8)
        img = cv2.imdecode(arr, -1)
        resized_img = imresize(img, (height,width), 'bilinear')
        transposed_resized_img = numpy.transpose(resized_img, (2,0,1))
        reqd_shape = (1,) + transposed_resized_img.shape
        #net.blobs["data_q"].reshape(*reqd_shape)
        #net.blobs["data_q"].data[...] = transposed_resized_img
        net.blobs[input_layer].reshape(*reqd_shape)
        net.blobs[input_layer].data[...] = transposed_resized_img
        net.forward()
        #result = net.blobs['latent_q_encode'].data[0].tolist()
        result = net.blobs[output_layer].data[0].tolist()
        return result

    def __get_file_from_s3(self, s3_key_name, local_path):
        """
        Fetches file from S3 to local machine
        Args:
            s3_key_name:
            local_path:

        Returns:

        """
        config = config_loader.get(sys.argv[2])
        s3_loader = S3Loader(config["s3"]["host"], config["s3"]["access_key"], config["s3"]["secret_key"])
        s3_loader.fetch_file(config["s3"]["bucket_name"], s3_key_name, local_path)

    def __fetch(self, fi):
        """

        Args:
            fi: FetchInput. It contains model_id and model paths

        Returns: Success or Failure

        """
        model_paths = json.loads(fi.modelPathsJson)
        self.logger.info("Fetch has been called with blob_key_name: " + model_paths["blob"] + ", modelId " + fi.modelId + ", proto: " + model_paths["prototxt"])
        bo = BaseOutput()
        try:
            self.__get_file_from_s3(model_paths["blob"], self.blob_dir + fi.modelId + "_blob")
            self.__get_file_from_s3(model_paths["prototxt"], self.prototxt_dir + fi.modelId + "_proto")
            bo.status = "Success"
        except Exception as e:
            self.logger.error('Fetch from S3 has failed.. ', exc_info=True)
            sys.stderr.write(str(e))
            bo.status = "Failure"
        return bo

    def commission(self, ci):
        """
        Commission a new model on this worker
        Args:
            ci: CommissionInput. It contains model_id and model paths

        Returns: Success or Failure

        """
        model_paths = json.loads(ci.modelPathsJson)
        self.logger.info("Commission has been called with modelId: " + ci.modelId + ", blob: " + model_paths["blob"] + ", proto: " + model_paths["prototxt"])

        fetch_from_s3 = config.get('s3', False)
        # if config defines s3 access and bucket details, will fetch from s3 into local store, else will look for models in local store. 
        if fetch_from_s3:
            # Fetches model and prototxt from S3 and stores at a location given in the config with _blob appended for model blob and _proto for prototxt
            self.__fetch(ci)
        bo = BaseOutput()
        try:
            self.net, self.height, self.width = self.__load_caffe_model(self.blob_dir + ci.modelId + "_blob", self.prototxt_dir + ci.modelId + "_proto")
            bo.status = "Success"
            self.logger.info("Commission has been successful.")
        except Exception as e:
            sys.stderr.write(str(e))
            self.logger.error("Commission has failed ", exc_info=True)
            bo.status = "Faliure"
        return bo

    def decommission(self):
        """
        Decommision the model from this worker
        Returns: Success or Failure

        """
        bo = BaseOutput()
        self.net = None
        self.height = None
        self.width = None
        self.logger.info("Decommission has been successful.")
        bo.status = "Success"
        return bo

    def predict(self, ip):
        """
        Caffe predict request handler
        Args:
            ip: Prediction input. It contains input image URL

        Returns:

        """
        po = PredictionOutput()
        bo = BaseOutput()
        po.bo = bo
        try:
            po.values = self.__caffe_predict(self.net, self.height, self.width, ip.url)
            bo.status = "Success"
        except Exception as e:
            self.logger.error("Error occurred while predicting. ", exc_info=True)
            bo.status = "Failure"
        return po

class CaffeWorker(Daemon):
    def run(self):
        """
        Starts thrift server in a Daemon
        Returns:

        """
        self.logger = logging.getLogger(__name__)
        config = config_loader.get(sys.argv[2])
        handler = WorkerHandler(sys.argv[2], self.rundir)
        proc = Worker.Processor(handler)
        trans_svr = TSocket.TServerSocket(port=int(sys.argv[3]) + int(config["port"]["start"]))
        trans_fac = TTransport.TFramedTransportFactory()
        proto_fac = TBinaryProtocol.TBinaryProtocolFactory()
        server = TServer.TThreadPoolServer(proc, trans_svr, trans_fac, proto_fac)
        self.logger.info("Starting CaffeWorker Daemon")
        server.serve()

if __name__ == "__main__":
    if len(sys.argv) == 4:
        setup_logging()
        logger = logging.getLogger(__name__)
        config = config_loader.get(sys.argv[2])
        sys.argv[2] = os.path.abspath(sys.argv[2])

        os.environ['GLOG_minloglevel'] = '3'
        sys.path.append(config["caffe"]["path"])
        import caffe
        caffe.set_mode_cpu()

        from commons.src.s3.s3_loader import S3Loader
        from commons.src.system.daemon import Daemon

        worker_name = "caffe-worker-" + sys.argv[3]
        daemon = CaffeWorker("/tmp/" + worker_name + ".pid",
                             stderr= worker_name + ".stderr")
        
        if 'start' == sys.argv[1]:
            logger.info("Caffe Worker has started. ")
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

