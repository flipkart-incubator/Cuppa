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

import redis
import json
import sys
import os
import logging


class RedisModelDataPoints:
    def __init__(self, host, port, database, password):
        self.host = host
        self.port = port
        self.database = database
        self.password = password
        self.pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.database)
        self.connection = redis.StrictRedis(password=self.password, connection_pool=self.pool)
        self.logger = self.logger = logging.getLogger(__name__)
        self.logger.info("[REDIS Inited]")


    def store_model_data_point(self, name, key, value):
        try:
            self.logger.info("[storing in redis....] key = %s value = %s " % (key, value))
            exists = self.connection.hexists(name, key)
            if exists:
                return False, 'Redis: Data Point Already Exists'
            else:
                self.connection.hset(name, key, value)
                return True, 'OK'
        except Exception as ex:
            self.logger.error("[REDIS INSERT EXCEPTION] %s" % str(ex), exc_info=True)
            return False, ex.message

    def get_all_data(self, model_id):
        try:
            self.logger.info("[redis getting all data]")
            return self.connection.hgetall(model_id), 'OK'
        except Exception as e:
            self.logger.error("[get_all_data exception] %s" % str(e))
            return False, e.message

    def delete_data_point(self, name, key):
        try:
            self.logger.info("[redis deleting data]")
            self.logger.debug("[redis deleting key = %s]" % key)
            exists = self.connection.hexists(name, key)
            if not exists:
                return False, 'Redis: Data Point Does Not Exists'
            response = self.connection.hdel(name, key)
            return True, 'OK'
        except Exception as e:
            self.logger.error("[redis delete excep] %s" % str(e))
            return False, e.message

    def get_data_point_ids(self, patrn):
        try:
            return list(self.connection.keys(pattern = patrn))
        except Exception as e:
            print e.message
            return None
