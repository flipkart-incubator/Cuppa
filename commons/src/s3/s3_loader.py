#!/usr/bin/env python

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

import boto
import boto.s3.connection

class S3Loader:
    """
    A generic S3 loader
    """
    def __init__(self, s3_host, access_key, secret_key):
        self.s3_host = s3_host
        self.access_key = access_key
        self.secret_key = secret_key
        self.connection = boto.connect_s3(aws_access_key_id = access_key,
                                          aws_secret_access_key = secret_key,
                                          host = s3_host,
                                          port = 80,
                                          is_secure = False,
                                          calling_format = boto.s3.connection.OrdinaryCallingFormat())

    def fetch_file(self, bucket_name, s3_key_name, local_path):
        """
        Fetches the file from S3 and copies the contents to local_path
        Args:
            bucket_name: S3 bucket name
            s3_key_name: S3 key name
            local_path: The contents will be copied to this path

        Returns: None

        """

        bucket = self.connection.get_bucket(bucket_name)
        key = bucket.get_key(s3_key_name)
        key.get_contents_to_filename(local_path)

