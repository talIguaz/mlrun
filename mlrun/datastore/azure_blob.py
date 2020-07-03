# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import os
from azure.storage.blob import BlobServiceClient
from .base import DataStore, FileStats

# Azure blobs will be represented with the following URL: az://<container name>. The storage account is already
# pointed to by the connection string, so the user is not expected to specify it in any way.


class AzureBlobStore(DataStore):
    def __init__(self, parent, schema, name, endpoint=''):
        super().__init__(parent, name, schema, endpoint)

        con_string = self._secret('AZURE_STORAGE_CONNECTION_STRING') or os.getenv(
            'AZURE_STORAGE_CONNECTION_STRING'
        )
        if con_string:
            self.bsc = BlobServiceClient.from_connection_string(con_string)

    def upload(self, key, src_path):
        # Need to strip leading / from key
        blob_client = self.bsc.get_blob_client(container=self.endpoint, blob=key[1:])
        with open(src_path, 'rb') as data:
            blob_client.upload_blob(data)

    def get(self, key, size=None, offset=0):
        blob_client = self.bsc.get_blob_client(container=self.endpoint, blob=key[1:])
        return blob_client.download_blob(offset, size).readall()

    def put(self, key, data, append=False):
        blob_client = self.bsc.get_blob_client(container=self.endpoint, blob=key[1:])
        # Note that append=True is not supported. If the blob already exists, this call will fail
        blob_client.upload_blob(data)

    def stat(self, key):
        blob_client = self.bsc.get_blob_client(container=self.endpoint, blob=key[1:])
        props = blob_client.get_blob_properties()
        size = props.size
        modified = props.last_modified
        return FileStats(size, time.mktime(modified.timetuple()))

    def listdir(self, key):
        if key and not key.endswith('/'):
            key = key[1:] + '/'

        key_length = len(key)
        container_client = self.bsc.get_container_client(self.endpoint)
        blob_list = container_client.list_blobs(name_starts_with=key)
        return [blob.name[key_length:] for blob in blob_list]
