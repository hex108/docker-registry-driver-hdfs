# -*- coding: utf-8 -*-
# Copyright (c) 2014 Insight Centre.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
docker_registry.drivers.hdfs
~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a hadoop based hdfs driver.  It would be better to do this
with snakebite (https://github.com/spotify/snakebite) but it doesn't
yet support writing to hdfs.

"""

import logging
import os
import hadoopy
import pika
import shutil
import time
from hadoopy._hdfs import _checked_hadoop_fs_command
from snakebite.client import Client

from ..core import driver
from ..core import exceptions
from ..core import lru

# Hadoopy fs functions are... deficient.  Yes this is gross.  I'm sorry.

logger = logging.getLogger(__name__)

def hdfs_mkdirp(path):
    cmd = "hadoop fs -mkdir -p %s" % (path)
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)


def hdfs_du(path):
    cmd = "hadoop fs -du -s %s" % (path)
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)
    if stdout:
        return stdout.split(' ')[0]


def hdfs_rmr(path):
    cmd = "hadoop fs -rm -r %s" % (path)
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)


def hdfs_putf(local_path, hdfs_path):
    cmd = "hadoop fs -put -f %s %s" % (local_path, hdfs_path)
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)


class Storage(driver.Base):

    #supports_bytes_range = True

    def __init__(self, path=None, config=None):
        self._root_path = path or '/registry'
        self._local_path = config.local_path or './local_registry'
        self._hdfs_nn_host = config.hdfs_nn_host
        self._hdfs_nn_port = config.hdfs_nn_port
        self._need_sync = config.need_sync
        # If it is master, it needs to sync the data to slaves
        self._is_master = config.is_master
        if self._need_sync:
            if self._is_master:
                logger.info("Master storage node.")
                self._mq_queue = config.mq_queue
                self._mq_host = config.mq_host
                self._init_mq(self._mq_host, self._mq_queue)
            else:
                logger.info("Slave storage node.")
        else:
            logger.info("Not in sync mode.")
        logger.info("HDFS namenode info: %s:%s" % (self._hdfs_nn_host, self._hdfs_nn_port))

    def _init_path(self, path=None):
        if path:
            hdfs_path = os.path.join(self._root_path, path)
        else:
            hdfs_path = self._root_path
        local_path = os.path.join(self._local_path, path)
        return local_path, hdfs_path

    def _init_mq(self, mq_host, mq_queue):
        connection = pika.BlockingConnection(pika.ConnectionParameters(mq_host))
        channel = connection.channel()
        channel.queue_declare(queue=mq_queue, durable=True)
        self._channel = channel

    def _create_local(self, local_path):
        dirname = os.path.dirname(local_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

    def _delete_local_file(self, local_file):
        if not os.path.exists(local_file):
            logger.warn("Try to delete local file %s does not exsit" % local_path)
        else:
            os.remove(local_file)

    def _create_hdfs(self, hdfs_path):
        dirname = os.path.dirname(hdfs_path)
        if not hadoopy.exists(dirname):
            hdfs_mkdirp(dirname)

    def _send_msg(self, msg):
        logger.info(msg)
        while True:
            try:
                self._channel.basic_publish(exchange='',
                                            routing_key=self._mq_queue,
                                            body=msg,
                                            properties=pika.BasicProperties(
                                                delivery_mode=2. # make message persistent
                                            ))
                return
            except Exception, e:
                logger.error(e)
                self._init_mq(self._mq_host, self._mq_queue)
                time.sleep(10)

    def _sync_with_slaves(self, path, operation):
        if not self._need_sync:
            return
        else:
            if self._is_master:
                logger.info("Sync with slaves : %s %s" % (operation, path))
            else:
                logger.error("MUST NOT %s %s on slaves" % (operation, path))
                return
        if path.split('/')[-1] != "_inprogress":
            if path[0] != '/':
                path = self._init_path(path)[1]
            self._send_msg(operation + " " + path)
        else:
            logger.info("Not sync file %s" % path)

    @lru.get
    def get_content(self, path):
        local_path, hdfs_path = self._init_path(path)
        self._create_local(local_path)
        try:
            if not os.path.exists(local_path):
                    hadoopy.get(hdfs_path, local_path)
            with open(local_path, mode='rb') as f:
                d = f.read()
        except Exception as e:
            raise exceptions.FileNotFoundError('%s is not there (%s)'
                                               % (local_path, e.strerror))
        self._delete_local_file(local_path)
        return d

    @lru.set
    def put_content(self, path, content):
        local_path, hdfs_path = self._init_path(path)
        self._create_local(local_path)
        with open(local_path, mode='wb') as f:
            f.write(content)
        self._create_hdfs(hdfs_path)
        hdfs_putf(local_path, hdfs_path)
        self._delete_local_file(local_path)
        self._sync_with_slaves(path, "ADD")
        return hdfs_path

    def stream_read(self, path, bytes_range=None):
        hdfs_path = (self._init_path(path))[1]

        try:
            client = Client(self._hdfs_nn_host, self._hdfs_nn_port)
            xs = client.cat([hdfs_path], True)
            for content in xs.next():
                yield content
        except Exception:
            raise exceptions.FileNotFoundError('%s is not there' % path)


    def stream_write(self, path, fp):
        # Size is mandatory
        local_path, hdfs_path = self._init_path(path)
        self._create_local(local_path)
        with open(local_path, mode='wb') as f:
            try:
                while True:
                    buf = fp.read(self.buffer_size)
                    if not buf:
                        break
                    f.write(buf)
            except IOError:
                pass
        self._create_hdfs(hdfs_path)
        hdfs_putf(local_path, hdfs_path)
        self._delete_local_file(local_path)
        self._sync_with_slaves(hdfs_path, "ADD")

    def list_directory(self, path=None):
        hdfs_path = (self._init_path(path))[1]
        try:
            return hadoopy.ls(hdfs_path)
        except Exception:
            raise exceptions.FileNotFoundError('%s is not there' % path)

    def exists(self, path):
        local_path, hdfs_path = self._init_path(path)
        if os.path.exists(local_path):
            return True
        else:
            return hadoopy.exists(hdfs_path)

    @lru.remove
    def remove(self, path):
        local_path, hdfs_path = self._init_path(path)
        if os.path.isdir(local_path):
            shutil.rmtree(local_path)
            return
        try:
            os.remove(local_path)
        except Exception:
            pass
        try:
            hdfs_rmr(hdfs_path)
        except Exception:
            raise exceptions.FileNotFoundError('%s is not there' % path)
        self._sync_with_slaves(hdfs_path, "DEL")

    def get_size(self, path):
        local_path, hdfs_path = self._init_path(path)
        try:
            return os.path.getsize(local_path)
        except OSError:
            try:
                return hdfs_du(hdfs_path)
            except Exception:
                raise exceptions.FileNotFoundError('%s is not there' % path)

#
# Editor modelines  -  http://www.wireshark.org/tools/modelines.html
#
# Local variables:
# c-basic-offset: 4
# tab-width: 8
# indent-tabs-mode: nil
# End:
#
# vi: set shiftwidth=4 tabstop=8 expandtab:
# :indentSize=4:tabSize=8:noTabs=true:
#
