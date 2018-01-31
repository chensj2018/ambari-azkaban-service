#!/usr/bin/env python

"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
import socket
import time
import traceback
import logging
import MySQLdb

from resource_management.core import global_lock
from resource_management.libraries.functions import format
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions import stack_tools
from resource_management.core.resources import Execute
from resource_management.core.signal_utils import TerminateStrategy
from ambari_commons.os_check import OSConst
from ambari_commons.os_family_impl import OsFamilyFuncImpl, OsFamilyImpl

AZ_EXECUTOR_MYSQL_HOSTNAME = '{{azkaban-db/mysql.host}}'
AZ_EXECUTOR_MYSQL_PORT = '{{azkaban-db/mysql.port}}'
AZ_EXECUTOR_MYSQL_USER = '{{azkaban-db/mysql.user}}'
AZ_EXECUTOR_MYSQL_PWD = '{{azkaban-db/mysql.password}}'
AZ_EXECUTOR_MYSQL_DBNAME = '{{azkaban-db/mysql.database}}'

logger = logging.getLogger('ambari_alerts')

def get_tokens():
  """
  Returns a tuple of tokens in the format {{site/property}} that will be used
  to build the dictionary passed into execute
  """
  return (AZ_EXECUTOR_MYSQL_HOSTNAME,AZ_EXECUTOR_MYSQL_PORT,AZ_EXECUTOR_MYSQL_USER,AZ_EXECUTOR_MYSQL_PWD,AZ_EXECUTOR_MYSQL_DBNAME)

def execute(configurations={}, parameters={}, host_name=None):
  """
  Returns a tuple containing the result code and a pre-formatted result label

  Keyword arguments:
  configurations (dictionary): a mapping of configuration key to value
  parameters (dictionary): a mapping of script parameter key to value
  host_name (string): the name of this host where the alert is running
  """
 
  if configurations is None:
    return (('UNKNOWN', ['There were no configurations supplied to the script.']))

  if not AZ_EXECUTOR_MYSQL_HOSTNAME in configurations:
    return (('UNKNOWN', ['executor mysql hostname were not supplied to the script.']))
  
  hostname = configurations[AZ_EXECUTOR_MYSQL_HOSTNAME]
  user = configurations[AZ_EXECUTOR_MYSQL_USER]
  pwd = configurations[AZ_EXECUTOR_MYSQL_PWD]
  dbname = configurations[AZ_EXECUTOR_MYSQL_DBNAME]
  
  import socket
  self_hostname = socket.getfqdn(socket.gethostname()) 
  
  result_code = None
  label = ""
  try:
    if get_executor_status(hostname,user,pwd,dbname,self_hostname):
      result_code = 'OK'
    else:
      result_code = 'CRITICAL'
      label= self_hostname + " - executor status not active"
  except:
    result_code = 'UNKNOWN'
  
  return (result_code, [label])

def get_executor_status(hostname,user,pwd,dbname,self_hostname):
  db = MySQLdb.connect(hostname,user,pwd,dbname)
  cursor = db.cursor()
  
  query_sql = "select active from azkaban.executors where host=\'{0}\' and active=1".format(self_hostname)
  results = []
  try:
     cursor.execute(query_sql)
     results = cursor.fetchall()
  except:
     logger.error("Error: unable to fecth data")
  
  db.close()
  
  return len(results) > 0



  
