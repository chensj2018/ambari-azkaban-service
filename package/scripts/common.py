# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import ConfigParser

script_dir = os.path.dirname(os.path.realpath(__file__))
config = ConfigParser.ConfigParser()
config.readfp(open(os.path.join(script_dir, 'download.ini')))

VERSION = '2.6.4.0-91'
AZKABAN_EXE_HOME = '/usr/hdp/'+VERSION+'/azkaban-exec-server'
AZKABAN_WEB_HOME = '/usr/hdp/'+VERSION+'/azkaban-web-server'
AZKABAN_SQL = '/usr/hdp/'+VERSION+'/azkaban-db/create-all-sql-3.40.0.sql'
AZKABAN_EXE_CONF = AZKABAN_EXE_HOME + '/conf'
AZKABAN_WEB_CONF = AZKABAN_WEB_HOME + '/conf'

AZKABAN_NAME = 'azkaban'
