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

import os.path as path

from common import AZKABAN_NAME, AZKABAN_WEB_HOME, AZKABAN_WEB_CONF, AZKABAN_SQL
from resource_management.core.exceptions import ExecutionFailed, ComponentIsNotRunning
from resource_management.core.resources.system import Execute
from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger

class WebServer(Script):
    def install(self, env):
        from params import java_home, azkaban_db
        import params
        env.set_params(params)
        self.install_packages(env)        
        Execute('echo execute.as.user=false > {0} '.format(AZKABAN_WEB_HOME + '/plugins/jobtypes/commonprivate.properties'))
        self.configure(env)

    def stop(self, env):
        Execute('cd {0} && (bin/azkaban-web-shutdown.sh || echo 1 > /dev/null)'.format(AZKABAN_WEB_HOME))

    def start(self, env):
        self.configure(env)
        if not self.is_init_azkaban_schema(env):
            self.create_azkaban_schema(env)
        
        Execute('cd {0} && bin/azkaban-web-start.sh'.format(AZKABAN_WEB_HOME))

    def status(self, env):
        try:
            Execute(
                'export AZ_CNT=`ps -ef |grep -v grep |grep azkaban-web-server | wc -l` && `if [ $AZ_CNT -ne 0 ];then exit 0;else exit 3;fi `'
            )
        except ExecutionFailed as ef:
            if ef.code == 3:
                raise ComponentIsNotRunning("ComponentIsNotRunning")
            else:
                raise ef

    def configure(self, env):
        from params import azkaban_db, azkaban_web_properties, azkaban_users, global_properties, log4j_properties
        key_val_template = '{0}={1}\n'

        with open(path.join(AZKABAN_WEB_CONF, 'azkaban.properties'), 'w') as f:
            for key, value in azkaban_db.iteritems():
                f.write(key_val_template.format(key, value))
            for key, value in azkaban_web_properties.iteritems():
                if key != 'content':
                    f.write(key_val_template.format(key, value))
            f.write(azkaban_web_properties['content'])

        with open(path.join(AZKABAN_WEB_CONF, 'azkaban-users.xml'), 'w') as f:
            f.write(str(azkaban_users['content']))

        with open(path.join(AZKABAN_WEB_CONF, 'global.properties'), 'w') as f:
            f.write(global_properties['content'])

        with open(path.join(AZKABAN_WEB_CONF, 'log4j.properties'), 'w') as f:
            f.write(log4j_properties['content'])
    
    def create_azkaban_schema(self, env):
        from params import azkaban_db
        Execute(
            'mysql -h{0} -P{1} -D{2} -u{3} -p{4} < {5}'.format(
                azkaban_db['mysql.host'],
                azkaban_db['mysql.port'],
                azkaban_db['mysql.database'],
                azkaban_db['mysql.user'],
                azkaban_db['mysql.password'],
                AZKABAN_SQL,
            )
        )
    
    def is_init_azkaban_schema(self, env):
        from params import azkaban_db
        import MySQLdb
        db = MySQLdb.connect(azkaban_db['mysql.host'],azkaban_db['mysql.user'],azkaban_db['mysql.password'],azkaban_db['mysql.database'])
        cursor = db.cursor()
        
        query_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = 'executors'".format(azkaban_db['mysql.database'])
        Logger.info("query sql : {0}".format(query_sql))
        results = []
        try:
           cursor.execute(query_sql)
           results = cursor.fetchall()
        except:
           Logger.error("Error: unable to fecth data")
        
        db.close()
        
        return len(results) > 0
    
if __name__ == '__main__':
    WebServer().execute()
