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

from common import AZKABAN_NAME, AZKABAN_EXE_HOME, AZKABAN_EXE_CONF
from resource_management.core.exceptions import ExecutionFailed, ComponentIsNotRunning
from resource_management.core.resources.system import Execute
from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger

class ExecutorServer(Script):
    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)
        Execute('echo execute.as.user=false > {0} '.format(AZKABAN_EXE_HOME + '/plugins/jobtypes/commonprivate.properties'))
        self.configure(env)

    def stop(self, env):
        Execute('cd {0} && (bin/azkaban-executor-shutdown.sh|| echo 1 > /dev/null)'.format(AZKABAN_EXE_HOME))

    def start(self, env):
        from params import azkaban_executor_properties
        self.configure(env)
        Execute('cd {0} && bin/azkaban-executor-start.sh'.format(AZKABAN_EXE_HOME))
        Execute(
            'curl http://localhost:{0}/executor?action=activate'.format(azkaban_executor_properties['executor.port']), tries=5, try_sleep = 2
        )

    def status(self, env):
        try:
            Execute(
                'export AZ_CNT=`ps -ef |grep -v grep |grep AzkabanExecutorServer | wc -l` && `if [ $AZ_CNT -ne 0 ];then exit 0;else exit 3;fi `'
           )
            
        except ExecutionFailed as ef:
            if ef.code == 3:
                raise ComponentIsNotRunning("ComponentIsNotRunning")
            else:
                raise ef

    def configure(self, env):
        from params import azkaban_executor_properties, log4j_properties, azkaban_db
        key_val_template = '{0}={1}\n'

        with open(path.join(AZKABAN_EXE_CONF, 'azkaban.properties'), 'w') as f:
            for key, value in azkaban_db.iteritems():
                f.write(key_val_template.format(key, value))
            for key, value in azkaban_executor_properties.iteritems():
                if key != 'content':
                    f.write(key_val_template.format(key, value))
            f.write(azkaban_executor_properties['content'])

        with open(path.join(AZKABAN_EXE_CONF, 'log4j.properties'), 'w') as f:
            f.write(log4j_properties['content'])

if __name__ == '__main__':
    ExecutorServer().execute()
