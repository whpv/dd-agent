#encoding=utf8
import sys
import subprocess
import re
import tempfile
from checks import AgentCheck
from subprocess import PIPE
#更改默认编码
reload(sys)
sys.setdefaultencoding("utf8")


class Oracle(AgentCheck):

    SERVICE_CHECK_NAME = 'oracle.can_connect'
    SYS_METRICS = {
        'Buffer Cache Hit Ratio':           'oracle.buffer_cachehit_ratio',
        'Cursor Cache Hit Ratio':           'oracle.cursor_cachehit_ratio',
        'Library Cache Hit Ratio':          'oracle.library_cachehit_ratio',
        'Shared Pool Free %':               'oracle.shared_pool_free',
        'Physical Reads Per Sec':           'oracle.physical_reads',
        'Physical Writes Per Sec':          'oracle.physical_writes',
        'Enqueue Timeouts Per Sec':         'oracle.enqueue_timeouts',
        'GC CR Block Received Per Second':  'oracle.gc_cr_receive_time',
        'Global Cache Blocks Corrupted':    'oracle.cache_blocks_corrupt',
        'Global Cache Blocks Lost':         'oracle.cache_blocks_lost',
        'Logons Per Sec':                   'oracle.logons',
        'Average Active Sessions':          'oracle.active_sessions',
        'Long Table Scans Per Sec':         'oracle.long_table_scans',
        'SQL Service Response Time':        'oracle.service_response_time',
        'User Rollbacks Per Sec':           'oracle.user_rollbacks',
        'Total Sorts Per User Call':        'oracle.sorts_per_user_call',
        'Rows Per Sort':                    'oracle.rows_per_sort',
        'Disk Sort Per Sec':                'oracle.disk_sorts',
        'Memory Sorts Ratio':               'oracle.memroy_sorts_ratio',
        'Database Wait Time Ratio':         'oracle.database_wait_time_ratio',
        'Enqueue Timeouts Per Sec':         'oracle.enqueue_timeouts',
        'Session Limit %':                  'oracle.session_limit_usage',
        'Session Count':                    'oracle.session_count',
        'Temp Space Used':                  'oracle.temp_space_used',
    }

    def check(self, instance):
        self.log.debug('Running oracke_check')
        self.server, self.user, self.password, tags = self._get_config(instance)
        if not self.server or not self.user:
            raise Exception("Oracle host and user are needed")

        self._get_sys_metrics()
        self._get_tablespace_metrics()

    def _get_config(self, instance):
        server = instance.get('server', None)
        user = instance.get('user', None)
        password = instance.get('password', None)
        tags = instance.get('tags', None)
        return (server, user, password, tags)

    def _get_connection(self, server, user, password):
        self.service_check_tags = [
            'server:%s' % server
        ]
        with tempfile.TemporaryFile() as std_err:
            proc = subprocess.Popen(["sqlplus", "-L", "%s/%s" % (user, password)], stdout=PIPE, stdin=PIPE,
                                    stderr=std_err)
            err=std_err.read()
            if err:
                self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                                   tags=self.service_check_tags)
                self.log.error(err)
                raise Exception("connect error")
            return proc



    def _get_sys_metrics(self):
        query = "SELECT METRIC_NAME, VALUE FROM GV$SYSMETRIC ORDER BY BEGIN_TIME;"
        #query = "SELECT METRIC_NAME, VALUE FROM GV$SYSMETRIC ORDER BY BEGIN_TIME;"+"SELECT TABLESPACE_NAME, USED_SPACE, TABLESPACE_SIZE, USED_PERCENT FROM DBA_TABLESPACE_USAGE_METRICS;"

        cur = self._getSqlplusMsg(query)
        for row in cur:
            try:
                metric_name = row[0]
                metric_value = row[1]
                if metric_name in self.SYS_METRICS:
                    self.gauge(self.SYS_METRICS[metric_name], metric_value)
            except Exception:
                continue


    def _get_tablespace_metrics(self):
        query = "SELECT TABLESPACE_NAME, USED_SPACE, TABLESPACE_SIZE, USED_PERCENT FROM DBA_TABLESPACE_USAGE_METRICS;"

        cur =self._getSqlplusMsg(query)

        for row in cur:
            try:
                tablespace_tag = 'tablespace:%s' % row[0]
                used = row[1]
                size = row[2]
                in_use = row[3]
                self.gauge('oracle.tablespace.used', used, tags=[tablespace_tag])
                self.gauge('oracle.tablespace.size', size, tags=[tablespace_tag])
                self.gauge('oracle.tablespace.in_use', in_use, tags=[tablespace_tag])
            except Exception:
                continue

    def _getSqlplusMsg(self,sql):
        proc =self._get_connection(self.server, self.user, self.password)
        proc.stdin.write(sql)
        (out, err) = proc.communicate()
        # out有判断是否是gbk，utf8，两者都找不到"rows selected "的中英文,则表示连接/或者查询失败

        if out.find("rows selected.")>0 or out.find("已选择".decode("utf8").encode("gbk"))>0 or out.find("已选择")>0:
            self.log.debug("Connected to Oracle DB")
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.OK,tags=self.service_check_tags)
        else:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                               tags=self.service_check_tags)
            self.log.error("connect failed")
            raise Exception("can not connect to oracle")
        try:
            out = out.decode('gbk', errors="ignore").encode('gb2312')
        except Exception:
            out = out.decode('utf8').encode('gb2312')
        out = out.replace("\t", " ")
        out1 = re.split("[\n\r]+", out)

        out1 = [re.split('\s\s+', x) for x in out1 if
                x.endswith(('.', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'))]
        return out1
