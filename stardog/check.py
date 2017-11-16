import base64
import re
import requests

from checks import AgentCheck


EVENT_TYPE = SOURCE_TYPE_NAME = 'stardog'


def convert_value(in_key, in_val):
    key = "stardog.%s" % in_key
    val = in_val['value']
    return {key: val}


def convert_count(in_key, in_val):
    key = "stardog.%s" % in_key
    val = in_val['count']
    return {key: val}


def convert_query_speed(in_key, in_val):
    try:
        if in_val['duration_units'] != 'seconds':
            raise Exception('Unsupported duration units')
        if in_val['rate_units'] != 'calls/second':
            raise Exception('Unsupported rate units')
    except KeyError:
        raise Exception('The units are not properly defined')

    entry_key = ["count", "max", "mean", "min", "p50", "p75", "p95", "p98",
               "p99", "p999", "stddev", "m15_rate", "m1_rate", "m5_rate",
               "mean_rate"]
    out_dict = {}
    for ent in entry_key:
        new_key = "stardog.%s.%s" % (in_key, ent)
        out_dict[new_key] = in_val[ent]
    return out_dict


_g_metrics_map = {
    'dbms.mem.mapped.max': convert_value,
    'dbms.memory.heap.reserve': convert_value,
    'dbms.mem.direct.pool.used': convert_value,
    'dbms.mem.mapped.used': convert_value,
    'dbms.mem.heap.used': convert_value,
    'dbms.memory.direct.reserve': convert_value,
    'dbms.mem.direct.max': convert_value,
    'dbms.page.cache.size': convert_value,
    'system.cpu.usage': convert_value,
    'dbms.mem.heap.max': convert_value,
    'dbms.mem.direct.buffer.used': convert_value,
    'dbms.mem.direct.buffer.max': convert_value,
    'databases.system.planCache.ratio': convert_value,
    'databases.system.planCache.size': convert_value,
    'system.uptime': convert_value,
    'dbms.memory.managed.heap': convert_value,
    'dbms.mem.direct.pool.max': convert_value,
    'dbms.mem.direct.used': convert_value,
    'dbms.memory.managed.direct': convert_value,
    'databases.*.txns.openTransactions': convert_count,
    'databases.*.txns.speed': convert_query_speed,
    'databases.*.queries.running': convert_count,
    'databases.*.queries.speed': convert_query_speed,
    'databases.*.openConnections': convert_count,
}


class StardogCheck(AgentCheck):
    def check(self, instance):
        try:
            auth_token = base64.b64encode(instance['username'] + ":" + instance['password'])
            response = requests.get(instance['stardog_url'] + '/admin/status', headers={'Authorization': 'Basic ' + auth_token})
        except KeyError:
            raise Exception('The Stardog check instance is not properly configured')

        if response.status_code != 200:
            response.raise_for_status()
        json_doc = response.json()
        try:
            tags = instance['tags']
            if type(tags) != list:
                self.log.warn('The tags list in the Stardog check is not configured properly')
                tags = []
        except KeyError:
            tags = []

        tags.append("stardog_url:%s" % instance['stardog_url'])
        for k in json_doc:
            # find match
            for regex in _g_metrics_map:
                p = re.compile(regex)
                if p.match(k) is not None:
                    convert_func = _g_metrics_map[regex]
                    values_map = convert_func(k, json_doc[k])
                    for report_key in values_map:
                        self.gauge(report_key, values_map[report_key], tags=tags)
                    break
