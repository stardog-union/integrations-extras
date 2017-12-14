import time
from datetime import datetime

MAX_TITLE_LEN = 32

ALERT_TYPES = {
    "FATAL": "error",
    "ERROR": "error",
    "WARN": "warning",
    "INFO": "info",
    "DEBUG": "info",
    "TRACE": "info",
}


def stardog_parser(logger, line):
    la = line.split()
    parse_tokens = ['ERROR', 'WARN', 'INFO']
    if la[0] in parse_tokens:
        # INFO  2017-12-04 11:29:26,294 [main] com.complexible.stardog.cli.impl.ServerStart:call(229): Memory options
        date_str = "%s %s000" % (la[1], la[2])
        date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S,%f")
        date = time.mktime(date.timetuple())

        event_dict = {
            'timestamp': date,
            'priority': 'normal',
            'aggregation_key': 'log_parser',
            'event_type': 'stardog.log',
            'alert_type': ALERT_TYPES[la[0]],
            'msg_text': ' '.join(la[3:]),
        }
        if la[0] in parse_tokens[:2]:
            event_dict['msg_title'] = "%s %s" % (la[0], la[4][-MAX_TITLE_LEN:])
            return [event_dict]
        elif la[0] == 'INFO':
            if la[4].startswith('com.complexible.stardog.pack.replication.impl.zookeeper.ZkCluster:join'):
                info_line = ' '.join(la[5:])
                if info_line.find('Attempting to join') >= 0:
                    event_dict['msg_title'] = 'Attempting to join cluster'
                    event_dict['alert_type'] = 'warning'
                    return [event_dict]
            elif la[4].startswith('com.complexible.stardog.StardogKernel:start'):
                info_line = ' '.join(la[5:])
                if info_line.find('Initializing Stardog') >= 0:
                    event_dict['msg_title'] = 'Starting stardog'
                    event_dict['alert_type'] = 'info'
                    return [event_dict]
            elif la[4].startswith('com.complexible.stardog.pack.replication.ReplicatedKernelImpl:joinCluster'):
                info_line = ' '.join(la[5:])
                if info_line.find('joined the cluster') >= 0:
                    event_dict['msg_title'] = 'Joined the cluster'
                    event_dict['alert_type'] = 'warning'
                    return [event_dict]
            elif la[4].startswith('com.complexible.stardog.StardogKernel'):
                info_line = ' '.join(la[5:])
                if info_line.find('Parsing triples finished in') >= 0:
                    event_dict['msg_title'] = 'Parsing triples finished'
                    event_dict['alert_type'] = 'warning'
                    return [event_dict]
                if info_line.find('Indexing triples finished in') >= 0:
                    event_dict['msg_title'] = 'Indexing triples finished'
                    event_dict['alert_type'] = 'warning'
                    return [event_dict]
    return None
