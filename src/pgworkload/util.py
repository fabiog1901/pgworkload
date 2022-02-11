import yaml
import http.server
import time
import tabulate
import numpy as np
import socket

RESERVED_WORDS = ['unique', 'inverted', 'index', 'constraint',
                  'family', 'like', 'primary', 'key',
                  'create', 'table',
                  'if', 'not', 'exists', 'null',
                  'global', 'local', 'temporary', 'temp', 'unlogged',
                  'visible', 'using', 'hash' 'with', 'bucket_count']

DEFAULT_ARRAY_COUNT = 3

class QuietServerHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler that doesn't output any log
    """
   
    def log_message(self, format, *args):
        pass


class Stats:
    def __init__(self, frequency):
        self.cumulative_counts = {}
        self.instantiation_time = time.time()
        self.frequency = frequency
        self.new_window()

    # reset stats while keeping cumulative counts
    def new_window(self):
        try:
            self.window_start_time = time.time()
            self.window_stats = {}
        finally:
            pass

    # add one latency measurement in seconds
    def add_latency_measurement(self, action, measurement):
        try:
            self.window_stats.setdefault(action, []).append(measurement)
            self.cumulative_counts.setdefault(action, 0)
            self.cumulative_counts[action] += 1
        finally:
            pass

    # print the current stats this instance has collected.
    # If action_list is empty, it will only prevent rows it has captured this period, otherwise it will print a row for each action.
    def print_stats(self, action_list=[]):
        def get_percentile_measurement(action, percentile):
            return np.percentile(self.window_stats.setdefault(action, [0]), percentile)

        def get_stats_row(action):
            elapsed = time.time() - self.instantiation_time

            if action in self.window_stats:
                return [action,
                        round(elapsed, 0),
                        self.cumulative_counts[action],
                        round(self.cumulative_counts[action] / elapsed, 2),
                        len(self.window_stats[action]),
                        round(
                            len(self.window_stats[action]) / self.frequency, 2),
                        round(
                            float(np.mean(self.window_stats[action]) * 1000), 2),
                        round(float(get_percentile_measurement(
                            action, 50)) * 1000, 2),
                        round(float(get_percentile_measurement(
                            action, 90)) * 1000, 2),
                        round(float(get_percentile_measurement(
                            action, 95)) * 1000, 2),
                        round(float(get_percentile_measurement(
                            action, 99)) * 1000, 2),
                        round(float(get_percentile_measurement(
                            action, 100)) * 1000, 2)]
            else:
                return [action, round(elapsed, 0), self.cumulative_counts.get(action, 0), 0, 0, 0, 0, 0, 0]

        header = ["id", "elapsed",  "tot_ops", "tot_ops/s",
                  "period_ops", "period_ops/s", "mean(ms)",  "p50(ms)", "p90(ms)", "p95(ms)", "p99(ms)", "pMax(ms)"]
        rows = []

        try:
            if len(action_list):
                for action in sorted(action_list):
                    rows.append(get_stats_row(action))
            else:
                for action in sorted(list(self.window_stats)):
                    rows.append(get_stats_row(action))
            print(tabulate.tabulate(rows, header), "\n")
        finally:
            pass


def __get_type_and_args(datatypes: list):

    # check if it is an array
    # string array
    # string []
    # string[]
    is_array = False
    datatypes = [x.lower() for x in datatypes]
    if '[]' in datatypes[0] or 'array' in datatypes or '[]' in datatypes:
        is_array = True

    datatype: str = datatypes[0].replace('[]', '')

    if datatype.lower() in ['bool', 'boolean']:
        return {'type': 'bool',
                'args': {
                    'seed': 0,
                    'null_pct': 0.0,
                    'array': DEFAULT_ARRAY_COUNT if is_array else 0}
                }

    elif datatype.lower() in ['int', 'integer', 'int2', 'int4', 'int8', 'int64', 'bigint', 'smallint']:
        return {'type': 'integer',
                'args': {
                    'min': 0,
                    'max':  1000000,
                    'seed': 0,
                    'null_pct': 0.0,
                    'array': DEFAULT_ARRAY_COUNT if is_array else 0}
                }

    elif datatype.lower() in ['string', 'char', 'character', 'varchar', 'text']:
        return {'type': 'string',
                'args': {
                    'min': 10,
                    'max': 30,
                    'seed': 0,
                    'null_pct': 0.0,
                    'array': DEFAULT_ARRAY_COUNT if is_array else 0}
                }

    elif datatype.lower() in ['decimal', 'float', 'dec', 'numeric', 'real', 'double']:
        return {'type': 'float',
                'args': {
                    'max': 10000,
                    'round': 2,
                    'seed': 0,
                    'null_pct': 0.0,
                    'array': DEFAULT_ARRAY_COUNT if is_array else 0}
                }

    elif datatype.lower() in ['time', 'timetz']:
        return {'type': 'time',
                'args': {
                    'start': '07:30:00',
                    'end': '15:30:00',
                    'micros': False,
                    'seed': 0,
                    'null_pct': 0.0,
                    'array': DEFAULT_ARRAY_COUNT if is_array else 0}
                }

    elif datatype.lower() in ['json', 'jsonb']:
        return {'type': 'jsonb',
                'args': {
                    'min': 10,
                    'max': 50,
                    'seed': 0,
                    'null_pct': 0.0}
                }
        
    elif datatype.lower() == 'date':
        return {'type': 'date',
                'args': {
                    'start': '2022-01-01',
                    'end': '2022-12-31',
                    'format': '%Y-%m-%d',
                    'seed': 0,
                    'null_pct': 0.0,
                    'array': DEFAULT_ARRAY_COUNT if is_array else 0}
                }

    elif datatype.lower() in ['timestamp', 'timestamptz']:
        return {'type': 'timestamp',
                'args': {
                    'start': '2022-01-01',
                    'end': '2022-12-31',
                    'format': '%Y-%m-%d %H:%M:%S.%f',
                    'seed': 0,
                    'null_pct': 0.0,
                    'array': DEFAULT_ARRAY_COUNT if is_array else 0}
                }

    elif datatype.lower() == 'uuid':
        return {'type': 'UUIDv4',
                'args': {
                    'seed': 0,
                    'null_pct': 0,
                    'array': DEFAULT_ARRAY_COUNT if is_array else 0}
                }

    else:
        raise ValueError(f"Data type not implemented: '{datatype}'")


def __get_table_name_and_table_list(create_table_stmt: str, sort_by: list, count: int = 1000000):

    p1 = create_table_stmt.find('(')
    p2 = create_table_stmt.rfind(')')

    # find table name (before parenthesis part)
    for i in create_table_stmt[:p1].split():
        if i.lower() not in RESERVED_WORDS:
            table_name = i.replace('.', '__')
            break

    # extract column definitions (within parentheses part)
    # eg: 
    #   id uuid primary key
    #   s string(30)
    col_def_raw = create_table_stmt[p1+1:p2]

    # remove slices delimited by parenthesis
    # eg: from id 'sting(30)' to 'id string'
    # this is important as within parenthesis we might find commas
    # and we need to split on commas later
    within_brackets = False
    col_def = ''
    for i in col_def_raw:
        if i == '(':
            within_brackets = True
            continue
        if i == ')':
            within_brackets = False
            continue
        if not within_brackets:
            col_def += i

    col_def = [x.strip().lower() for x in col_def.split(',')]

    ll = []
    for x in col_def:
        # remove commented lines
        if not x.startswith('--'):
            col_name_and_type = x.strip().split(" ")[:3]
            # remove those lines that are not column definition,
            # like CONSTRAINT, INDEX, FAMILY, etc..
            if col_name_and_type[0].lower() not in RESERVED_WORDS:
                ll.append(col_name_and_type)

    table_list = []
    table_list.append({'count': count})
    table_list[0]['sort-by'] = sort_by
    table_list[0]['tables'] = {}

    for x in ll:
        table_list[0]['tables'][x[0]] = __get_type_and_args(x[1:])

    return table_name, table_list


def __get_create_table_stmts(ddl: str):
    """Parses a DDL SQL file and returns only the CREATE TABLE stmts

    Args:
        ddl (str): the raw DDL string

    Returns:
        list: the list of CREATE TABLE stmts
    """

    # separate input into a 'create table' stmts list
    stmts = ' '.join(x.lower() for x in ddl.split())
    
    # strip whitespace and remove empty items
    stmts: list = [x.strip() for x in stmts.split(';') if x != '']

    # keep only string that start with 'create' and
    # have word 'table' between beginning and the first open parenthesis
    create_table_stmts = []
    for i in stmts:
        p1 = i.find('(')
        if i.startswith('create'):
            if 'table' in i[:p1].lower():
                create_table_stmts.append(i)

    return create_table_stmts

def get_hostname():
    return socket.gethostname()

def ddl_to_yaml(ddl: str):
          
    stmts = __get_create_table_stmts(ddl)

    d = {}
    for x in stmts:
        table_name, table_list = __get_table_name_and_table_list(
            x, count=1000, sort_by=[])
        d[table_name] = table_list

    return yaml.dump(d, default_flow_style=False, sort_keys=False)
