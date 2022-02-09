import yaml

RESERVED_WORDS = ['unique', 'inverted', 'index', 'constraint',
                  'family', 'like', 'primary', 'key',
                  'create', 'table',
                  'if', 'not', 'exists', 'null',
                  'global', 'local', 'temporary', 'temp', 'unlogged',
                  'visible', 'using', 'hash' 'with', 'bucket_count']

DEFAULT_ARRAY_COUNT = 3


def get_type_and_args(datatypes: list):

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


def get_table_name_and_table_list(create_table_stmt: str, sort_by: list, count: int = 1000000):

    p1 = create_table_stmt.find('(')
    p2 = create_table_stmt.rfind(')')

    # find table name (before parenthesis part)
    for i in create_table_stmt[:p1].split():
        if i.lower() not in RESERVED_WORDS:
            table_name = i.replace('.', '__')
            break

    # extract column definitions (within parentheses part)
    # eg: id string(30)
    # this is important as within parenthesis we might find commas
    # and we need to split on commas later
    col_def_raw = create_table_stmt[p1+1:p2]

    # remove slices delimited by parenthesis
    # eg: from id 'sting(30)' to 'id string'
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

    col_def = col_def.split(',')

    ll = []
    for x in col_def:
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
        table_list[0]['tables'][x[0]] = get_type_and_args(x[1:])

    return table_name, table_list


def get_create_table_stmts(ddl: str):
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
            print(i)
            if 'table' in i[:p1].lower():
                create_table_stmts.append(i)

    return create_table_stmts


def ddl_to_yaml(input: str, output: str):

    with open(input, 'r') as f:
        ddl = f.read()
        
    stmts = get_create_table_stmts(ddl)

    d = {}
    for x in stmts:
        table_name, table_list = get_table_name_and_table_list(
            x, count=1000, sort_by=[])
        d[table_name] = table_list

    with open(output, 'w') as f:
        f.write(yaml.dump(d, default_flow_style=False, sort_keys=False))
