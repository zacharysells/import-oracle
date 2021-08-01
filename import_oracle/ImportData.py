import glob
import logging
import json
import sys
import os
import argparse
import cx_Oracle

parser = argparse.ArgumentParser()
parser.add_argument("json_file_path", help="Relative path to file or file glob of json data import files")
parser.add_argument("--executesql", help="`json_file_path` arg will be parsed as an ExecuteSQL script.", action="store_true")
parser.add_argument("--selectall", help="Print all rows in table", action="store_true")
parser.add_argument("--bootstrap", help="Bootstrap db with provided .sql file. WARNING this will drop the table first.")
args = parser.parse_args()

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

def execute_sql(connection, query, commit=True):
    logging.info('Executing SQL query="%s"' % query)
    curs = connection.cursor()
    curs.execute(query)
    connection.commit()
    pass

def executemany_sql(connection, db_table, fields, rows, commit=True):
    inserts = '\n'.join(['into %s(%s) values(%s)' % (db_table, ','.join(fields), row) for row in rows])
    sql_query = """insert all %s SELECT * FROM dual""" % inserts
    logging.info('Bulk SQL query="%s"' % sql_query)
    try:
        pass
        curs = connection.cursor()
        curs.execute(sql_query)
        connection.commit()
    except cx_Oracle.DatabaseError as e:
        errorObj, = e.args
        logging.error("Row %d has error %s" % (curs.rowcount, errorObj.message))
        raise e

def select_all(connection, db_table):
    curs = connection.cursor()
    curs.execute('SELECT * FROM %s' % db_table)
    for row in curs.fetchall():
        for elem in row:
            print('%s ' % str(elem).ljust(2), end='')
        print()

def create_db_connection(db_host, db_port, db_user, db_pass, db_service, db_encoding):
    logging.info("Connecting to database %s@%s" % (db_user, db_host))
    dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_service)
    connection = cx_Oracle.connect(db_user, db_pass, dsn, encoding=db_encoding)
    return connection

def map_mapping_types(row, mapping_list, header=True, header_values=None):
    row_string = ''
    for i, elem in enumerate(mapping_list):
        e = elem['Type']
        if e.startswith('DBF-'):
            row_string = '%s,%s' % (row_string, e.replace('DBF-', ''))
        elif e.startswith('C-'):
            row_string = "%s,'%s'" % (row_string, e.replace('C-', ''))
        elif e == 'S':
            if header:
                source_column = int(header_values.index(elem['Source']))
            else:
                source_column = i
            # Check for db_mapping parameter
            if elem.get('DB_CONVERSION'):
                cell_value = elem['DB_CONVERSION'].replace('?', row[source_column])
                row_string = row_string + (",%s" % cell_value)
            else:
                cell_value = str(row[source_column]).replace("'", "''")
                row_string = row_string + (",'%s'" % cell_value)
        else:
            logging.fatal("Unknown type %s in Column Mappings" % e)
            sys.exit(1)
    return row_string.lstrip(',')

def process_import(descriptor_file):
    # descriptor_file arg should be a relative path to a .json 
    # file with all the required info to handle the data import.

    with open(descriptor_file, 'r') as fp:
        descriptor_file_data = json.load(fp)
        if 'TargetInfo' in descriptor_file_data:
            db_host = descriptor_file_data['TargetInfo']['DBServer']
            db_port = int(descriptor_file_data['TargetInfo']['DBPort'])
            db_schema = descriptor_file_data['TargetInfo']['Schema']
            db_service = descriptor_file_data['TargetInfo']['DBService']
            db_user = descriptor_file_data['TargetInfo']['UserName']
            db_pass = descriptor_file_data['TargetInfo']['PassWord']
            db_table = descriptor_file_data['TargetInfo']['TableName']
            db_encoding = descriptor_file_data['TargetInfo'].get('DBEncoding', 'UTF-8')
        if 'SourceInfo' in descriptor_file_data:
            source_file = descriptor_file_data['SourceInfo']['Location']
            source_delimeter = descriptor_file_data['SourceInfo']['Delimiter']
            source_filetype = descriptor_file_data['SourceInfo']['FileType']
            source_fileheader = True if descriptor_file_data['SourceInfo']['FileHeader'].lower() == 'yes' else False
            MAX_BYTES_PER_CHUNK = descriptor_file_data['MaxBytesPerChunk']

    connection = create_db_connection(db_host, db_port, db_user, db_pass, db_service, db_encoding)
    if args.bootstrap:
        with open(args.bootstrap, 'r') as fp:
            try:
                execute_sql(connection, "DROP TABLE %s.%s" % (db_schema, db_table))
            except:
                pass
            for sql_command in fp.read().split(';'):
                execute_sql(connection, sql_command)
    if args.selectall:
        select_all(connection, db_table)
        return

    if args.executesql:
        logging.info("Running 'ExecuteSQL' method on %s" % descriptor_file)
        sorted_sql_statements = sorted(descriptor_file_data['SQLStatements'], key=lambda k: int(k.get('Order') or sys.maxsize))
        for sql in sorted_sql_statements:
            execute_sql(connection, sql['SQL'].rstrip(';'))

        return

    sorted_col_mappings = sorted(descriptor_file_data['ColMappings'], key=lambda k: int(k.get('Order') or sys.maxsize))
    header_values = None
    with open(os.path.join(os.path.dirname(__file__), source_file), 'r') as f:
        if source_fileheader:
            header_values = next(f)
            header_values = header_values.strip().split(source_delimeter)
        while True:
            bulk_row_insert = []
            lines = f.readlines(MAX_BYTES_PER_CHUNK)
            if not lines:
                break
            logging.info("Reading %d bytes from input file - %d rows" % (MAX_BYTES_PER_CHUNK, len(lines)))
            for line in lines:
                data = line.strip().split(source_delimeter)

                bulk_row_insert.append(map_mapping_types(data, sorted_col_mappings, header=source_fileheader, header_values=header_values))
            logging.info('Bulk inserting %d rows' % len(bulk_row_insert))
            executemany_sql(connection, db_table, [x['Target'] for x in sorted_col_mappings], bulk_row_insert)

if __name__ == "__main__":
    for desc_file in glob.glob(args.json_file_path):
        logging.info("Processing %s" % desc_file)
        process_import(desc_file)
