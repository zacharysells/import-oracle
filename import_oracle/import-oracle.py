import cx_Oracle
from datetime import datetime
import os
import argparse

DB_USER = os.environ.get('DB_USER', "")
DB_PASSWORD = os.environ.get('DB_PASSWORD', "")
DB_HOST = os.environ.get('DB_HOST', "")

MAX_BYTES_PER_CHUNK=16000

parser = argparse.ArgumentParser(description='Import CSV data into Oracle')
parser.add_argument('--header', dest='header', action='store_const', const=True, default=False, help='Use if first row of input file is a header row and should not be imported.')
parser.add_argument('--delimiter', default='~', help='Delimiter to use when parsing input file. Defaults to ~. use "tab" keyword to specify input is delimeted by tabs')
parser.add_argument('--empty-target', dest='empty_target', action='store_const', const=True, default=False, help='Use this flag to clear out target database schema before importing data.')
parser.add_argument('country', help='Country to be added to country column: SCL, ALA etc')
parser.add_argument('target', help='Target database table to import data into')
parser.add_argument('inputfile', help='Input file for parsing')
args = parser.parse_args()

load_date_now = datetime.now()
load_date = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
LOG_FILE = '%s_%s_error_log.txt' % (args.target, load_date_now.strftime('%Y%m%d-%H:%M:%S'))

def log(msg, level='INFO'):
    print('[%s] - %s' % (level, msg))
    if level == 'ERROR':
        with open(LOG_FILE, 'a+') as fp:
            fp.write('[ERROR] - %s' % msg)

def execute_sql(query, commit=True):
    log('SQL query="%s"' % query, level='DEBUG')
    try:
        curs = connection.cursor()
        curs.execute(query)
        connection.commit()
    except Exception as e:
        log(str(e), level='ERROR')

def executemany_sql(rows, commit=True):
    sql_query = """insert into %s values (%s)""" % (args.target,','.join([':%d' % d for d in range(1,len(rows[0])+1)]))
    log('Bulk SQL query="%s"' % sql_query, level='DEBUG')
    try:
        curs = connection.cursor()
        curs.executemany(sql_query, rows)
        connection.commit()
    except cx_Oracle.DatabaseError as e:
        errorObj, = e.args
        log("Row %d has error %s" % (curs.rowcount, errorObj.message), level='ERROR')

def empty_target():
    sql_query = "delete from %s" % args.target
    execute_sql(sql_query)

def import_data():
    f = open(args.inputfile, 'r')
    if args.header:
        next(f)
    delimiter = args.delimiter if args.delimiter != 'tab' else '\t'

    # Bulk insert
    while True:
        lines = f.readlines(MAX_BYTES_PER_CHUNK)
        if not lines:
            break
        log("Reading %d bytes from input file - %d rows" % (MAX_BYTES_PER_CHUNK, len(lines)), level='DEBUG')
        bulk_row_insert=[]
        for line in lines:
            data = line.strip().split(delimiter)
            bulk_row_insert.append(data + [args.country] + [load_date_now])
        log('Bulk inserting %d rows' % len(bulk_row_insert), level='DEBUG')
        executemany_sql(bulk_row_insert)
    
    # # Uncomment this block for sequential insert
    # for row in f.readlines():
    #     data = row.strip().split(delimiter)
    #     log('Inserting data %s' % data)
    #     sql_query = "insert into %s values (%s, '%s', to_date('%s', 'yyyy/mm/dd hh24:mi:ss'))" % (args.target, ','.join("'{}'".format(x) for x in data), args.country, load_date)
    #     execute_sql(sql_query)

if __name__ == "__main__":
    try:
        dsn = cx_Oracle.makedsn(DB_HOST, 1521, sid="orcl")
        connection = cx_Oracle.connect(DB_USER, DB_PASSWORD, dsn, encoding="UTF-8")
    except Exception as e:
        log(str(e), level='ERROR')
        raise e

    if args.empty_target:
        log('Empty target flag passed in. Deleting all rows from target DB %s' % args.target)
        empty_target()
    import_data()

    # This block below prints all rows from <target>. It's mostly for debugging and can be removed if not needed.
    print("\nData in %s" % args.target)
    print("+-----------------")
    curs = connection.cursor()
    curs.execute('SELECT * FROM %s' % args.target)
    for row in curs.fetchall():
        for elem in row:
            print(str(elem).ljust(2), end='')
        print()