import glob
import logging
import json
import sys
import re
import os
import argparse
import datetime
from string import ascii_uppercase
import cx_Oracle
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell
import openpyxl
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument("json_file_path", help="Relative path to file or file glob of json data import files")
parser.add_argument("--executesql", help="`json_file_path` arg will be parsed as an ExecuteSQL script.", action="store_true")
parser.add_argument("--selectall", help="Print all rows in table", action="store_true")
parser.add_argument("--bootstrap", help="Bootstrap db with provided .sql file. WARNING this will drop the table first.")
parser.add_argument("--loglevel", help="Log level. Allowed values are [DEBUG, INFO, WARN, ERROR]. Defaults to INFO", default="INFO")
args = parser.parse_args()


loglevel_mapping = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARN': logging.WARN,
    'ERROR': logging.ERROR
}
log_format = "%(asctime)s %(levelname)-8s %(message)s"
logging.basicConfig(
    format=log_format,
    level=loglevel_mapping[args.loglevel],
    datefmt='%Y-%m-%d %H:%M:%S'
)

logs = logging.getLogger(__name__)
date_strftime_fmt = '%m/%d/%Y %H:%M:%S'
oracle_strftime_fmt = 'MM/DD/YY HH24:MI:SS'
script_execution_time = datetime.datetime.now()
script_execution_time_str = script_execution_time.strftime(date_strftime_fmt)

def set_error_log_handler(filename="error"):
    global logs
    logs.info('Setting error log handler - %s.log' % filename)
    err_file = logging.FileHandler('%s.log' % filename, delay=True)
    err_file.setLevel(logging.ERROR)
    err_file.setFormatter(logging.Formatter(log_format))
    logs.handlers = []
    logs.addHandler(err_file)

def execute_sql(connection, query, commit=True, rollback_transaction=False):
    logs.debug('Executing SQL query="%s"' % query)
    curs = connection.cursor()
    try:
        curs.execute(query)
    except Exception as e:
        if rollback_transaction:
            logs.error('Failed query. Rolling back transactions.')
            connection.rollback()
        raise e
    if commit:
        logs.debug('Committing SQL Transaction.')
        connection.commit()
    pass

def executemany_sql(connection, db_table, fields, rows, commit=True):
    inserts = '\n'.join(['into %s(%s) values(%s)' % (db_table, ','.join(fields), row) for row in rows])
    sql_query = """insert all %s SELECT * FROM dual""" % inserts
    logs.debug('Bulk SQL query="%s"' % sql_query)
    try:
        curs = connection.cursor()
        curs.execute(sql_query)
        connection.commit()
    except cx_Oracle.DatabaseError as e:
        errorObj, = e.args
        logs.error("Row %d has error %s" % (curs.rowcount, errorObj.message))
        raise e

def execute_select_sql(connection, query):
    """
    This function will execute the provided `query` and return the results as a list of rows.
    The first row in the results list will be the column names.
    """
    logs.debug('Executing SQL query="%s"' % query)
    curs = connection.cursor()
    curs.execute(query)
    column_alias = [c[0] for c in curs.description]
    rows = [list(c) for c in curs.fetchall()]
    rows.insert(0, column_alias)
    return rows

def select_all(connection, db_table):
    query = 'SELECT * FROM %s' % db_table
    rows = execute_select_sql(connection, query)
    headers = rows.pop(0)
    for header in headers:
        print('%s ' % str(header).ljust(2), end='')
    print('-------------------------------------------------')
    for row in rows:
        for elem in row:
            print('%s ' % str(elem).ljust(2), end='')
        print()

def create_db_connection(db_host, db_port, db_user, db_pass, db_service, db_encoding):
    logs.info("Connecting to database %s@%s" % (db_user, db_host))
    dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_service)
    connection = cx_Oracle.connect(db_user, db_pass, dsn, encoding=db_encoding)
    return connection

def convert_xlsx_to_csv(filename):
    xlsx = openpyxl.load_workbook(filename)
    sheet = xlsx.active
    data = sheet.rows
    temp_dir = tempfile.mkdtemp()
    csv_file = os.path.join(temp_dir, "%s.csv" % os.path.basename(filename).split('.')[0])
    csv = open(csv_file, "w+")

    for i, row in enumerate(data):
        if sheet.row_dimensions[i+1].hidden:
            continue

        l = list(row)
        for i in range(len(l)):
            cell_value = str(l[i].value) if str(l[i].value) != 'None' else ' '
            if ',' in cell_value:
                cell_value = '"%s"' % cell_value
            if i == len(l) - 1:
                csv.write(cell_value)
            else:
                csv.write(cell_value + ',')
        csv.write('\n')
    return csv.name

def map_mapping_types(row, mapping_list, row_num=None, source_filename=None, header=True, header_values=None):
    row += [''] * (len(header_values) - len(row))
    row_string = ''
    for i, elem in enumerate(mapping_list):
        quoted = True
        e = elem.get('Type')
        if e.startswith('DBF-'):
            cell_value = e.replace('DBF-', '')
            quoted = False
        elif e.startswith('C-'):
            cell_value = e.replace('C-', '')
        elif e == 'S-datetime.now()':
            cell_value = "TO_DATE('%s', '%s')" % (script_execution_time_str, oracle_strftime_fmt)
            quoted = False
        elif e.startswith('S-FILENAME'):
            _, i, e = tuple(e.split('.'))
            cell_value = os.path.basename(source_filename)[int(i)-1:int(e)]
        elif e == 'S-ROWNUM':
            cell_value = row_num
        else: # Default to 'S' column type.
            if header:
                source_column = int(header_values.index(elem['Source']))
            else:
                source_column = i
            cell_value = str(row[source_column]).replace("'", "''")

        # Check for db_mapping parameter
        if elem.get('DB_CONVERSION'):
            cell_value = elem['DB_CONVERSION'].replace('?', cell_value)
            quoted=False
        if quoted:
            row_string = "%s,'%s'" % (row_string, cell_value)
        else:
            row_string = "%s,%s" % (row_string, cell_value)
    return row_string.lstrip(',')

def process_descriptor_file(descriptor_file):
    with open(descriptor_file, 'r') as fp:
        descriptor_file_data = json.load(fp)
        if 'FileName' in descriptor_file_data['TargetInfo']:
            process_export(descriptor_file)
        if 'DBServer' in descriptor_file_data['TargetInfo']:
            process_import(descriptor_file)

def process_export(descriptor_file):
    logs.info('Processing export for %s' % descriptor_file)
    with open(descriptor_file, 'r') as fp:
        descriptor_file_data = json.load(fp)
        if 'SourceInfo' in descriptor_file_data:
            db_host = descriptor_file_data['SourceInfo']['DBServer']
            db_port = int(descriptor_file_data['SourceInfo']['DBPort'])
            db_schema = descriptor_file_data['SourceInfo']['Schema']
            db_service = descriptor_file_data['SourceInfo']['DBService']
            db_user = descriptor_file_data['SourceInfo']['UserName']
            db_pass = descriptor_file_data['SourceInfo']['PassWord']
            db_sql_query = descriptor_file_data['SourceInfo']['SQL']
            db_encoding = descriptor_file_data['SourceInfo'].get('DBEncoding', 'UTF-8')
            db_filename_prefix_col = descriptor_file_data['SourceInfo'].get('FileNamePrefixColName')
        if 'TargetInfo' in descriptor_file_data:
            source_location = descriptor_file_data['TargetInfo']['Location']
            source_file = descriptor_file_data['TargetInfo']['FileName']
            date_format = descriptor_file_data['TargetInfo'].get('DateFmt', '{:%m/%d/%y %H:%M:%S}')
            unlocked_columns = descriptor_file_data['TargetInfo'].get('UnlockedColumns', [])
    connection = create_db_connection(db_host, db_port, db_user, db_pass, db_service, db_encoding)
    sorted_col_mappings = sorted(descriptor_file_data['ColMappings'], key=lambda k: int(k.get('Order') or sys.maxsize))

    data = execute_select_sql(connection, db_sql_query.replace('~', '"'))
    headers = data.pop(0)

    file_prefixes_unique = [''] # Single element as empty string so the for-loop will process once with no prefix
    if db_filename_prefix_col:
        filename_prefix_index = headers.index(db_filename_prefix_col)
        used=set()
        file_prefixes = [row[filename_prefix_index] for row in data if row]
        file_prefixes_unique = [x for x in file_prefixes if x not in used and (used.add(x) or True)]

    logs.debug('File prefixes determined: %s' % file_prefixes_unique)
    workbooks = []
    for file_prefix in file_prefixes_unique:
        if db_filename_prefix_col:
            workbook_name = '%s-%s' % (file_prefix, source_file)
        else:
            workbook_name = source_file
        logs.info('Opening %s for writing' % os.path.join(source_location, workbook_name))
        workbook = xlsxwriter.Workbook(os.path.join(source_location, workbook_name))
        locked = workbook.add_format()
        locked.set_locked(True)

        unlocked = workbook.add_format()
        unlocked.set_locked(False)
        workbooks.append({
            'workbook': workbook,
            'name': os.path.join(source_location, workbook_name),
            'prefix': file_prefix
        })
        worksheet = workbook.add_worksheet()
        worksheet.protect()
        header_format = workbook.add_format({
            'border': 1,
            'bg_color': '#C6EFCE',
            'bold': True,
            'text_wrap': True,
            'valign': 'vcenter',
            'indent': 1
        })
        header_format.set_locked(False)
        # Set column width based on header size
        worksheet.write_row('A1', headers, header_format)
        for i in range(len(headers)):
            worksheet.set_column(i, i, len(headers[i]) + 5)

        nrow = 1 # Start at one, skip header row
        for row in data:
            if db_filename_prefix_col and (row[filename_prefix_index] != file_prefix):
                # Move on to next file prefix
                continue
            logs.debug('Inserting row %s into spreadsheet' % row)
            ncolumn = 0
            for item in row:
                if isinstance(item, datetime.datetime):
                    item = date_format.format(item)
                if headers[ncolumn] in unlocked_columns:
                    logs.debug('Writing unlocked cell in column %d[%s]' % (ncolumn, headers[ncolumn]))
                    worksheet.write(nrow, ncolumn, item, unlocked)
                else:
                    logs.debug('Writing locked cell in column %d[%s]' % (ncolumn, headers[ncolumn]))
                    worksheet.write(nrow, ncolumn, item, locked)
                ncolumn += 1
            nrow += 1

        # Hide filename prefix column - if any
        if db_filename_prefix_col:
            worksheet.set_column(headers.index(db_filename_prefix_col), headers.index(db_filename_prefix_col), None, None, {'hidden': True})

    # Iterate through all workbooks. Create Dropdown lists if needed.
    for workbook in workbooks:
        prefix = workbook['prefix']
        name = workbook['name']
        workbook = workbook['workbook']
        hidden_row_counter = 1
        worksheet = workbook.worksheets()[0]
        for col_mapping in sorted_col_mappings:
            if 'DDList' in col_mapping and col_mapping['DDList'].lower() in {'yes', 'true'}:
                logs.info('Applying dropdown list validation on column "%s" in workbook %s' % (col_mapping['Source'], name))
                ddlist_query = col_mapping['DDListSQL']
                if '^' in ddlist_query:
                    ddlist_query = ddlist_query.replace('^', "WHERE %s = '%s'" % (db_filename_prefix_col, prefix))
                unique_elements_in_column = execute_select_sql(connection, ddlist_query)[1:] # Skipping header row with [1:]
                unique_elements_in_column = sum([x for x in unique_elements_in_column],[])
                column_index = headers.index(col_mapping['Target'])

                worksheet.write_row(xl_rowcol_to_cell(len(data) + hidden_row_counter, 0), unique_elements_in_column)
                worksheet.data_validation(1, column_index, 1048575, column_index, {
                    'validate': 'list',
                    'source': '=%s:%s' % (xl_rowcol_to_cell(len(data) + hidden_row_counter, 0, row_abs=True, col_abs=True), xl_rowcol_to_cell(len(data) + hidden_row_counter, len(unique_elements_in_column), row_abs=True, col_abs=True))
                })
                worksheet.set_row(len(data) + hidden_row_counter, None, None, {'hidden': True})
                hidden_row_counter += 1

        logs.info('Closing %s.' % os.path.join(source_location, source_file))
        workbook.close()


def process_import(descriptor_file):
    # descriptor_file arg should be a relative path to a .json
    # file with all the required info to handle the data import.
    logs.info('Processing import for %s' % descriptor_file)
    with open(descriptor_file, 'r') as fp:
        descriptor_file_data = json.load(fp)
        if 'TargetInfo' in descriptor_file_data:
            db_host = descriptor_file_data['TargetInfo']['DBServer']
            db_port = int(descriptor_file_data['TargetInfo']['DBPort'])
            db_schema = descriptor_file_data['TargetInfo']['Schema']
            db_service = descriptor_file_data['TargetInfo']['DBService']
            db_user = descriptor_file_data['TargetInfo']['UserName']
            db_pass = descriptor_file_data['TargetInfo']['PassWord']
            db_table = descriptor_file_data['TargetInfo'].get('TableName')
            db_encoding = descriptor_file_data['TargetInfo'].get('DBEncoding', 'UTF-8')
        if 'SourceInfo' in descriptor_file_data:
            source_file = descriptor_file_data['SourceInfo']['Location']
            source_delimeter = descriptor_file_data['SourceInfo'].get('Delimiter', ',')
            source_filetype = descriptor_file_data['SourceInfo']['FileType']
            source_fileheader = True if descriptor_file_data['SourceInfo']['FileHeader'].lower() == 'yes' else False
            MAX_BYTES_PER_CHUNK = descriptor_file_data['MaxBytesPerChunk']

    connection = create_db_connection(db_host, db_port, db_user, db_pass, db_service, db_encoding)
    if args.bootstrap:
        if not db_table:
            logs.error('TargetInfo.TableName not provided in JSON descriptor file.')
            return
        with open(args.bootstrap, 'r') as fp:
            try:
                execute_sql(connection, "DROP TABLE %s.%s" % (db_schema, db_table))
            except:
                pass
            for sql_command in fp.read().split(';'):
                execute_sql(connection, sql_command)
    if args.selectall:
        if not db_table:
            logs.error('TargetInfo.TableName not provided in JSON descriptor file.')
            return
        select_all(connection, db_table)
        return

    if args.executesql:
        logs.info("Running 'ExecuteSQL' method on %s" % descriptor_file)
        set_error_log_handler('{filename}-{date:%Y-%m-%d_%H-%M-%S}'.format(
            filename='executesql-errors',
            date=script_execution_time
        ))
        sorted_sql_statements = sorted(descriptor_file_data['SQLStatements'], key=lambda k: int(k.get('Order') or sys.maxsize))
        for sql in sorted_sql_statements:
            commit = sql is sorted_sql_statements[-1]
            if sql.get('SQLSource', 'inline').lower() == 'inline':
                execute_sql(connection, sql['SQL'].rstrip(';'), commit=commit, rollback_transaction=True)
            else:
                sql_source_file = os.path.join(os.path.dirname(__file__), sql['SQLSourceFile'])
                logs.info("Processing SQLSourceFile %s" % sql_source_file)
                with open(sql_source_file, 'r') as fp:
                    file_sql_statements = fp.read().split(';')

                    # If the sql file ends in a ';' we will end up with an empty string SQL query so we're removing that here
                    try:
                        file_sql_statements.remove('')
                    except ValueError:
                        pass
                    for sql in file_sql_statements:
                        commit = sql is file_sql_statements[-1]
                        execute_sql(connection, sql.strip('\n'), commit=commit, rollback_transaction=True)
        return

    import_file_path = os.path.join(os.path.dirname(__file__), source_file)
    for import_file in glob.glob(import_file_path):
        logs.info("Processing %s" % import_file)
        set_error_log_handler('{filename}-{date:%Y-%m-%d_%H-%M-%S}'.format(
            filename=os.path.basename(import_file).split('.')[0],
            date=script_execution_time
        ))
        if source_filetype == 'xlsx':
            logs.info('Converting %s to temporary csv' % import_file)
            import_file = convert_xlsx_to_csv(import_file)
            logs.info('Temporary csv created - %s' % import_file)

        sorted_col_mappings = sorted(descriptor_file_data['ColMappings'], key=lambda k: int(k.get('Order') or sys.maxsize))
        header_values = None
        regex_pattern = re.compile(r'''%s(?=(?:[^"]*"[^"]*")*[^"]*$)''' % source_delimeter)
        with open(import_file, 'r') as f:
            if source_fileheader:
                header_values = next(f)
                header_values = regex_pattern.split(header_values.strip())
                header_values = ['' if i == ' ' else i for i in header_values]
            row_counter=1
            while True:
                row_counter_start=row_counter
                bulk_row_insert = []
                lines = f.readlines(MAX_BYTES_PER_CHUNK)
                if not lines:
                    break
                logs.info("Reading %d bytes from input file - %d row(s)" % (MAX_BYTES_PER_CHUNK, len(lines)))
                for line in lines:
                    data = regex_pattern.split(line.strip())
                    data = ['' if i == ' ' else i for i in data]
                    bulk_row_insert.append(map_mapping_types(data, sorted_col_mappings, row_num=row_counter, source_filename=import_file, header=source_fileheader, header_values=header_values))
                    row_counter += 1
                logs.info('Bulk inserting rows %d-%d' % (row_counter_start, row_counter-1))
                try:
                    executemany_sql(connection, db_table, [x['Target'] for x in sorted_col_mappings], bulk_row_insert)
                except cx_Oracle.DatabaseError as e:
                    pass

if __name__ == "__main__":
    for desc_file in glob.glob(args.json_file_path):
        process_descriptor_file(desc_file)
