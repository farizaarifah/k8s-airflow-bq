import psycopg2
import pandas as pd
import numpy as np
import optparse
import numpy as np
import petl as etl
import pandas_gbq
import datetime as dt
import os
import pytz
import sys

from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dependencies import db_config


# Tabulate
pd.options.display.max_colwidth = 100000

parser = optparse.OptionParser(usage="usage: %prog [options]")
parser.add_option('--table', dest='table', help='specify table source')
parser.add_option('--db', dest='db', help='specify database source')
parser.add_option('--schema', dest='schema', help='specify schema source')
parser.add_option('--dataset', dest='dataset', help='specify dataset source')

(options, args) = parser.parse_args()

if not options.table:
    parser.error('table is not given')
if not options.db:
    parser.error('database is not given')
if not options.schema:
    parser.error('schema is not given')
if not options.dataset:
    parser.error('dataset is not given')

table = options.table
db = options.db
schema = options.schema
dataset = options.dataset


def get_count(conn, schema, table, db_name):
    # TODO: Ini juga perlu kita sederhanakan logic-nya.
    # Di sini untuk p2p perlu pakai db_name juga.
    if (db != 'hijra_staging' and table in ['audit_trail','log_login','anl_user_register','user_lounges','rdl_api_log']) == True:
#         if db == 'hijra' and table in ['application_activity']:
#             sql = "SELECT COUNT(*) FROM {}.{} WHERE DATE(insert_date) >= (CURRENT_DATE - INTERVAL '1 DAY')".format(schema,table)
        if db_name == 'p2p_prod' and table in ['rdl_api_log']:
            sql = "SELECT COUNT(*) FROM {}.{} WHERE DATE(created_date) >= (CURRENT_DATE - INTERVAL '2 DAY')".format(schema,table)
        if db == 'hijra' and table in ['anl_user_register']:
            sql = "SELECT COUNT(*) FROM {}.{} WHERE id != 7934".format(schema,table)
        if db == 'hijra' and table in ['user_lounges']:
            sql = "SELECT COUNT(*) FROM {}.{} WHERE id != 7534".format(schema,table)
        if db_name == 'p2p_prod' and table in ['log_login']:
            sql = "SELECT COUNT(*) FROM {}.{} WHERE DATE(log_date) >= (CURRENT_DATE - INTERVAL '2 DAY')".format(schema,table)
        if db_name == 'p2p_prod' and table in ['audit_trail','application_activity']:
            sql = "SELECT COUNT(*) FROM {}.{} WHERE DATE(activity_date) >= (CURRENT_DATE - INTERVAL '5 DAY')".format(schema,table)
    else:
        sql = "SELECT COUNT(*) FROM {}.{}".format(schema,table)

    df = pd.read_sql_query(sql, conn)
    count = int(str(df['count'].values).replace('[','').replace(']',''))

    return count


def get_data(conn, db, dataset, schema, table, db_name):
    # Object client bigquery cursor
    client = bigquery.Client()
    client.query("""DROP TABLE IF EXISTS {dataset}.{table}_temp""".format(dataset=dataset,table=table)).result()

    cursor = conn.cursor(name='fetch_large_result')

    # TODO: ini harus direfactor supaya clear.
    # Untuk sementara buat db2 p2p exact pakai db_name dulu saja karena ada beberapa entity dengan db_name sama.
    if db == 'hijra' and table in ['anl_user_register', 'user_lounges']:
        if table in ['anl_user_register']:
            cursor.execute("SELECT * FROM {}.{} WHERE id != 7934".format(schema,table))
        elif table in ['user_lounges']:
            cursor.execute("SELECT * FROM {}.{} WHERE id != 7534".format(schema,table))
    elif db_name == 'p2p_prod' and table in ['rdl_api_log', 'log_login', 'audit_trail']:
        if table in ['rdl_api_log']:
            cursor.execute("SELECT * FROM {}.{} WHERE DATE(created_date) >= (CURRENT_DATE - INTERVAL '2 DAY')".format(schema,table))
        elif table in ['log_login']:
            cursor.execute("SELECT * FROM {}.{} WHERE DATE(log_date) >= (CURRENT_DATE - INTERVAL '2 DAY')".format(schema,table))
        elif table in ['audit_trail']:
            cursor.execute("SELECT * FROM {}.{} WHERE DATE(activity_date) >= (CURRENT_DATE - INTERVAL '5 DAY')".format(schema,table))
    elif db == 'metabase' and table in ['query_execution', 'query']:
        if db == 'metabase' and table in ['query_execution']:
            cursor.execute("SELECT id, encode(hash, 'hex') as hash, started_at, running_time, result_rows, native, context, error, executor_id, card_id, dashboard_id, pulse_id, database_id, cache_hit, action_id, is_sandboxed, encode(cache_hash, 'hex') as cache_hash FROM {}.{}".format(schema,table))
        elif db == 'metabase' and table in ['query']:
            cursor.execute("SELECT encode(query_hash, 'hex') AS query_hash, average_execution_time, query FROM {}.{}".format(schema,table))
    else:
        cursor.execute('SELECT * FROM {}.{}'.format(schema,table))

    while True:
        if table in ['fcm_notification_request','email_bulk','notification2','application_activity','audit_trail','log_login']:
            if db == 'hijra' and table in ['application_activity']:
                # consume result over a series of iterations
                records = cursor.fetchmany(size=100000)
                columns = [column[0] for column in cursor.description]

            else:
                # consume result over a series of iterations
                records = cursor.fetchmany(size=150000)
                columns = [column[0] for column in cursor.description]

        else:
            # consume result over a series of iterations
            records = cursor.fetchmany(size=10000000)
            columns = [column[0] for column in cursor.description]

        if not records:
            break

        results = []
        for row in records:
            results.append(dict(zip(columns, row)))

        print(sys.getsizeof(results))

        df = pd.DataFrame(results)
        df = df.applymap(lambda x: " ".join(x.splitlines()) if isinstance(x, str) else x)
        df = df.astype('object')

        if table == 'bank_account' and schema == 'alamisharia':
            df['owner_name'] = df['owner_name'].str.strip('\r')

        # df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Create and insert temp table
        pandas_gbq.to_gbq(df, dataset + '.' + table + '_temp', project_id='alami-group-data',if_exists='append',api_method='load_csv')

    cursor.close()


def partition_exists(dataset: str, table: str) -> bool:
    client = bigquery.Client()
    try:
        table_ref = client.dataset(dataset_id=dataset, project="alami-group-data").table(table_id=table)
        table = client.get_table(table_ref)
        return table.time_partitioning is not None
    except NotFound as e:
        print("TABLE NOT FOUND: ", dataset, table)
        return False


def transform_table(conn, table, count, schema, dataset, db, db_name):
    # Object client bigquery cursor
    client = bigquery.Client()
    print(db, db_name, schema, table)
    # Load table schema
    query_col_desc = """SELECT CASE
                               WHEN udt_name IN ('int4','int8','int2') THEN CONCAT('CAST(`',COLUMN_NAME,'` AS FLOAT64)')
                               ELSE CONCAT('`',COLUMN_NAME,'`')
                           END AS COLUMN_NAME,
                           CONCAT('`',COLUMN_NAME,'`') as COLUMN_NAMES,
                           CASE
                               WHEN COLUMN_NAME IN ('logo_web') THEN 'STRING'
                               WHEN column_name IN ('bni_trx_id_va') THEN 'NUMERIC'
                               WHEN udt_name IN ('int8',
                                                 'int4',
                                                 'int2') THEN 'INT64'
                               WHEN udt_name IN ('bytea') THEN 'STRING'
                               WHEN udt_name IN ('varchar',
                                                 'text','bpchar','jsonb','json','uuid','UUID','_text') THEN 'STRING'
                               WHEN udt_name IN ('float8','float4','numeric') THEN 'FLOAT64'
                               WHEN udt_name IN ('timestamptz','timestamp') THEN 'TIMESTAMP'
                               ELSE UPPER(udt_name)
                           END AS data_type,
                           col_description('{schema}.{source_table}'::regclass, ordinal_position) as description
                        from information_schema.columns
                        where column_name NOT IN ('log_data','call_to_action') and table_schema = '{schema}' and table_name = '{source_table}'""".format(schema=schema,source_table=table)

    column = etl.fromdb(conn, query_col_desc)
    df_schema_exist = pd.DataFrame(etl.todataframe(column))
    df_schema = df_schema_exist.replace('None', '')
    df_schema.replace(to_replace=[None], value=np.nan, inplace=True)
    df_schema.fillna(value='', inplace=True)
    partition_columns = df_schema['column_names'][df_schema['data_type'].isin(['TIMESTAMP','DATE'])].head(1).to_string(index=False)
    print(partition_columns)

    # Create sql syntax
    column = ', CAST('+ df_schema.column_name + ' AS ' + df_schema.data_type + ') AS ' + df_schema.column_names
    rm = column[0]
    rm = rm[2 : : ]
    column = column.drop(labels=0)
    #column = column.drop(['pre_audit_trail', 'after_audit_trail'], axis = 1)
    column = column.append(pd.Series(rm), ignore_index=False)
    column_table = pd.DataFrame(column).sort_index()
    column_table = column_table.to_string(header=False,index=False)

    # Load table schema
    query_col_desc = """select CONCAT('`',column_name,'`') AS column_name, column_name as column_names, CASE
                                    WHEN column_name IN ('logo_web') THEN 'STRING'
                                    WHEN column_name IN ('bni_trx_id_va') THEN 'NUMERIC'
                                    WHEN udt_name IN ('int8','int4','int2') THEN 'INT64'
                                    WHEN udt_name IN ('bytea') THEN 'STRING'
                                    WHEN udt_name IN ('varchar','text','bpchar','jsonb','json','uuid','UUID','_text') THEN 'STRING'
                                    WHEN udt_name IN ('float8','float4','numeric') THEN 'FLOAT64'
                                    WHEN udt_name IN ('timestamptz','timestamp') THEN 'TIMESTAMP'
                                    ELSE UPPER(udt_name)
                               END AS data_type, col_description('{schema}.{source_table}'::regclass, ordinal_position) as description
                        from information_schema.columns
                        where column_name NOT IN ('log_data','call_to_action') and table_schema = '{schema}' and table_name = '{source_table}'""".format(schema=schema,source_table=table)

    column = etl.fromdb(conn, query_col_desc)
    df_schema_exist = pd.DataFrame(etl.todataframe(column))
    df_schema = df_schema_exist.replace('None', '')
    df_schema.replace(to_replace=[None], value=np.nan, inplace=True)
    df_schema.fillna(value='', inplace=True)


    columns2 = ', '+ df_schema.column_name + ' STRING'
    rm2 = columns2[0]
    rm2 = rm2[2 : : ]
    columns2 = columns2.drop(labels=0)
    columns2 = columns2.append(pd.Series(rm2), ignore_index=False)
    column_lists2 = pd.DataFrame(columns2).sort_index()
    column_lists2 = column_lists2.to_string(header=False,index=False)

    
    # List column to create
    columns = ', '+ df_schema.column_name
    rm = columns[0]
    rm = rm[2 : : ]
    columns = columns.drop(labels=0)
    columns = columns.append(pd.Series(rm), ignore_index=False)
    column_lists = pd.DataFrame(columns).sort_index()
    column_lists = column_lists.to_string(header=False,index=False)
    
    # ini logic untuk append data, kalau p2p_prod dan 3 table ini hapus dulu id yang udah masuknya
    if db_name == "p2p_prod" and table in ['audit_trail','log_login','rdl_api_log','fdc_monitor_api_log']:
        print(db, table, '1')
        if table in ['audit_trail','log_login','rdl_api_log']:
            client.query("""DELETE FROM {dataset}.{table}
                            WHERE id IN (SELECT DISTINCT id
                                FROM {dataset}.{table}_temp)""".format(dataset=dataset,table=table,column_table=column_table)).result()

        client.query("""INSERT {dataset}.{table} ({column_list})
                        SELECT {column_table}
                        FROM {dataset}.{table}_temp""".format(dataset=dataset,table=table,column_list=column_lists,column_table=column_table)).result()
    
    elif db_name == 'colms_prod':
        try:
            # CREATE TABLE
            # print('creating table')
            query = """
            CREATE OR REPLACE TABLE {dataset}.datalake__{db_name}__{schema}__{table}__prod ({column_lists2})
            """.format(dataset=dataset, db_name=db_name, table=table, schema=schema, column_lists2=column_lists2)
            client.query(query).result()
            
            try:
                # INSERT TABLE
                cursor = conn.cursor(name='fetch_large_result')
                sql = "SELECT * FROM {}.{}".format(schema,table)
                cursor.execute(sql)
                records = cursor.fetchmany(size=100000)
                columns = [column[0] for column in cursor.description]
                results = []
                for row in records:
                    results.append(dict(zip(columns, row)))
            
                print(sys.getsizeof(results))
            
                df = pd.DataFrame(results)
                df = df.applymap(lambda x: " ".join(x.splitlines()) if isinstance(x, str) else x)
                # print(df['obligor_id'])
                # df = df.replace(np.nan, '', regex=True)
                # print(df['obligor_id'])
                df = df.astype('str')    
                df = df.replace('None', '')
                # df = df.fillna('', inplace=True)
        
                tables__ = '{dataset}.datalake__{db_name}__{schema}__{table}__prod'.format(dataset=dataset, db_name=db_name, table=table, schema=schema)
                pandas_gbq.to_gbq(df, tables__, project_id='alami-group-data',if_exists='replace',api_method='load_csv')
            
            except Exception as d:
                print(str(d))
        except Exception as e:
            print(str(e))

    # logic untuk partition column
    elif db != "hijra_staging":
        # these tables not using partition column and add uuid column
        if table in ['pefindo_individual','pefindo_company','pefindo_subject_id']:
            client.query("""CREATE OR REPLACE TABLE {dataset}.{table} AS
                            SELECT GENERATE_UUID() AS uuid, {column_table}
                            FROM {dataset}.{table}_temp""".format(dataset=dataset,table=table,column_table=column_table)).result()
        else:
            if partition_exists(dataset=dataset, table=table) or (count > 100000 and partition_columns != 'Series([], )' and table != 'pefindo_contract_overview'):
                # List column to create
                columns = ', '+ df_schema.column_name + ' ' + df_schema.data_type
                rm = columns[0]
                rm = rm[2 : : ]
                columns = columns.drop(labels=0)
                columns = columns.append(pd.Series(rm), ignore_index=False)
                column_list = pd.DataFrame(columns).sort_index()
                column_list = column_list.to_string(header=False,index=False)

                # Drop main table
                client.query("""DROP TABLE IF EXISTS {dataset}.{table}""".format(dataset=dataset,table=table)).result()

                if not df_schema.loc[df_schema['column_names'] == "timestamp"].empty:
                    partition_column = "timestamp"
                if not df_schema.loc[df_schema['column_names'] == "create_date"].empty:
                    partition_column = "create_date"
                if not df_schema.loc[df_schema['column_names'] == "created_at"].empty:
                    partition_column = "created_at"
                if not df_schema.loc[df_schema['column_names'] == "added_at"].empty:
                    partition_column = "added_at"
                if not df_schema.loc[df_schema['column_names'] == "insert_date"].empty:
                    partition_column = "insert_date"
                if not df_schema.loc[df_schema['column_names'] == "created_date"].empty:
                    partition_column = "created_date"
                if not df_schema.loc[df_schema['column_names'] == "log_date"].empty:
                    partition_column = "log_date"
                if not df_schema.loc[df_schema['column_names'] == "activity_date"].empty:
                    partition_column = "activity_date"
                if not df_schema.loc[df_schema['column_names'] == "transaction_date"].empty:
                    partition_column = "transaction_date"
                if not df_schema.loc[df_schema['column_names'] == "date_snapshot"].empty:
                    partition_column = "date_snapshot"
                if not df_schema.loc[df_schema['column_names'] == "dateofinquiry"].empty:
                    partition_column = "dateofinquiry"
                if not df_schema.loc[df_schema['column_names'] == "amount_booked_date"].empty:
                    partition_column = "amount_booked_date"
                if not df_schema.loc[df_schema['column_names'] == "date_uuid"].empty:
                    partition_column = "date_uuid"
                if not df_schema.loc[df_schema['column_names'] == "creationDate"].empty:
                    partition_column = "creationDate"
                try:
                    partition_column
                except:
                    partition_column = partition_columns

                try:
                    # Create main table
                    client.query("""CREATE TABLE {dataset}.{table} (
                                        {column_list}
                                    )
                                    PARTITION BY
                                        DATE({partition_column})
                                    OPTIONS
                                        (description="a table partitioned by {partition_column}")""".format(dataset=dataset,table=table,column_list=column_list,partition_column=partition_column)).result()
                except:
                    # Create main table
                    client.query("""CREATE TABLE {dataset}.{table} (
                                        {column_list}
                                    )
                                    PARTITION BY
                                        {partition_column}
                                    OPTIONS
                                        (description="a table partitioned by {partition_column}")""".format(dataset=dataset,table=table,column_list=column_list,partition_column=partition_column)).result()

                # Insert into main table
                client.query("""INSERT {dataset}.{table}
                                SELECT {column_table}
                                FROM {dataset}.{table}_temp""".format(dataset=dataset,table=table, column_table=column_table)).result()

            else:
                client.query("""CREATE OR REPLACE TABLE {dataset}.{table} AS
                                SELECT {column_table}
                                FROM {dataset}.{table}_temp""".format(dataset=dataset,table=table,column_table=column_table)).result()

    # Drop temp table
    client.query("""DROP TABLE {dataset}.{table}_temp""".format(dataset=dataset,table=table)).result()


def create_table(conn, db, dataset, schema, table):
    # Object client bigquery cursor
    client = bigquery.Client()

    # Load table schema
    query_col_desc = """select CONCAT('`',column_name,'`') AS column_name, CASE
                                    WHEN column_name IN ('logo_web') THEN 'STRING'
                                    WHEN column_name IN ('bni_trx_id_va') THEN 'NUMERIC'
                                    WHEN udt_name IN ('int8','int4','int2') THEN 'INT64'
                                    WHEN udt_name IN ('bytea') THEN 'STRING'
                                    WHEN udt_name IN ('varchar','text','bpchar','jsonb','json','uuid','UUID','_text') THEN 'STRING'
                                    WHEN udt_name IN ('float8','float4','numeric') THEN 'FLOAT64'
                                    WHEN udt_name IN ('timestamptz','timestamp') THEN 'TIMESTAMP'
                                    ELSE UPPER(udt_name)
                               END AS data_type, col_description('{schema}.{source_table}'::regclass, ordinal_position) as description
                        from information_schema.columns
                        where column_name NOT IN ('log_data','call_to_action') and table_schema = '{schema}' and table_name = '{source_table}'""".format(schema=schema,source_table=table)

    column = etl.fromdb(conn, query_col_desc)
    df_schema_exist = pd.DataFrame(etl.todataframe(column))
    df_schema = df_schema_exist.replace('None', '')
    df_schema.replace(to_replace=[None], value=np.nan, inplace=True)
    df_schema.fillna(value='', inplace=True)

    # List column to create
    columns = ', '+ df_schema.column_name + ' ' + df_schema.data_type
    rm = columns[0]
    rm = rm[2 : : ]
    columns = columns.drop(labels=0)

    #column = column.drop(['pre_audit_trail', 'after_audit_trail'], axis = 1)
    columns = columns.append(pd.Series(rm), ignore_index=False)
    column_list = pd.DataFrame(columns).sort_index()
    column_list = column_list.to_string(header=False,index=False)
    print(column_list)
    # Create main table
    if dataset=="datalakes":
        query="""
        CREATE OR REPLACE TABLE {dataset}.datalake__{db}__{schema}__{table}__prod ({column_list})
        """.format(db=db,schema=schema,dataset=dataset,table=table,column_list=column_list)
        
    else:
        query="""
        CREATE OR REPLACE TABLE {dataset}.{table} ({column_list})
        """.format(dataset=dataset,table=table,column_list=column_list)

    client.query(query).result()

def update_desc(conn, table, schema, dataset, db):
    # Object client bigquery cursor
    client = bigquery.Client()

    # Load table schema
    query_col_desc = """select column_name, CASE
                                    WHEN column_name IN ('logo_web') THEN 'STRING'
                                    WHEN column_name IN ('bni_trx_id_va') THEN 'NUMERIC'
                                    WHEN udt_name IN ('int8','int4','int2') THEN 'INT64'
                                    WHEN udt_name IN ('bytea') THEN 'STRING'
                                    WHEN udt_name IN ('varchar','text','bpchar','jsonb','json','uuid','UUID','_text') THEN 'STRING'
                                    WHEN udt_name IN ('float8','float4','numeric') THEN 'FLOAT64'
                                    WHEN udt_name IN ('timestamptz','timestamp') THEN 'TIMESTAMP'
                                    ELSE UPPER(udt_name)
                               END AS data_type, col_description('{schema}.{source_table}'::regclass, ordinal_position) as description
                        from information_schema.columns
                        where column_name NOT IN ('log_data','call_to_action') and table_schema = '{schema}' and table_name = '{source_table}'""".format(schema=schema,source_table=table)

    column = etl.fromdb(conn, query_col_desc)
    df_schema_exist = pd.DataFrame(etl.todataframe(column))
    if table in ['pefindo_individual','pefindo_company','pefindo_subject_id']:
        df_schema_exist = df_schema_exist.append({'column_name':'uuid','data_type':'STRING','description':'None'}, ignore_index=True)
    df_schema = df_schema_exist.replace('None', '')
    df_schema.replace(to_replace=[None], value=np.nan, inplace=True)
    df_schema.fillna(value='', inplace=True)

    # Create metadata schema for bigquery table
    schema = []
    for i in range(len(df_schema)):
        relaxed = bigquery.SchemaField("{}".format(df_schema['column_name'][i]), "{}".format(df_schema['data_type'][i]), description="{}".format(df_schema['description'][i]))
        i = +1
        schema.append(relaxed)

    # Load table description
    query_tab_desc = """SELECT description
                            FROM pg_Description
                            JOIN pg_Class ON pg_Description.ObjOID = pg_Class.OID
                            WHERE ObjSubID = 0
                              AND relname = '{source_table}'""".format(source_table=table)

    desc = etl.fromdb(conn, query_tab_desc)
    df_tab_desc = pd.DataFrame(etl.todataframe(desc))

    # Update table and column description
    table_ref = client.dataset('{}'.format(dataset)).table(table)
    table = client.get_table(table_ref)
    table.schema = schema
    table = client.update_table(table, ["schema"])
    table.description = df_tab_desc.to_string(header=False,index=False)
    table = client.update_table(table, ["description"])


def audit_trail(db, table, total_rows):
    time = dt.datetime.now(pytz.timezone('Asia/Jakarta'))
    d = {'load_time': time, 'source': 'database', 'database_name': '{}'.format(db), 'table_name': '{}'.format(table), 'rows': '{}'.format(total_rows)}
    df = pd.DataFrame.from_dict([d])
    df = df[['load_time','source','database_name','table_name','rows']]
    df = df.astype({'rows':'int64'})
    pandas_gbq.to_gbq(df, 'audit.audit_trail', project_id='alami-group-data',if_exists='append',api_method='load_csv')


def copy_table(table, schema, dataset, db):
    # Object client bigquery cursor
    client = bigquery.Client()

    if dataset == 'hijra_lake':
        client.query(
            """CREATE OR REPLACE TABLE prod__hijra__{db}__{schema}.{table} CLONE {dataset}.{table}"""
            .format(dataset=dataset,table=table,schema=schema,db=db)
            ).result()

    if dataset == 'p2p_lake':
        client.query(
            """CREATE OR REPLACE TABLE prod__p2p__{db}__{schema}.{table} CLONE {dataset}.{table}"""
            .format(dataset=dataset,table=table,schema=schema,db=db)
                ).result()
        
    if dataset == 'datalakes':
        pass

def main(db, dataset, schema, table):
    # DB connect
    if db == 'colms_prod':
        db_host  = db_config.db_colms_server
        db_username = db_config.db_colms_username
        db_password = db_config.db_colms_password
        db_name = db_config.db_colms_name
        db_port = db_config.db_colms_driver
        
    conn = psycopg2.connect(
        host=db_host,
        user=db_username,
        password=db_password,
        database=db_name,
        port=db_port,
        connect_timeout=5)

    print("Processing: {}: {}.{}".format(db, schema, table))

    count = get_count(conn, schema, table, db_name)
    print(count)
    test = '{db} {schema} {table}'.format(db=db, schema=schema, table=table, dataset=dataset)
    print(test)


    if count != 0:
        if db =='colms_prod':
            get_data(conn, db, dataset, schema, table, db_name)
            transform_table(conn, table, count, schema, dataset, db, db_name)
        else:        
            get_data(conn, db, dataset, schema, table, db_name)
            transform_table(conn, table, count, schema, dataset, db, db_name)
            update_desc(conn, table, schema, dataset, db)
            audit_trail(db, table, count)
            copy_table(table, schema, dataset, db)
    else:
        create_table(conn, db, dataset, schema, table)
        audit_trail(db, table, count)
        copy_table(table, schema, dataset, db)

    return "SUCCESS"


if __name__ == "__main__":
    main(db, dataset, schema, table)
