import pyodbc as py
import libs.db.connectionmanager as cn
import libs.db.Config as Cfg
from sqlalchemy.orm import sessionmaker, aliased, scoped_session
from collections import OrderedDict


# Vertica Connection and methods
class DWAccessLayer:
    def __init__(self, silo_id, app_connection, context=""):
        self._app_connection = app_connection
        self._config = Cfg.Config(app_connection, silo_id)
        self._connection_pool = None
        self._silo_id = silo_id
        self._context = context
        self._arguments = ['dw.user.id', 'dw.user.password', 'dw.server.name', 'dw.database.name',
                           'dw.schema.name', 'dw.backupserver.name']
        self.get_connection()

    # Get the vertica configuration details from RSI_CORE_CFGPROPERTY (Refer Vertica_config.ps1)
    def get_dw_config(self):
        dw_user_id = ''
        dw_user_password = ''
        # Create a empty list and then append all the input filter conditions. If any Context is available
        # then change the user id and password accordingly. Once the the list is ready with all the filter conditions
        # then pass the list to the calling method.
        argument_list = []
        for arg in self._arguments:
            if self._context != '' and arg == 'dw.user.id':
                dw_user_id = 'dw.'+self._context+'user.id'
                arg = dw_user_id
            elif self._context == '' and arg == 'dw.user.id':
                dw_user_id = arg
            if self._context != '' and arg == 'dw.user.password':
                dw_user_password = 'dw.'+self._context+'user.password'
                arg = dw_user_password
            elif self._context == '' and arg == 'dw.user.password':
                dw_user_password = arg
            argument_list.append(arg)
        core_config = self._config.get_config(argument_list)
        # if the user id/password/ the output result string is null then Raise the exception
        if len(core_config) == 0 or dw_user_id not in core_config or dw_user_password not in core_config:
            print('No ' + dw_user_id + ' or ' + dw_user_password + ' for ' + self._silo_id)
            # raise Exception('No ' + self._dw_user_id + 'or ' + self._dw_user_password + 'for ' + self._silo_id)
        return core_config

    # Connect to Vertica based on the configuration
    def get_connection(self):
        vertica_configuration = DWAccessLayer.get_dw_config(self)
        # if the backup server is not specified in the input then it will return False else returns the backup server
        if self._connection_pool is None:
            if vertica_configuration.get('dw.backupserver.name', 'False') == 'False':
                self._connection_pool = py.connect("DRIVER={{Vertica}}; SERVER={0}; DATABASE=Fusion; UID={1}; "
                                                   "PWD={2}; PreferredAddressFamily=none".
                                                   format(vertica_configuration['dw.server.name'],
                                                          vertica_configuration['dw.user.id'],
                                                          vertica_configuration['dw.user.password']))
            else:  # Vertica connection along with the back up server
                self._connection_pool = py.connect("DRIVER={{Vertica}}; SERVER={0}; DATABASE=Fusion; UID={1}; PWD={2}; "
                                                   "PreferredAddressFamily=none; BackupServerNode={3}".
                                                   format(vertica_configuration['dw.server.name'],
                                                          vertica_configuration['dw.user.id'],
                                                          vertica_configuration['dw.user.password'],
                                                          vertica_configuration['dw.backupserver.name']))
        return self._connection_pool

    def set_connection_autocommit(self, commit=True):
        self._connection_pool.autocommit = commit

    def close_connection(self):
        self._connection_pool.close()

    def command_commit(self):
        self._connection_pool.commit()

    def command_rollback(self):
        self._connection_pool.rollback()

    def copy_data_from_app(self, destination, source_query, destination_column_list=None,
                           server_name=None, db_name=None):
        app_connection = self._app_connection
        config = self._config
        app_driver = config["etl.app.driver"]
        app_user = config["etl.app.user.id"]
        app_password = config["etl.app.user.password"]
        if server_name and db_name:
            app_connection = AppAccessLayer(server_name, db_name, app_user, app_password)
            app_database = db_name
        else:
            app_database = config["db.database.name"]

        app_server = app_connection.instance_ip
        app_port = app_connection.instance_port

        connection_string = "Driver={0};UID={1};PWD={2};server={3},{4};database={5}"\
            .format(app_driver, app_user, app_password, app_server, app_port, app_database)
        if destination_column_list:
            destination = destination + "(" + str(destination_column_list) + ")"
        copy_query = "COPY {0} WITH SOURCE ODBCSource() PARSER ODBCLoader(connect='{1}', query='{2}') " \
                     "DIRECT STREAM NAME '{3}' REJECTED DATA AS TABLE {4}".\
            format(destination, connection_string, source_query, "RDP_SQLtoVertica",'REJECTED_ROWS')
        print(copy_query)
        rowcount = self.execute(copy_query, True)
        return rowcount

    # This method will look if the column(s), exists in the specified table or not.
    # If not then it will add the column in that table
    def add_column(self, schema_name, columns):
        c = ''
        # connection = DWAccessLayer.get_connection(self)
        try:
            input_columns = [{k.upper(): v.upper() for k, v in d.items()} for d in columns]
            table_column_list = [{k: v for k, v in c.items() if k != 'DATA_TYPE'} for c in input_columns]
            # convert all the incoming column (list of dict) into frozen set
            incoming_column_set = {frozenset(row.items()) for row in table_column_list}
            # find all the column(s) and their data type from the system table
            table_meta_info_qry = "SELECT A.TABLE_NAME, C.COLUMN_NAME " \
                                  "FROM ALL_TABLES A JOIN COLUMNS C ON C.TABLE_ID = A.TABLE_ID " \
                                  "WHERE A.SCHEMA_NAME = '{SCHEMA_NAME}' " \
                                  "AND A.TABLE_NAME ='{TABLE_NAME}'".format(SCHEMA_NAME=schema_name,
                                                                            TABLE_NAME=columns[0].get("TABLE_NAME"))
            table_meta_info = self.query_with_result(table_meta_info_qry)
            table_meta_info = [{k.upper(): v.upper() for k, v in d.items()} for d in table_meta_info]
            # convert existing column list of dict into frozen set
            existing_column_set = {frozenset(row.items()) for row in table_meta_info}
            # find all the new columns (new as well as change in data type)
            new_column_set = incoming_column_set-existing_column_set
            all_new_column = []
            # convert the frozen set into a list of dict
            for all_columns in new_column_set:
                all_new_column.append((dict(all_columns)))
            # Create a list of dicts with column name and data type
            col_data_type = []
            for new_col in all_new_column:
                for col in input_columns:
                    if all(col[k] == v for k, v in new_col.items()):
                        col_data_type.append(col)
                        break
            # generate the ALTER Statement
            if all_new_column:
                alter_qry = []
                for i in col_data_type:
                    stmt = "ALTER TABLE {SCHEMA_NAME}.{TABLE_NAME} ADD COLUMN \"{COLUMN_NAME}\" {DATA_TYPE}; ". \
                        format(SCHEMA_NAME=schema_name,
                               TABLE_NAME=i['TABLE_NAME'],
                               COLUMN_NAME=i['COLUMN_NAME'],
                               DATA_TYPE=i['DATA_TYPE'])
                    alter_qry.append(stmt)
                # convert the alter query into a string
                alter_stmt = ''.join(alter_qry)
                self.execute(alter_stmt, commit=True)
        except Exception as e:
            raise
        finally:
            del c

    # All the Query () will accept the sql parameter as a SELECT statement. If you pass a DML statement
    # then it will throw an error
    # Returns result set (could be multiple rows)
    def query_with_result(self, sql):
        c = ''
        connection = DWAccessLayer.get_connection(self)
        try:
            c = connection.cursor()
            c.execute(sql)
            columns = [column[0] for column in c.description]
            results = []
            for row in c.fetchall():
                # results.append(OrderedDict(zip(columns, row)))
                results.append(dict(zip(columns, row)))
            return results
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise
        finally:
            del c

        # All the Query () will accept the sql parameter as a SELECT statement. If you pass a DML statement

        # then it will throw an error
        # Returns result set (could be multiple rows)
    def query_cursor(self, sql):
            c = ''
            connection = DWAccessLayer.get_connection(self)
            try:
                c = connection.cursor()
                c.execute(sql)
                return c
            except py.ProgrammingError as e:
                print("failed to execute {}, error: {}".format(sql, e))
                raise
            finally:
                del c

    # Execute the SQL (SELECT) statement and return the rows
    def query_scalar(self, sql):
        c = ''
        connection = DWAccessLayer.get_connection(self)
        try:
            c = connection.cursor()
            c.execute(sql)
            scalar_value = c.fetchone()  # Gives the number of returned rows from the SELECT query
            return scalar_value
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise
        finally:
            del c

    # Execute the SQL (SELECT) statement with the input connection and return the rows
    @staticmethod
    def query_with_connection(connection, sql):
        c = ''
        try:
            c = connection.cursor()
            c.execute(sql)  # Execute the input query
            columns = [column[0] for column in c.description]
            results = []
            for row in c.fetchall():
                results.append(dict(zip(columns, row)))
            return results
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise
        finally:
            del c

    # This method will accept a DML statement and Returns affected count.
    def execute(self, sql, commit=False):
        c = ''
        connection = DWAccessLayer.get_connection(self)
        try:
            c = connection.cursor()
            c.execute(sql)  # Execute the input SQL statement
            affected_count = c.rowcount
            if commit is True:
                connection.commit()
            return affected_count
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise
        finally:
            del c

    # This method can be used to execute scripts configured in META_SQL_SCRIPTS, param_dictionary should contain the
    # list of parameter values that has to be replaced and the run time values as param_name, param_value format.
    # [{param_name:"Event_Key", param_value:"123"}, {
    # def execute_meta_sql_scripts(self, script_name, param_dictionary):
    #     # get the app connection
    #     app_connection = AppAccessLayer()
    #     query_to_execute = ''
    #     try:
    #         meta_sql_query = "SELECT SQL_QUERY, QUERY_ORDER, SQL_TYPE " \
    #                          "FROM META_SQL_SCRIPTS " \
    #                          "WHERE SQL_SCRIPT_NAME = '"+script_name + "' ORDER BY QUERY_ORDER"
    #         # execute the meta_sql_query in the app db (SQL)
    #         meta_queries = app_connection.query_with_result(meta_sql_query)
    #         for query in meta_queries:
    #             # get the sql query which will run on DW
    #             query_to_execute = query['SQL_QUERY']
    #             # get the query type based on that will call the appropriate method
    #             query_type = query['SQL_TYPE']
    #             for params in param_dictionary:
    #                 query_to_execute = query_to_execute.replace(params["param_name"], params["param_value"])
    #             if query_type == "SEL":
    #                 return self.query_with_result(query_to_execute)
    #             else:
    #                 self.execute(query_to_execute)
    #     except py.ProgrammingError as e:
    #         print("failed to execute {}, error: {}".format(query_to_execute, e))

    # This method will accept a DML statement and Returns no result set.
    @staticmethod
    def execute_with_connection(connection, sql, commit=True):
        c = ''
        try:
            c = connection.cursor()
            c.execute(sql)  # Execute the input SQL statement
            if commit is True:
                c.commit()
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise
        finally:
            del c

    # Vertica insertion (bulk)
    # data: result set from the query
    # column_list: list of columns which need to be filled
    # Sample column_list : column_list = [{'SOURCE_COLUMN': 'FACT_CURRENCY', 'TARGET_COLUMN': 'CURRENCY'},
    #                                   {'SOURCE_COLUMN': 'RETAILER_NAME', 'TARGET_COLUMN': 'RETAILER'},
    #                                   {'SOURCE_COLUMN': 'RETAILER_KEY', 'TARGET_COLUMN': 'KEY'},
    #                                   {'SOURCE_COLUMN': 'ACTIVE', 'TARGET_COLUMN': 'ACTIVE'}]
    # batch size: 10,000
    def batch_insert(self, table_name, column_list, data, commit=True):
        c = ''
        # Prepare the list of all incoming source column
        source_col = [col['SOURCE_COLUMN'] for col in column_list]
        # Verify if the source column present in the result set or not. If not raise exception
        for col in source_col:
            exist_flag = any(col in source_col for source_col in data)
            if exist_flag:
                continue
            else:
                raise Exception('Column not exists in the output list')
        # Define a new list which will contain list of dicts for all key (Target Column) - value pair
        # assuming all key has a valid value.
        final_dict = []
        for new_data in data:
            final_dict.append({col['TARGET_COLUMN']: new_data[col['SOURCE_COLUMN']]
                              for col in column_list if col['SOURCE_COLUMN'] in new_data})
        # get all the distinct keys in a dict
        insert_cols = ""
        for colname in final_dict[0].keys():
            if insert_cols == "":
                insert_cols = colname
            else:
                insert_cols = insert_cols + "," + colname

        insert_cond = ', '.join('?' * len(final_dict[0]))
        # Create a list of tuples
        insert_values = []
        insert_stmt = "insert into " + table_name + " ({}) values ({})".format(insert_cols, insert_cond)
        connection = DWAccessLayer.get_connection(self)
        batch_size = 100
        cur_batch_count = 0
        for val in final_dict:
            insert_values.append(tuple(val.values()))
            cur_batch_count += 1

            if cur_batch_count >= batch_size:
                cur_batch_count = 0
                try:
                    c = connection.cursor()
                    c.executemany(insert_stmt, insert_values)
                    if commit is True:
                        c.commit()
                    insert_values = []
                except py.ProgrammingError as e:
                    print("failed to execute {}, error: {}".format(insert_stmt, e))
                    raise
                finally:
                    pass

        connection = DWAccessLayer.get_connection(self)
        try:
            c = connection.cursor()
            c.executemany(insert_stmt, insert_values)
            if commit is True:
                c.commit()
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(insert_stmt, e))
            raise
        finally:
            del c
        # Prepare the Insert statement skeleton


# SQL Server Connection and methods (Refer connect.ps1)
class AppAccessLayer:

    def __init__(self, rdp_server=None, db_name=None, use_name=None, password=None, get_ip=True):
        self._rdp_server = rdp_server
        self._db_name = db_name
        self._user_name = use_name
        self._password = password
        self._connection_pool = None
        self._config_db_conn = cn.Connections(self._rdp_server, self._db_name)
        self.get_connection()
        self._instance_ip = None
        self._instance_port = None
        if get_ip:
            self.get_instance_ip_port()
        else:
            self._instance_ip = rdp_server
            self._instance_port = 1433

    @property
    def instance_ip(self):
        return self._instance_ip

    @property
    def instance_port(self):
        return self._instance_port

    def get_session(self):
        session_maker = scoped_session(sessionmaker(bind=self._config_db_conn.config_db_engine))
        session = session_maker()
        return session

    def get_connection(self):
        # If the Trusted Connection is Yes then connect to DB based Server and DB Name.
        # Otherwise connect to DB based on the user name and  password along with the server and DB name
        if self._connection_pool is None:
            if self._rdp_server is not None and self._db_name is not None:
                if self._user_name and self._password:
                    self._connection_pool = py.connect("DRIVER={{SQL Server}}; SERVER={0}; DATABASE={1}; UID={2}; "
                                                       "PWD={3}".
                                                       format(self._rdp_server, self._db_name,
                                                              self._user_name, self._password))
                else:
                    self._connection_pool = py.connect("DRIVER={{SQL Server}}; SERVER={0}; DATABASE={1}; "
                                                       "trusted_connection={2}".
                                                       format(self._rdp_server, self._db_name, "Yes"))
            elif self._config_db_conn.config_db_connection_string["TrustedConnection"] == 'Yes':
                self._connection_pool = py.connect("DRIVER={{SQL Server}}; SERVER={0}; DATABASE={1}; "
                                                   "trusted_connection={2}".
                                                   format(self._config_db_conn.config_db_connection_string["Server"],
                                                          self._config_db_conn.
                                                          config_db_connection_string["InitialCatalog"],
                                                          self._config_db_conn.
                                                          config_db_connection_string["TrustedConnection"]))
            else:
                self._connection_pool = py.connect("DRIVER={{SQL Server}}; SERVER={0}; DATABASE={1}; UID={2}; "
                                                   "PWD={3}".
                                                   format(self._config_db_conn.config_db_connection_string["Server"],
                                                          self._config_db_conn.
                                                          config_db_connection_string["InitialCatalog"],
                                                          self._config_db_conn.
                                                          config_db_connection_string["UserID"],
                                                          self._config_db_conn.
                                                          config_db_connection_string["Password"],
                                                          self._config_db_conn.
                                                          config_db_connection_string["TrustedConnection"]))
        return self._connection_pool

    def close_connection(self):
        self._connection_pool.close()

    def get_instance_ip_port(self):
        query = "select top 1 local_net_address, local_tcp_port from sys.dm_exec_connections " \
                "where local_net_address is not null and session_id = @@SPID"
        ip_port = self.query_with_result(query)
        self._instance_ip = ip_port[0].get("local_net_address")
        self._instance_port = ip_port[0].get("local_tcp_port")

    def get_identity_value(self,sql):
        connection = AppAccessLayer.get_connection(self)
        try:
            with connection.cursor() as c:  # will close the cursor explicitly.
                c.execute(sql)  # Execute the input SQL statement
                for identity_value in c.fetchall():
                    return identity_value[0]
                c.commit()
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise
        finally:
            pass

    # All the Query () will accept the sql parameter as a SELECT statement. If you pass a DML statement
    # then it will throw an error
    # Returns result set (could be multiple rows)
    def query_with_result(self, sql):
        connection = AppAccessLayer.get_connection(self)
        try:
            with connection.cursor() as c:  # will close the cursor explicitly.
                c.execute(sql)
                columns = [column[0] for column in c.description]
                results = []
                for row in c.fetchall():
                    results.append(dict(zip(columns, row)))
                return results
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise e
        finally:
            pass

    # Execute the SQL (SELECT) statement and return the rows
    def query_scalar(self, sql):
        connection = AppAccessLayer.get_connection(self)
        try:
            with connection.cursor() as c:  # will close the cursor explicitly.
                c.execute(sql)
                scalar_value = c.fetchone()  # Gives the number of returned rows from the SELECT query
                return scalar_value
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise
        finally:
            pass

    # Execute the SQL (SELECT) statement with the input connection and return the rows
    @staticmethod
    def query_with_connection(connection, sql):
        try:
            with connection.cursor() as c:
                c.execute(sql)  # Execute the input query
                columns = [column[0] for column in c.description]
                results = []
                for row in c.fetchall():
                    results.append(dict(zip(columns, row)))
                return results
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise

    def query_over_orm(self, orm_table, query, filter_cond, group_by):
        try:
            if query != '' and filter_cond != '' and group_by != '':
                orm_session = self.get_session()
                orm_table = aliased('config.' + orm_table)
                orm_query = query + '.\\' + filter_cond + '.\\' + group_by
                orm_result = orm_session.orm_query
                return orm_result
            elif query != '' and filter_cond != '' and group_by == '':
                orm_session = self.get_session()
                orm_table = aliased('config.' + orm_table)
                orm_query = query + '.\\' + filter_cond
                orm_result = orm_session.orm_query
                return orm_result
            elif query != '' and filter_cond == '' and group_by == '':
                orm_session = self.get_session()
                orm_table = aliased('config.' + orm_table)
                orm_query = query
                orm_result = orm_session.orm_query
                return orm_result
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(query, e))
            raise

    #  This method will accept a DML statement and Returns no result set.
    def execute(self, sql):
        connection = AppAccessLayer.get_connection(self)
        try:
            with connection.cursor() as c:  # will close the cursor explicitly.
                c.execute(sql)  # Execute the input SQL statement
                affected_count = c.rowcount
                c.commit()
                return affected_count
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise
        finally:
            pass

    def execute_with_rowcount(self, sql):
        connection = AppAccessLayer.get_connection(self)
        try:
            with connection.cursor() as c:  # will close the cursor explicitly.
                c.execute(sql)  # Execute the input SQL statement
                affected_count = c.rowcount
                c.commit()
                return affected_count
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise
        finally:
            pass

    # Execute DML statement with connection
    @staticmethod
    def execute_with_connection(connection, sql):
        try:
            with connection.cursor() as c:  # will close the cursor explicitly.
                c.execute(sql)  # Execute the input SQL
                c.commit()
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
            raise

    # SQL server insert
    # data: result set from the query
    # column_list: list of columns which need to be filled
    # Sample column_list : column_list = [{'SOURCE_COLUMN': 'FACT_CURRENCY', 'TARGET_COLUMN': 'CURRENCY'},
    #                                   {'SOURCE_COLUMN': 'RETAILER_NAME', 'TARGET_COLUMN': 'RETAILER'},
    #                                   {'SOURCE_COLUMN': 'RETAILER_KEY', 'TARGET_COLUMN': 'KEY'},
    #                                   {'SOURCE_COLUMN': 'ACTIVE', 'TARGET_COLUMN': 'ACTIVE'}]
    # batch size: 10,000
    def batch_insert(self, table_name, column_list, data):
        # Prepare the list of all incoming source column
        source_col = [col['SOURCE_COLUMN'] for col in column_list]
        # Verify if the source column present in the result set or not. If not raise exception
        for col in source_col:
            exist_flag = any(col in source_col for source_col in data)
            if exist_flag:
                continue
            else:
                raise Exception('Column not exists in the output list')
        # Define a new list which will contain list of dicts for all key (Target Column) - value pair
        # assuming all key has a valid value.
        final_dict = []
        for new_data in data:
            final_dict.append({col['TARGET_COLUMN']: new_data[col['SOURCE_COLUMN']]
                              for col in column_list if col['SOURCE_COLUMN'] in new_data})
        # get all the distinct keys in a dict
        all_columns = {k for d in final_dict for k in d.keys()}
        all_columns = sorted(all_columns)
        # concatenate all the distinct columns separated by "," into a string
        insert_cols = ', '.join(all_columns)
        # get the number of "?" (cond) for the Insert statement
        insert_cond = ', '.join('?' * len(all_columns))
        # Create a list of tuples
        insert_values = []
        for val in final_dict:
            val = OrderedDict(sorted(val.items()))
            insert_values.append(tuple(val.values()))
        # Prepare the Insert statement skeleton
        insert_stmt = "insert into " + table_name + " ({}) values ({})".format(insert_cols, insert_cond)
        connection = AppAccessLayer.get_connection(self)
        try:
            with connection.cursor() as c:  # will close the cursor explicitly.
                c.executemany(insert_stmt, insert_values)
                c.commit()
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(insert_stmt, e))
            raise
        finally:
            pass

    # bulk copy data rom Vertica to SQL Server
    def bulk_copy_from_dw(self):
        pass

