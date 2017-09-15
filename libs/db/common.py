import connectionmanager as cn
import configdb as config
import time as tm
import datetime as dt
import DBOperations as dbop
from sqlalchemy import update, exists
from sqlalchemy.orm import sessionmaker, aliased, scoped_session
from sqlalchemy.orm.exc import MultipleResultsFound
import pyodbc as py
import linecache
import sys
import os
import datetime
from random import randint


class Event:

    def create_event(self):
        pass

    def consume_event(self):
        pass

    def postpone_event(self):
        pass


class RSIVariable:

    # setting up the initial connections
    def __init__(self, rdp_server=None, db_name=None):
        if rdp_server is not None and db_name is not None:
            _config_db_conn = cn.Connections(rdp_server, db_name)
        else:
            _config_db_conn = cn.Connections()
        _session_maker = scoped_session(sessionmaker(bind=_config_db_conn.config_db_engine))
        self._session = _session_maker()

    # Below method will return the variable value for an input variable name from RSIVariable Table if there is a match.
    # If there is no match then it will return the default value specified in the parameter.
    # If the query returns multiple values for the specified variable name then it will throw exception
    def get_rsi_variable(self, variable_name, default):
        rsi_var = aliased(config.RSIVariable)
        try:
            result = self._session.query(rsi_var.VARIABLE_VALUE).\
                filter(rsi_var.VARIABLE_NAME == variable_name).scalar()
            if result:
                return result
            else:
                return default
        # Exception raised if the query returns multiple variable values for the specified variable name
        except MultipleResultsFound as e:
            print("Multiple Results Found in RSIVariable for the Variable Name {}".format(variable_name))
        # close the session
        finally:
            self._session.close()

    # This method will check if any variable value exists or not for the entered variable name.
    # If exists then updates the existing variable value with the entered variable value
    # otherwise insert a new row with the entered variable name and variable value.
    def set_rsi_variable(self, variable_name, variable_value):
        rsi_var = aliased(config.RSIVariable)
        try:
            # does an existance check for the variable name in the RSIVariable table.
            # If found in db then update the variable value else insert a new record accordingly
            result = self._session.query(rsi_var.VARIABLE_VALUE).\
                filter(rsi_var.VARIABLE_NAME == variable_name).\
                filter(exists().
                       where(rsi_var.VARIABLE_NAME == variable_name)).scalar()
            # variable value exists for the specified variable name
            if result:
                # update the existing variable value with the specified value for the variable name
                update_variable_value = update(config.RSIVariable).\
                    where(config.RSIVariable.VARIABLE_NAME == variable_name).\
                    values(VARIABLE_VALUE=variable_value)
                self._session.execute(update_variable_value)
                self._session.commit()
            # variable value does not exists for the specified variable name
            else:
                # Insert a new record with the supplied variable value and variable name.
                insert_rsi_variable = config.RSIVariable(VARIABLE_NAME=variable_name, VARIABLE_VALUE=variable_value)
                self._session.add(insert_rsi_variable)
                self._session.commit()
        # Exception raised if the query returns multiple variable values for the specified variable name
        except MultipleResultsFound as e:
            print("Multiple Results Found in RSIVariable for the Variable Name {}".format(variable_name))
        # close the session
        finally:
            self._session.close()


class Misc:

    # Add days to period_key which data type is integer, but content is datetime and returns an Integer too
    @staticmethod
    def add_period_key(self, period_key, days):
        return int((dt.datetime.strptime(str(period_key), "%Y%m%d").date() + dt.timedelta(days=days)).
                   strftime('%Y%m%d'))

    @staticmethod
    def start_tick(self):
        # This function is used to start recording a code snippet time
        start_time = tm.time()
        return start_time

    @staticmethod
    def end_tick(self):
        # This function is used to end recording a code snippet time
        end_time = tm.time()
        return end_time

    @staticmethod
    def time_taken(self, end_time, start_time):
        time_taken = end_time - start_time
        time_taken = str(dt.timedelta(seconds=time_taken))
        return time_taken

    def assert_exit(self):
        pass

    @staticmethod
    def enable_sql_server_option(self, option, value, app=None):
        # create the connection object
        if app is not None:
            app_access_layer = app
        else:
            app_access_layer = dbop.AppAccessLayer()
        sql = "sp_configure '" + option + "', " + value + "; reconfigure with override;"
        app_access_layer.execute(sql)


class Email:
    def __init__(self, profile, subject, body, attachment, app=None):
        self._profile = profile
        self._subject = subject
        self._body = body
        self._attachment = attachment
        self._app = app
        # create the connection object
        if self._app is not None:
            app_access_layer = self._app
        else:
            app_access_layer = dbop.AppAccessLayer()
        self._connection = app_access_layer.get_connection()

    def send_mail(self):
        sql = "exec [etl].[SP$RSI_send_email] '" + self._profile + "', '" + self._subject + "', '" + \
                  self._body + "', '" + self._attachment + "'"
        try:
            with self._connection.cursor() as c:  # will close the cursor explicitly.
                c.execute(sql)  # Execute the input SQL statement
                c.commit()
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql, e))
        finally:
            self._connection.close()


class Log:
    def __init__(self, app=None):
        self._app = app
        # global dictionary logger
        self.ps_logger = None

    def PrintException(self):
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))

    # utility function so that the function should not be static when self is not referenced
    def is_not_used(self):
        pass

    # writes the log to the database
    def write_log(self, log_type, key, description, status):
        # if rdp_server and db_name is available, then connect to the database
        # if not available then connect to the database with the default values
        description = description.replace("'", "`")
        if self._app is not None:
            app_access_layer = self._app
        else:
            app_access_layer = dbop.AppAccessLayer()

        if not key:
            key = -1
        # length of message in the database
        max_length = 3799
        local_desc = description
        # log message to append when broken to smaller set
        message_first = "==> Continued to next"
        message_second = "Continued from prev ==> "
        i = 1
        # loop until description is not empty
        while local_desc != "":
            if len(local_desc) > max_length:
                if i == 0:
                    # log to be written to the database
                    message = message_second + local_desc[:max_length] + message_first
                    # call to the procedure
                    log_sql = "EXEC dbo.sp$RSI_ADD_LOG @pvs_owner_type = '{}' ,@pvn_owner_key = {}, " \
                              "@pvs_description = '{}', @pvs_status= '{}'".format(log_type, key, message, status)
                    app_access_layer.execute(log_sql)

                else:
                    message = local_desc[:max_length] + message_first
                    log_sql = "EXEC dbo.sp$RSI_ADD_LOG @pvs_owner_type = '{}' ,@pvn_owner_key = {}, " \
                              "@pvs_description = '{}', @pvs_status= '{}'".format(log_type, key, message, status)
                    app_access_layer.execute(log_sql)
                i = 0
            else:
                if i == 0:
                    # message split, so message_second is added to the next database entry
                    message = message_second + local_desc
                    log_sql = "EXEC dbo.sp$RSI_ADD_LOG @pvs_owner_type = '{}' ,@pvn_owner_key = {}, " \
                              "@pvs_description = '{}', @pvs_status= '{}'".format(log_type, key, message, status)
                    app_access_layer.execute(log_sql)
                # message needs no split
                else:
                    message = local_desc
                    log_sql = "EXEC dbo.sp$RSI_ADD_LOG @pvs_owner_type = '{}' ,@pvn_owner_key = {}, " \
                              "@pvs_description = '{}', @pvs_status= '{}'".format(log_type, key, message, status)
                    app_access_layer.execute(log_sql)
            local_desc = local_desc[max_length:]

    # register_levels is the level at which the log has to done.. INFO, WARN, ERROR, DEBUG.
    # Registration is done for 2 methods - log_std_out and log_file. function has to be of these two.
    # argument is the arguments that has to be passed to the method
    # in string format 'filename="testfile.txt", dir="\\feee\ee"'
    # thread_number is the job number which will be prefixed in the log file name,..if not passed will be taken as blank
    def register_log(self, register_levels, function, argument, thread_number):
        # create empty hash table if it doesn't exist
        if not self.ps_logger:
            self.ps_logger = {}
        # store the argument as a dictionary
        func_arg = {function: argument, 'thread_number': thread_number}
        # lvl an list of function-argument
        lvl = list()
        # add the dictionary pair of function argument to the list
        lvl.append(func_arg)
        # add the level if it doesn't exist already
        for level in register_levels:
            # If that level does not exist add it to the logger
            if self.ps_logger.get(level) is None:
                self.ps_logger[level] = lvl
            # if it already existed, add the non duplicate entry
            elif self.ps_logger.get(level) is not None:
                value = self.ps_logger[level]
                # check if the lvl exists in value(logger)
                lvl1 = lvl[0]
                if lvl1 not in value:
                    value = value+lvl
                    self.ps_logger[level] = value

    # calls log_std_out or log_file based on the function name
    # call is made dynamically to the respective function
    # call is made with text, level and dictionary containing function-argument-thread_number
    def write_log_level(self, level, text):
        # if logger doesn't exist, then return
        if self.ps_logger is None:
            return
        # function-argument pair for the given level
        loggers = self.ps_logger.get(level)
        # length of the list
        len_list = len(loggers) - 1
        # retrieve the dictionary from the list
        while len_list >= 0:
            func_arg_thread = (loggers[len_list])
            len_list -= 1
        # retrieve the name of the function from the dictionary
            function_name = ''
            for key in func_arg_thread:
                if key is not 'thread_number':
                    function_name = key
        # dynamic function call
            method_attr = getattr(self, function_name)
            method_attr(text, level, func_arg_thread)

    # un-register global logger
    def un_register_all_loggers(self):
        self.ps_logger = None

    # call made to write_log_level with level as info
    # call made to write_log_level with level as info
    def log_info(self, text):
        self.write_log_level("info", text)

    # call made to write_log_level with level as debug
    def log_debug(self, text):
        self.write_log_level("debug", text)

    # call made to write_log_level with level as warn
    def log_warn(self, text):
        self.write_log_level("warn", text)

    # call made to write_log_level with level as error
    def log_error(self, text):
        self.write_log_level("error", text)

    # prints the current date time level and the text to the console
    # dict_arguments not used but needed for dynamic function call
    def log_std_out(self, text, level, dict_arguments):
        self.is_not_used()
        # get the current date and time
        current_date_time = datetime.datetime.now()
        # set the date time format : mm:dd:yyyy
        curr_date = str(current_date_time.year) + "/" + str(current_date_time.month) + "/" + str(current_date_time.day)
        # append time to the date
        curr_date = curr_date + " " + str(current_date_time.hour) + ":" + str(current_date_time.minute) + ":" \
                    + str(current_date_time.second)
        text = "[" + curr_date + "]" + "[" + level + "]" + text
        print(text)

    # calls write_log_file with directory, path-name, file_name (log) and thread_number
    # input is {directory="", name=""}
    def log_file(self, text, level, dict_arguments):
        directory = ''
        name = ''
        # need name, directory, thread_number for the call to write log file method
        for key in dict_arguments:
            # get the thread number
            if key is 'thread_number':
                thread_number = dict_arguments[key]
            else:
                dictionary_dir_path = dict_arguments.get(key)
                for file_info in dictionary_dir_path:
                    if file_info is 'name':
                        name = dictionary_dir_path.get(file_info)
                    else:
                        directory = dictionary_dir_path.get(file_info)
        # [level] in this format
        log = "[" + level + "]" + " " + text
        self.write_log_file(directory, name, log, thread_number)

    # directory is the specified directory to store the file
    # name is the name of the file
    # log --> [level] message
    # thread_number is the thread number
    def write_log_file(self, directory, name, log, thread_number):
        self.is_not_used()
        # log_file is the name of the file which is going to be created to store log information
        log_file = ''
        # check if the given directory exists, if not create one
        if not os.path.exists(directory):
            os.makedirs(directory)
        curr_date = datetime.datetime.now().strftime('%Y%m%d')
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        # setting file name and the message in the log file - if thread number exists or if it doesn't
        if not thread_number:
            name_thread_file_name = name.replace(".",  "_" + curr_date + ".")
            message = "[" + curr_time + "]" + log
        else:
            name_thread_file_name = name.replace(".", "_" + thread_number + "_" + curr_date + ".")
            message = "[" + curr_time + "]" + "[" + "Thread_" + thread_number + "]" + log
        # check if the file already exists and create a file if it does not exist
        log_file = os.path.join(directory, name_thread_file_name)
        # try to write to the file after checking if the file size is more than 1MB
        try:
            # if file size is more than 1MB it will be written to the log in the check_log_file method
            self.check_log_file(log_file)
            # File size < 1MB
            # If less, then directly write to the file
            file_o = open(log_file, 'a+')
            file_o.write(message + '\n')
        # File error --> File locked for read or write
        except IOError:
            prefix = datetime.datetime.now().strftime('%H%M%S%f') + "(" + str(randint(0,9)) + ")."
            new_file_name = log_file.replace(".", prefix)
            file_o = open(new_file_name, 'a+')
            file_o.write(message + '\n')

    # checks if the log file exists and for its size (> 1MB)
    # if size > 1MB, rename the existing file
    def check_log_file(self, log_file):
        self.is_not_used()
        # type cast to string
        log_file = str(log_file)
        # Check if the file exists
        if os.path.isfile(log_file):
            # output of get size is in bytes
            if os.path.getsize(log_file) >= 5000000:
                prefix = datetime.datetime.now().strftime('%H%M%S%f') + "(" + str(randint(0,9)) + ")."
                new_file_name = log_file.replace(".", prefix)
                # logfile is too huge, recreate it now.
                try:
                    os.rename(log_file, new_file_name)
                # file is locked
                except IOError:
                    pass
            else:
                return bool(0)
        # file doesn't exist --> first file of the day
        else:
            return bool(0)


class WorkTables:

    def __init__(self, context='', app=None):
        self._app = app
        self._context = context
        # create the connection object
        if self._app is not None:
            self._app_access_layer = self._app
        else:
            self._app_access_layer = dbop.AppAccessLayer()

    def get_work_table(self, table_prefix, event_key, timeout):
        query = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED" \
                "BEGIN TRANSACTION" \
                "declare @work_table NVARCHAR(400)" \
                "SELECT TOP 1 @work_table = WORK_TABLE_NAME" \
                "FROM dbo.WORK_TABLE_AUDIT wa  with(ROWLOCK XLOCK READPAST READCOMMITTEDLOCK)" \
                "WHERE WORK_TABLE_PREFIX = '"+ table_prefix + "'" \
                "AND EVENT_KEY IS NULL;" \
                "ORDER BY WORK_TABLE_NAME ASC;" \
                "UPDATE dbo.WORK_TABLE_AUDIT SET EVENT_KEY = " + event_key + " WHERE WORK_TABLE_NAME = @work_table;" \
                "COMMIT;" \
                "SELECT @work_table"
        work_table = self._app_access_layer.query_scalar(query)
        return work_table

    def release_work_table(self, event_key):
        query = "UPDATE WORK_TABLE_AUDIT SET EVENT_KEY = NULL WHERE EVENT_KEY=" + event_key
        self._app_access_layer.execute(query)
        pass

    def create_new_work_tables(self, silo_id, module):
        query_meta_work_tables = "SELECT WORK_TABLE_PREFIX, TABLES_PER_PROCESS, DB_TYPE, TABLE_CREATION_SCRIPT  " \
                                 "FROM META_WORK_TABLES WHERE MODULE_USED='" + module + "'"
        dw = dbop.DWAccessLayer(silo_id, self._context, self._app)
        meta_work_tables = self._app_access_layer.query_with_result(query_meta_work_tables)
        for meta_work_table in meta_work_tables:
            query_to_execute = meta_work_table["TABLE_CREATION_SCRIPT"]
            table_per_process = meta_work_table["TABLES_PER_PROCESS"]
            db_type = meta_work_table["DB_TYPE"]
            for i in range(table_per_process):
                if db_type == "APP":
                    self._app_access_layer.execute(query_to_execute)
                else:
                    dw.execute(query_to_execute)



