import pymysql
import sys
import os


class Query_Executor:
    """
    Small helper class to execute query, and log them if there is an error
    """
    def __init__(self, parameters):
        self.log_file = os.path.join(parameters['paths']['program_path'], parameters['paths']['sql_error_log'])
        self.connection = pymysql.connect(
            host=parameters['database']['host'],
            port=int(parameters['database']['port']),
            user=parameters['database']['user'],
            password=parameters['database']['password'],
            database=parameters['database']['database'],
            cursorclass=pymysql.cursors.DictCursor,
            charset='utf8mb4',
            autocommit=True,
            # init_command='USE pubmed_raw;'
        )

    def execute_select(self, sql_command):
        result_set = []
        connection = self.connection
        cursor = connection.cursor()
        try:
            #~ cursor.execute('SET ROLE pubmed_role;')
            cursor.execute(sql_command)
            for row in cursor:
                result_set.append(dict(row))
            connection.close()
        except:
            exception = sys.exc_info()[1]
            errors_log = open(self.log_file, 'a')
            errors_log.write('{} - {}\n'.format(exception, sql_command))
            errors_log.close()

        return result_set

    def execute(self, sql_command):
        connection = self.connection
        cursor = connection.cursor()
        try:
            #~ cursor.execute('SET ROLE pubmed_role;')
            cursor.execute(sql_command)
            connection.close()
        except:
            exception = sys.exc_info()[1]
            errors_log = open(self.log_file, 'a')
            errors_log.write('{} - {}\n'.format(exception, sql_command.encode('utf8')))
            errors_log.close()
