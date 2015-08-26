__author__ = 'kasper'

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class PostgresTest(object):
    '''
    This is a python class created to test queries
    '''
    __con = None
    __cur = None
    __dbuser = None
    __dbpass = None
    __dbhost = None

    def __init__(self):

        self.__dbuser = 'kasper'
        self.__dbpass = 'kasper1234'
        self.__dbhost = 'localhost'

    def openConnection(self, db_name='testdatabase_kasper'):
        '''
        :param db_name: database to connect to.
        :return: database connection.
        '''
        print 'Open connection'
        self.__con = connect(dbname=db_name, user=self.__dbuser, host=self.__dbhost, password=self.__dbpass)
        self.__con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def closeConnection(self):
        '''
        Closes the database connection.
        '''

        print 'Close connection'
        self.__con.close()

    def main(self):
        print 'Start main'
        self.openConnection()
        cur = self.__con.cursor()
        query = """
            DROP TABLE IF EXISTS COMPANY;
            CREATE TABLE COMPANY(
                ID INT PRIMARY KEY     NOT NULL,
                NAME           TEXT    NOT NULL,
                AGE            INT     NOT NULL,
                ADDRESS        CHAR(50),
                SALARY         REAL
                );
            DROP TABLE IF EXISTS COMPANY;
            CREATE TABLE COMPANY(
                ID INT PRIMARY KEY     NOT NULL,
                NAME           TEXT    NOT NULL,
                AGE            INT     NOT NULL,
                ADDRESS        CHAR(50),
                SALARY         REAL
                );
            """

        cur.execute(query)
        cur.close()
        self.closeConnection()
        print 'Stop main'

if __name__ == '__main__':
    postgresTest = PostgresTest()
    postgresTest.main()