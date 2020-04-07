from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
import pandas as pd
from pymapd import connect, DatabaseError
from typing import Dict,List, Any
import prefect

class OmniSciLoadTableTask(Task):

    """
    Task for loading an OmniSci database table from a pandas dataframe
    Args:
        - db_name (str): name of OmniSci database
        - user (str): user name used to authenticate
        - password (str): password used to authenticate
        - host (str): database host address
        - port (int, optional): port used to connect to Omnisci database, defaults to 6274 if not provided
    """

    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        data = None,
        port: int = 6274,
        drop_existing = True,
        **kwargs
    ):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.data = data
        self.drop_existing = False
        super().__init__(**kwargs)

    @defaults_from_attrs("data", "drop_existing")
    def run(self, data = None, drop_existing = False):
        """
        Task run method. Loads a pandas dataframe into an omnisci table
        Args:
            - data: Dict[str, pd.DataFrame] a Dict with table name and the data as a Pandas DataFrame
        Returns:
            - None
        Raises:
            - ValueError: if data is None
            - DatabaseError: if exception occurs when executing the query
        """

        ## connect to database, open cursor
        ## allow psycopg2 to pass through any exceptions raised
        conn = connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        logger = prefect.context.get('logger')
        ## try to execute query
        try:
            with conn:
                data_to_load = data['table_data']
                table_name = data['table_name']
                num_rows = len(data_to_load.index)
                logger.info(f'Proceeding to load {num_rows} into {table_name}')

                if drop_existing:
                    executed = conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    
                conn.load_table(table_name, data_to_load, create='infer', preserve_index=False)

            conn.close()
            return executed

        ## pass through error, and ensure connection is closed
        except (Exception, DatabaseError) as error:
            conn.close()
            raise error

class OmniSciQueryTable(Task):
    """
    Task for loading an OmniSci database table from a pandas dataframe
    Args:
        - db_name (str): name of OmniSci database
        - user (str): user name used to authenticate
        - password (str): password used to authenticate
        - host (str): database host address
        - port (int, optional): port used to connect to Omnisci database, defaults to 6274 if not provided
        - query (str, optional): query to execute against database
    """

    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        data = None,
        port: int = 6274,
        query: str = None,
        **kwargs
    ):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.query = query
        super().__init__(**kwargs)

    @defaults_from_attrs("query")
    def run(self, query = None):
        """
        Task run method. Queries an omnisci table and uses the python API to return a pandas dataframe
        Args:
            - query: query string to execute
        Returns:
            - pandas.DataFrame
        Raises:
            - ValueError: if query string is None
            - DatabaseError: if exception occurs when executing the query
        """
        if(query is None):
            raise ValueError('Query string must be provided, cannot be empty')


        ## connect to database, open cursor
        conn = connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        logger = prefect.context.get('logger')

        logger.info(data)
        ## try to execute query
        try:
            with conn:
                result_df = conn.execute( query )
                return result_df

        ## pass through error, and ensure connection is closed
        except (Exception, DatabaseError) as error:
            conn.close()
            raise error
