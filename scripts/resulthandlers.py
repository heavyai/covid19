
from prefect.engine.result_handlers import ResultHandler, LocalResultHandler
from typing import Dict,List, Any
import os
import prefect
import sqlite3
from enum import Enum

class Mode(Enum):
    APPEND = 0
    REPLACE = 1

class SQLiteResultHandler(ResultHandler):


    def __init__(self, result_db_path: str = None, validate: bool = True, result_tbl_name: str=None, mode=Mode.REPLACE):
        full_prefect_path = os.path.abspath(prefect.config.home_dir)
        if (
            result_db_path is None
            or os.path.commonpath([full_prefect_path, os.path.abspath(result_db_path)]) == full_prefect_path
        ):
            directory = os.path.join(prefect.config.home_dir, "results")
        else:
            directory = result_db_path

        if validate:
            abs_directory = os.path.abspath(os.path.expanduser(directory))
            if not os.path.exists(abs_directory):
                os.makedirs(abs_directory)
        else:
            abs_directory = directory
        self.result_db_path = f'{abs_directory}/results.db'
        self.mode = mode
        self.result_tbl_name = 'result' if result_tbl_name is None else result_tbl_name

        super().__init__()

    def write(self, result) -> Any:
        tbl_data = result['data']['table_data']

        if_exists = 'replace' if self.mode is Mode.REPLACE else 'append'

        with sqlite3.connect(self.result_db_path) as conn:
            tbl_data.to_sql(self.result_tbl_name, conn, if_exists=if_exists, index=False)
            return tbl_data.to_json(orient='table')

    def read(result_db_path:str):
        with sqlite3.connect(self.result_db_path) as conn:
            tbl_data = pd.read_sql(f'select * from {self.result_tbl_name}', conn)
            return tbl_data