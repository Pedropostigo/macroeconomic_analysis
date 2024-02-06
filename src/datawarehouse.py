import os

import sqlite3 as sql


class DataWarehouse():

    def __init__(self):
        
        self.db_filepath = os.path.join("datawarehouse", "datawarehouse.db")

    def read_sql(self, file_path):
        with open(file_path, 'r') as f:
            sql_script = f.read()

        return sql_script

    def execute_script(self, script_file_path: str) -> None:
        script = self.read_sql(script_file_path)

        with sql.connect(self.db_filepath) as conn:
            conn.executescript(script)
            conn.commit()
    

if __name__ == "__main__":

    dwh = DataWarehouse()
    conn  = dwh.execute_script(os.path.join("src", "sql_datawarehouse",
                                            "sqlite", "gdp_eur_create_tables.sql"))
