import sqlalchemy as sql


class DataWarehouse():

    def __init__(self):
        pass

    def create_connection(self):
        conn = sql.create_engine('sqlite:///datawarehouse/datawarehouse.db')
        return conn
    
    def create_metadata(self, schema = None):
        metadata = sql.MetaData(schema = schema)
        return metadata
    

if __name__ == "__main__":

    dwh = DataWarehouse()

    conn  = dwh.create_connection()
    metadata = dwh.create_metadata()


    metadata.create_all(conn)