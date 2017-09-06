from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.engine import url


class DBPreProcess:
    DB_ALIAS = ""
    TABLE_ALIAS = ""

    def __init__(self, credentials):
        self.data_set = None
        self.db_uri = url.URL(drivername="mysql+pymysql", **credentials)
        self.db_engine = create_engine(self.db_uri)

        self.create_schema()

    def send_to_db(self, if_exist="replace"):
        self.data_set.to_sql(name=self.TABLE_ALIAS, con=self.db_engine, schema=self.DB_ALIAS,
                             if_exists=if_exist, chunksize=5000, index=False)
        print("DONE", self.TABLE_ALIAS)
        return True

    def _check_class_members_implemented(self):
        if not all([self.DB_ALIAS, self.TABLE_ALIAS]):
            raise NotImplementedError("DB_ALIAS and TABLE_ALIAS members must be overrated")

    def create_schema(self):
        # self._check_class_members_implemented()
        conn = self.db_engine.connect()
        execute_status = conn.execute("CREATE DATABASE IF NOT EXISTS {};".format(self.DB_ALIAS))
        conn.close()
        if execute_status:
            print("{} schema created :)".format(self.DB_ALIAS))
        else:
            print("Error on CREATE DATABASE")
