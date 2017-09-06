from async_db import DBPreProcess
from random import random
import pandas as pd
from dask.delayed import delayed


CREDENTIALS = {
    'host': '127.0.0.1',
    'password': 'coqueta',
    'username': 'root'
}


def make_df(cols, n):
    df = {col: [random() for i in range(n)] for col in cols}
    return pd.DataFrame(df)


class One(DBPreProcess):
    DB_ALIAS = "test_tables"
    TABLE_ALIAS = "one"

    def __init__(self, table):
        self.TABLE_ALIAS = table
        super().__init__(credentials=CREDENTIALS)
        self.data_set = make_df("ONE", 1000)


class Two(DBPreProcess):
    DB_ALIAS = "test_tables"
    TABLE_ALIAS = "two"

    def __init__(self):
        super().__init__(credentials=CREDENTIALS)
        self.data_set = make_df("TWO", 20)




if __name__ == '__main__':
    flags = []
    for i in range(40):
        data1 = One(str(i))
        x = delayed(data1.send_to_db)()
        flags.append(x)

    ok = delayed(all)(flags)

