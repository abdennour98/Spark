import pandas as pd
import argparse
from sqlalchemy import create_engine
import os
from time import time

#  usr
#  password
#  host
#  port
#  database name

#  table name
# url of the csv file

def main(params):
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url=params.url
    parquet_name="output.parquet"
    csv_name="output.csv"

    os.system(f"wget {url} -O {parquet_name}")
    pd.read_parquet(parquet_name).to_csv(csv_name)
    df=pd.read_csv(csv_name,index_col=0)

    df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)

    engine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_iter=pd.read_csv(csv_name,index_col=0,iterator=True,chunksize=100000)



    df.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')
    try:
        while True:
                t_start=time()
                df=next(df_iter)
                df.to_sql(name=table_name,con=engine,if_exists='append')
                t_end=time()
                print("inserted another chunk....., took %.3f"%(t_end-t_start))
    except StopIteration as e:
        print("all chunks have been successfully added")
    
if __name__=='__main__':
        parser = argparse.ArgumentParser(description='Ingest csv data to postgres')
        parser.add_argument('--user', help='user name for postgres')
        parser.add_argument('--password', help='password for postgres')
        parser.add_argument('--host', help='host for postgres')
        parser.add_argument('--port', help='port for postgres')
        parser.add_argument('--db', help='database name for postgres')
        parser.add_argument('--table_name', help='name of the table where we will write the resultats to')
        parser.add_argument('--url', help='url of the csv file')
        args=parser.parse_args()
        main(args)
     
    






