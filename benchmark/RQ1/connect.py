import psycopg2
from psycopg2 import extras
import numpy as np

host = '127.0.0.1'
port = 5438
user = 'root'

class Connection:
   def __init__(self, db):
      self.db = db
      self.conn=psycopg2.connect(host=host,port=port,user=user,database=db)
      self.cursor=self.conn.cursor()

   def execute_sql(self, sql):
      self.cursor.execute(sql)
      # data = self.cursor.fetchone()
      if self.cursor.description:
         result = self.cursor.fetchall()
         return list(result)
         
   
   def get_tables(self):
      sql = f'select table_name from information_schema.tables where table_catalog = \'{self.db}\' and table_schema = \'public\''
      print(self.execute_sql(sql))
      return [table[0] for table in self.execute_sql(sql)] 
   
   def desc_table(self,table_name):
      self.execute_sql(f'select * from {table_name}')
      return [temp.name for temp in self.cursor.description]

   def __del__(self):
      self.conn.close()