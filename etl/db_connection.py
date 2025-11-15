import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
 
load_dotenv()
 
HOST = os.getenv("DB_HOST", "localhost")
USER = os.getenv("DB_USER", "root")
PASSWORD = os.getenv("DB_PASSWORD", "")
DATABASE = os.getenv("DB_NAME", "negocio")
BUSINESS_ID = int(os.getenv("BUSINESS_ID", 1))
 
class DBConnection:
    def __init__(self, host=HOST, user=USER, password=PASSWORD, database=DATABASE):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
 
    def connect(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            return self.conn
        except ImportError as e:
            raise ImportError(
                "mysql-connector-python no está instalado. Instale con: pip install mysql-connector-python"
            ) from e
        except Error as e:
            raise ConnectionError(f"Error al conectar a MySQL: {e}") from e
 
    def query(self, sql):
        if not self.conn or not self.conn.is_connected():
            raise RuntimeError("No hay conexión establecida o se perdió.")
 
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(sql)
            return cur.fetchall()
        except Error as e:
            raise Exception(f"Error al ejecutar la consulta SQL: {e}") from e
        finally:
            cur.close()
 
    def close(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()
            self.conn = None
