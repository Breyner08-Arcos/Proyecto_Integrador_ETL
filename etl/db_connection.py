class DBConnection:
    def __init__(self, host='localhost', user='root', password='', database='negocio'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        try:
            import mysql.connector
        except Exception as e:
            raise ImportError('mysql-connector-python no está instalado. Instale con: pip install mysql-connector-python') from e
        self.conn = mysql.connector.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        return self.conn

    def query(self, sql):
        if not self.conn:
            raise RuntimeError('No hay conexión establecida.')
        cur = self.conn.cursor(dictionary=True)
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        return res

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
