import pandas as pd
import re
import os
import sqlite3

class ETLBase:
    def __init__(self, file_path, columns, output_name, db_conn=None):
        self.file_path = file_path
        self.columns = columns
        self.output_name = output_name
        self.db_conn = db_conn
        self.df = None

    def _find_insert_blocks(self, text):
        pattern = re.compile(r"INSERT\s+INTO\s+[^\(]+\((?:[^\)]*)\)\s*VALUES\s*\((.*?)\);", re.IGNORECASE | re.DOTALL)
        # Fallback: capture VALUES(...) occurrences if the above doesn't match
        vals = re.findall(r"VALUES\s*\((.*?)\);", text, flags=re.IGNORECASE | re.DOTALL)
        return vals

    def _parse_values(self, values_text):
        parts = []
        cur = ''
        in_str = False
        i = 0
        while i < len(values_text):
            c = values_text[i]
            if c == "'" and not in_str:
                in_str = True
                cur = ''
                i += 1
                while i < len(values_text):
                    if values_text[i] == "'" and i+1 < len(values_text) and values_text[i+1] == "'":
                        cur += "'"
                        i += 2
                        continue
                    if values_text[i] == "'" :
                        i += 1
                        break
                    cur += values_text[i]
                    i += 1
                parts.append(cur)
                # skip possible spaces and comma
                while i < len(values_text) and values_text[i] in " \t\n\r":
                    i += 1
                if i < len(values_text) and values_text[i] == ',':
                    i += 1
            else:
                # number, NULL or bare token until comma
                token = ''
                while i < len(values_text) and values_text[i] != ',':
                    token += values_text[i]
                    i += 1
                token = token.strip()
                if token.upper() == 'NULL' or token == '':
                    parts.append(None)
                else:
                    # try to convert numeric-like tokens
                    parts.append(token.strip().strip("'"))
                if i < len(values_text) and values_text[i] == ',':
                    i += 1
        return parts

    def extract(self):
        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
        raw_rows = re.findall(r"VALUES\s*\((.*?)\)\s*;", text, flags=re.IGNORECASE | re.DOTALL)
        data = []
        for r in raw_rows:
            vals = self._parse_values(r)
            vals += [None] * (len(self.columns) - len(vals))
            data.append(vals[:len(self.columns)])
        self.df = pd.DataFrame(data, columns=self.columns)
        return self.df

    def transform(self):
        raise NotImplementedError

    def save_csv(self):
        os.makedirs('outputs', exist_ok=True)
        path = os.path.join('outputs', f'{self.output_name}.csv')
        self.df.to_csv(path, index=False)
        return path

    def save_sqlite(self, db_path='outputs/base_etl.db'):
        os.makedirs('outputs', exist_ok=True)
        conn = sqlite3.connect(db_path)
        self.df.to_sql(self.output_name, conn, if_exists='replace', index=False)
        conn.close()
        return db_path
