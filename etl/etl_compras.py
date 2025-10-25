from .base_etl import ETLBase
import pandas as pd

class ETLCompras(ETLBase):
    def transform(self):
        self.df['fecha'] = pd.to_datetime(self.df.get('fecha'), errors='coerce')
        self.df['cantidad'] = pd.to_numeric(self.df.get('cantidad'), errors='coerce')
        self.df['precio_unitario'] = pd.to_numeric(self.df.get('precio_unitario'), errors='coerce')
        self.df = self.df.dropna(subset=['fecha', 'codigo']).reset_index(drop=True)
        return self.df
