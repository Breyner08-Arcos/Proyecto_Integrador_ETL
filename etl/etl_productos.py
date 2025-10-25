from .base_etl import ETLBase
import pandas as pd

class ETLProductos(ETLBase):
    def transform(self):
        self.df['precio'] = pd.to_numeric(self.df.get('precio'), errors='coerce')
        self.df['stock'] = pd.to_numeric(self.df.get('stock'), errors='coerce')
        self.df['costo'] = pd.to_numeric(self.df.get('costo'), errors='coerce')
        self.df = self.df.drop_duplicates(subset=['codigo']).reset_index(drop=True)
        return self.df
