import pandas as pd

class DataCei:
    def __init__(self, df:pd.DataFrame=None):
        self.df = df
    
    def load_data(self, path:str):
        df_to_concat = pd.read_csv(path, sep=';')
        df_to_concat['data'] = pd.to_datetime(df_to_concat['data'], format='%d/%m/%Y', dayfirst=True)
        df_to_concat = df_to_concat.sort_values('data')
        df_to_concat = pd.concat([df_to_concat, self.df], ignore_index=True)
        df_to_concat = df_to_concat.sort_values(['data'])
        self.df = df_to_concat
            
    def save_to_csv(self, path:str) -> pd.DataFrame:
        if self.df is None:
            raise Exception('Data is empty')
        self.df['data'] =  self.df['data'].dt.strftime('%d/%m/%Y')
        self.df.to_csv(path, ';', index=False)