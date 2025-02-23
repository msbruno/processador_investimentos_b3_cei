import pandas as pd

class DataCei:
    """Treated dataset from CEI's xlsx
    """
    def __init__(self, df:pd.DataFrame=None):
        self.df = df
        self.__date_format = '%d/%m/%Y'

    def load_data(self, path:str):
        """Load data from csv path
        """
        df = pd.read_csv(path, sep=';')
        df['data'] = pd.to_datetime(df['data'], 
                                    format=self.__date_format, 
                                    dayfirst=True)
        df = df.sort_values('data')
        df = pd.concat([df, self.df], ignore_index=True)
        df = df.sort_values(['data'])
        self.df = df
     
    def save_to_csv(self, path:str) -> pd.DataFrame:
        """Save data into a csv file
        """
        if self.df is None:
            raise Exception('Data is empty')
        self.df['data'] =  self.df['data'].dt.strftime(self.__date_format)
        self.df.to_csv(path, ';', index=False)
        