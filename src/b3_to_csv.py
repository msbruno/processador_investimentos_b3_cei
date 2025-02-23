import pandas as pd

class DataCei:
    def __init__(self, df:pd.DataFrame=None):
        self.df = df
    
    def load_data(self, path:str):
        df_to_concat = pd.read_csv(path:path, sep=';')
        df_to_concat['data'] = pd.to_datetime(df_to_concat['data'], format='%d/%m/%Y', dayfirst=True)
        df_to_concat = df_to_concat.sort_values('data')
        df_to_concat = pd.concat([df_to_concat, self.df], ignore_index=True)
        if self.df is None:
            self.df = df_to_concat
        else: 
            df_to_concat = df_to_concat.sort_values(['data'])
        self.df = df_to_concat
            
    def save_to_csv(self, path:str) -> pd.DataFrame:
        if self.df is None:
            raise Exception('Data is empty')
        self.df['data'] =  self.df['data'].dt.strftime('%d/%m/%Y')
        self.df.to_csv(path, ';', index=False)
    
    
class B3CsvProcessor:
    def __init__(self, fees:dict=None, asset_type:dict=dict()):
        self.__fees = fees
        self.__asset_type = asset_type
        pass
    
    def __rename_columns(self, df:pd.DataFrame):
        columns={'Data do Negócio':'data', 'Tipo de Movimentação': 'operacao', 'Código de Negociação': 'ticker', 'Quantidade': 'qtd', 'Preço': 'pm'}
        return df.rename(columns=columns)

    def __treat_datatypes(self, df:pd.DataFrame):
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y', dayfirst=True)
        return df

    def __add_fees(self, df:pd.DataFrame):
        df['corretagem'] = 0
        if self.__fees is not None:
            df['corretagem'] = df.apply(lambda x: self.__fees[x['Instituição']], axis=1)
        return df

    def __remove_unecessary_columns(self, df:pd.DataFrame):
        return df.drop(['Mercado', 'Prazo/Vencimento', 'Valor', 'Instituição'], axis=1)

    def __create_str_date(self, df:pd.DataFrame):
        df['data_str'] = df['data'].dt.strftime('%d/%m/%Y')
        return df

    def __remove_fraction_identifier_from_sticker(self, df):
        df['ticker'] = df['ticker'].apply(lambda x : x[:-1] if x[-1] == 'F' else x)
        return df

    def __treat_operation(self, df):
        temp = df.copy()
        op = {'Compra': 'COMPRADA', 'Venda': 'VENDIDA'}
        temp['operacao'] = temp.apply(lambda x: op[x['operacao']], axis=1)
        return temp

    def __treat_columns(self, df_original:pd.DataFrame):
        df = df_original.copy()
        df = self.__rename_columns(df)
        df = self.__treat_datatypes(df)
        df = self.__add_fees(df)
        df = self.__create_str_date(df)
        df = self.__remove_unecessary_columns(df)
        df = self.__remove_fraction_identifier_from_sticker(df)
        df = self.__treat_operation(df)
        return df

    def __treat_duplicated_operations_at_day(self, df):
        temp = df.copy()
        temp['value'] = temp['pm'] * temp['qtd']
        temp = temp.groupby(['data','data_str', 'ticker', 'operacao']).sum(['value', 'qtd']).reset_index()
        temp['pm'] = temp['value'] / temp['qtd']
        return temp

    def __sort(self, df:pd.DataFrame):
        return df.sort_values(['data', 'ticker', 'corretagem'])

    def __func(self, x, operation):
        return (x['qtd'], x['pm']) if operation == x['operacao'] else (0,0)

    def __create_column_for_operation(self, df):
        temp = df.copy()
        temp['qtd_venda'], temp['pm_venda'] = zip(*temp.apply(lambda x:self.__func(x, 'VENDIDA'), axis=1))
        temp['qtd_compra'], temp['pm_compra'] = zip(*temp.apply(lambda x:self.__func(x, 'COMPRADA'), axis=1))
        return temp

    #TODO - create tipo based on stock type
    def __map_type(self, df):
        temp = df.copy()
        temp['tipo'] = ''
        return temp

    def create_treated_dataframe(self, path_xlsx: str)-> DataCei:
        df = pd.read_excel(path_xlsx)
        df = self.__treat_columns(df)
        df = self.__sort(df)
        df = self.__treat_duplicated_operations_at_day(df)
        df = self.__create_column_for_operation(df)
        df = self.__map_type(df)
        df['data'] = pd.to_datetime(df['data_str'], format='%d/%m/%Y', dayfirst=True)
        self.__result = df[['ticker','data','operacao','tipo','corretagem', 'qtd_compra', 'qtd_venda', 'pm_compra', 'pm_venda']]
        return DataCei(self.__result)

