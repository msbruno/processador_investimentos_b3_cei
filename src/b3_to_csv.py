import pandas as pd
from src.data_cei import DataCei

class B3CsvProcessor:
    """ Processor of B3 xlsx.
    """

    def __init__(self, fees:dict=None):
        """ Constructor.
        Keyword arguments:
        fees -- dictionary with institution names as key and fees as value 
        """
        self.__date_format = '%d/%m/%Y'
        self.__fees = fees

    def __rename_columns(self, df:pd.DataFrame):
        columns={
            'Data do Negócio':'data', 
            'Tipo de Movimentação': 'operacao', 
            'Código de Negociação': 'ticker', 
            'Quantidade': 'qtd', 
            'Preço': 'pm'
        }
        return df.rename(columns=columns)

    def __treat_datatypes(self, df:pd.DataFrame):
        df['data'] = pd.to_datetime(df['data'],
                                    format=self.__date_format,
                                    dayfirst=True)
        return df

    def __add_fees(self, df:pd.DataFrame):
        df['corretagem'] = 0
        if self.__fees is not None:
            df['corretagem'] = df.apply(lambda x: self.__fees[x['Instituição']], axis=1)
        return df

    def __remove_unecessary_columns(self, df:pd.DataFrame):
        return df.drop(['Mercado',
                        'Prazo/Vencimento',
                        'Valor',
                        'Instituição'], axis=1)

    def __create_str_date(self, df:pd.DataFrame):
        df['data_str'] = df['data'].dt.strftime(self.__date_format)
        return df

    def __remove_fraction_identifier_from_sticker(self, df):
        df['ticker'] = df['ticker'].apply(lambda x : x[:-1] if x[-1] == 'F' else x)
        return df

    def __treat_operation(self, df):
        df['operacao'] = df['operacao'].str.upper()
        return df

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
        group_condition = ['data','data_str', 'ticker', 'operacao']
        temp = temp.groupby(group_condition).sum(['value', 'qtd']).reset_index()
        temp['pm'] = temp['value'] / temp['qtd']
        return temp

    def __sort(self, df:pd.DataFrame):
        return df.sort_values(['data', 'ticker', 'corretagem'])

    def create_treated_dataframe(self, path_xlsx: str)-> DataCei:
        """Create treated Dataframe from B3's xslx .
        Keyword arguments:
        path_xlsx -- file's path 
        """
        df = pd.read_excel(path_xlsx)
        df = self.__treat_columns(df)
        df = self.__sort(df)
        df = self.__treat_duplicated_operations_at_day(df)
        df['data'] = pd.to_datetime(df['data_str'], format=self.__date_format, dayfirst=True)
        result = df[
            [
                'ticker',
                'data',
                'operacao',
                'corretagem', 
                'qtd',
                'pm'
            ]
        ]
        return DataCei(result)
    