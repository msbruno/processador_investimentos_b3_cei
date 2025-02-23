import unittest
from src.b3_to_csv import B3CsvProcessor
import os

def get_resource(file):
    return (os.path.join(os.path.dirname(__file__), 
                   '',
                   'resources', 
                   'Teste_PY_Negociacao.xlsx'))

class TestB3CsvProcessor(unittest.TestCase):
    def test_create_treated_dataframe_from_xlsx(self):
        path_xlsx = get_resource('Teste_PY_Negociacao.xlsx')
        processor = B3CsvProcessor()
        cei_data = processor.create_treated_dataframe(path_xlsx)
        df = cei_data.df
        tickers = df['ticker'].unique()
        assert len(tickers) == 2 
        operations = df.groupby("ticker")['operacao'].count().to_dict()
        assert 'HGLG11' in operations
        assert 'HGLG11' in operations
        assert operations['HGLG11'] == 3
        assert operations['SUZB3'] == 2

if __name__ == "__main__":
    unittest.main()