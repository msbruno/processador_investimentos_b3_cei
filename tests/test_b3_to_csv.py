import unittest
import sys
sys.path.append('../src')
from b3_to_csv import B3CsvProcessor

class TestCalculadora(unittest.TestCase):
    def test_create_treated_dataframe_from_xlsx(self):
        path_xlsx = './resource/Teste_PY_Negociacao.xlsx'
        processor = B3CsvProcessor()
        df = processor.create_treated_dataframe(path_xlsx)
        print(df)
        tickers = df['ticker'].unique()
        assert len(tickers) == 2 
        operations = df.groupby("ticker")['operacao'].count().to_dict()
        assert 'HGLG11' in operations
        assert 'HGLG11' in operations
        assert operations['HGLG11'] == 3
        assert operations['SUZB3'] == 2

if __name__ == "__main__":
    unittest.main()