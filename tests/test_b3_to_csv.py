import unittest
from src.b3_to_csv import B3CsvProcessor, DataCei
import os
from pathlib import Path

def get_resource(file):
    return (os.path.join(os.path.dirname(__file__), 
                   '',
                   'resources', 
                   file))

class TestB3CsvProcessor(unittest.TestCase):
    def test_create_treated_dataframe_from_xlsx(self):
        path_xlsx = get_resource('Teste_PY_Negociacao.xlsx')
        processor = B3CsvProcessor()
        cei_data = processor.create_treated_dataframe(path_xlsx)
        df = cei_data.df
        tickers = df['ticker'].unique()
        self.assertEqual(2, len(tickers))
        operations = df.groupby("ticker")['operacao'].count().to_dict()
        self.assertTrue('HGLG11' in operations)
        self.assertTrue('HGLG11' in operations)
        self.assertEqual(3, operations['HGLG11'])
        self.assertEqual(2, operations['SUZB3'])

class TestDataCei(unittest.TestCase):

    def test_save_to_file(self):
        path_xlsx = get_resource('Teste_PY_Negociacao.xlsx')
        processor = B3CsvProcessor()
        cei_data = processor.create_treated_dataframe(path_xlsx)
        path_to_save = get_resource("bkp.csv")
        cei_data.save_to_csv(path_to_save)

        my_bkp_file = Path(path_to_save)
        self.assertTrue(my_bkp_file.is_file())

    def test_load_data(self):
        data = DataCei()
        data.load_data(get_resource("bkp.csv"))
        operations = data.df.groupby("ticker")['operacao'].count().to_dict()
        self.assertTrue('HGLG11' in operations)
        self.assertTrue('HGLG11' in operations)
        self.assertEqual(3, operations['HGLG11'])
        self.assertEqual(2, operations['SUZB3'])
            

if __name__ == "__main__":
    unittest.main()