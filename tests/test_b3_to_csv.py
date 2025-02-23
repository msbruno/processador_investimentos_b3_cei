import os
from pathlib import Path
import unittest
from src.b3_to_csv import B3CsvProcessor, DataCei

def get_resource(file):
    """Define a function to get resource from directory test.resource
    """
    return (os.path.join(os.path.dirname(__file__),
                   '',
                   'resources', 
                   file))

class TestB3CsvProcessor(unittest.TestCase):
    """Test Class for CsvProcessor
    """
    def test_create_treated_dataframe_from_xlsx(self):
        """Test if is possible to create a treated dataframe from a xlsx file
        """
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
    """Test Class for DataCei
    """
    def test_save_to_file(self):
        """Test if is possible to save a treated dataframe to a csv file
        """
        path_xlsx = get_resource('Teste_PY_Negociacao.xlsx')
        processor = B3CsvProcessor()
        cei_data = processor.create_treated_dataframe(path_xlsx)
        path_to_save = get_resource("bkp.csv")
        cei_data.save_to_csv(path_to_save)

        my_bkp_file = Path(path_to_save)
        self.assertTrue(my_bkp_file.is_file())

    def test_load_data(self):
        """Test if is possible to load a treated csv file into a DataCei class
        """
        data = DataCei()
        data.load_data(get_resource("bkp.csv"))
        operations = data.df.groupby("ticker")['operacao'].count().to_dict()
        self.assertTrue('HGLG11' in operations)
        self.assertTrue('HGLG11' in operations)
        self.assertEqual(3, operations['HGLG11'])
        self.assertEqual(2, operations['SUZB3'])

if __name__ == "__main__":
    unittest.main()
