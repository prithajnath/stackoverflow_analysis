from stackexchangedump import StackOverflowDump

import pandas as pd


class TestStackOverflow:
    def test_ok(self, stackoverflow_csv_dumper):
        filename = stackoverflow_csv_dumper.convert_to_csv()

        df1 = pd.read_xml("tests/test_Posts.xml")
        df2 = pd.read_csv(filename)

        assert df1["Score"].sum() == df2["Score"].sum()
