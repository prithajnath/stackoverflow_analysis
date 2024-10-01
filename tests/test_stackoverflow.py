from stackexchangedump import StackOverflowDump

import pandas as pd


class TestStackOverflow:
    def test_ok(self, stackoverflow_csv_dumper):
        filename = stackoverflow_csv_dumper.convert_to_csv()

        df = pd.read_csv(filename)

        assert df["Score"].sum() == 110
