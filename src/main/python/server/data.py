import pandas as pd
from sdv.io.local import CSVHandler
from sdv.metadata import Metadata
from sdv.multi_table import HMASynthesizer
from base import TmpContext


class Data:

    def __init__(self, ctx: TmpContext):
        connector = CSVHandler()
        path_data = ctx.path_data
        self.data = connector.read(folder_name=path_data)
        self.metadata = Metadata.detect_from_dataframes(self.data)
        self.synthesizer = HMASynthesizer(self.metadata)
        self.synthesizer.fit(self.data)
        self.synthetic_data = self.synthesizer.sample()

    def sample(self, scale: float) -> pd.DataFrame:
        self.synthetic_data = self.synthesizer.sample(scale=scale)
        return self.synthetic_data


