""" the base of this pipeline system has extracted from https://github.com/rob-dalton/pandas-etl-pipeline/wiki/Examples"""
from pandas import DataFrame, isna
from typing import List, Union

from files_process.etls.utils import insert_row
from logger import setup_logger


class Step(object):
    """Step to run in a Pipeline.

    A Step is a function and a set of arguments that
    are called during Pipeline.run().
    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self, log):
        return self.func(log, *self.args, **self.kwargs)


class Transform(Step):
    """Transform to run in a data pipeline.

    A Transform is a subclass of Step. When run, its function
    is passed Pipeline.data as the first positional argument.
    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self, data: DataFrame, log: DataFrame):
        return self.func(data, log, *self.args, **self.kwargs)


class Load(Transform):
    """Load to run in a data pipeline.

    A Load is a subclass of Transform. It requires a
    destination keyword argument (indicates where the data will be
    saved or passed to).
    """
    def __init__(self, *args, **kwargs):
        if kwargs.get('destination') is None:
            raise Exception("No destination provided for Load.")
        super(Load, self).__init__(*args, **kwargs)


class Pipeline:
    """Class to create and run a data pipeline for a Pandas DataFrame.

    Attributes:
    - source: The data source for the pipeline. Either a DataFrame object or file path of a CSV file to read.
    - steps: List of Steps, Transforms, and Load operations to run.
    - extract: (Optional) The Step to run for extraction.
    - load: (Optional) The final Step in a pipeline. Should save or pass Pipeline.data somewhere.
    - post_load: (Optional) The Step to run after loading.
    - logger: Logger for the pipeline.
    """

    def __init__(self,
                 source: Union[str, DataFrame],
                 steps: List[Union[Step, Transform, Load]],
                 extract: Step = None,
                 load: Load = None,
                 post_load: Step = None,
                 logger=None):
        self.data = None
        self.source = source
        self.steps = steps
        self.extract = extract
        self.load = load
        self.post_load = post_load
        self.log = DataFrame(columns=["identifier", "message"])
        self.logger = logger or setup_logger("etl_pipeline")

    def _extract(self, **kwargs) -> DataFrame:
        """Run the extraction Step."""
        return self.extract.run(self.log, **kwargs)

    def run(self, load=True, **kwargs) -> DataFrame:
        if isinstance(self.source, DataFrame):
            self.data = self.source
        else:
            self.data, self.log = self._extract()

        if self.data.empty:
            self.log = insert_row(self.log, ["error", "!!WARNING¡¡ data extracted is empty"])
            return self.log

        for step in self.steps:
            if isinstance(step, Transform):
                self.data, self.log = step.run(self.data, self.log)
            else:
                self.log = step.run(self.log)

        if self.load:
            _, self.log = self.load.run(self.data, self.log)

        if self.post_load:
            self.log = self.post_load.run(self.log)

        return self.log
