""" the base of this pipeline system has extracted from https://github.com/rob-dalton/pandas-etl-pipeline/wiki/Examples"""
from pandas import DataFrame, isna
from typing import List, Union


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


class Pipeline(object):
    """Class to create and run a data pipeline for a Pandas Dataframe.

    ATTRIBUTES
    - source: The data source for the pipeline. Either a DataFrame object or fpath of CSV file to read.
    - extract: (Optional) The Step to run for extraction.
    - transformations: List of Steps and Transforms to run.
    - load: (Optional) The final Step in a pipeline. Should save or
            pass Pipeline.data somewhere.
    """

    def __init__(self,
                 source: Union[str, DataFrame],
                 steps: List[Union[Step, Transform, Load]],
                 extract: Step = None,
                 load: Load = None):
        self.data = None
        self.source = source
        self.steps = steps
        self.extract = extract
        self.load = load
        self.log = DataFrame(columns=["identifier", "message"])

    def _extract(self) -> DataFrame:
        """Run Step for extraction.

        Step is passed Pipeline.source as its first positional arg.
        """
        new_args = [args for args in self.extract.args]
        new_args.insert(0, self.source)
        self.extract.args = new_args
        return self.extract.run(self.log)

    def run(self, load=True):
        # set self.data
        if type(self.source) is DataFrame:
            self.data = self.source
        else:
            # Run extraction step if source is fpath
            # NOTE: Wait til run() to call _extract() in case
            #       source depends on other Pipelines.
            self.data, self.log = self._extract()

        if (len(self.data) <= 0):
            self.log = insert_row(self.log, ["error", "!!WARNING¡¡ data extracted is empty"])
            print("!!WARNING¡¡ data is empty")
        else:
            # Run steps
            for step in self.steps:
                if isinstance(step, Load):
                    step.run(self.data, self.log)
                elif isinstance(step, Transform):
                    # update self.data with Transform
                    self.data, self.log = step.run(self.data, self.log)
                else:
                    self.log = step.run(self.log)

        # Run load step
        if self.load is not None:
            self.load.run(self.data, self.log)


def save_to_csv(df: DataFrame, destination: str):
    df.to_csv(destination, index=False)


def insert_row(df: DataFrame, row: List):
    insert_loc = df.index.max()

    if isna(insert_loc):
        df.loc[0] = row
    else:
        df.loc[insert_loc + 1] = row

    return df
