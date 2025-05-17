import os
import pandas as pd
import glob
from settings import Settings


def extract(log, *args) -> pd.DataFrame:
    settings = Settings()

    if (not os.path.exists(settings.PROCESS_DIR)):
        os.mkdir(settings.PROCESS_DIR)

    all_files = glob.glob(os.path.join(settings.PROCESS_DIR, "*.csv"))

    dfs = pd.concat((pd.read_csv(f, sep=settings.SEPARATOR) for f in all_files))

    return dfs, log
