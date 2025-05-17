import os
import pandas as pd
import glob


# TODO: revisar la variable PROCESS_DIR y el SEPARATOR para ser reemplazada por variables de el task
def extract(log, *args, **kwargs) -> pd.DataFrame:
    
    settings = kwargs
    
    if (not os.path.exists(settings["process_dir"])):
        os.mkdir(settings["process_dir"])

    all_files = glob.glob(os.path.join(settings["process_dir"], "*.csv"))

    dfs = pd.concat((pd.read_csv(f, sep=settings["separator"]) for f in all_files))

    return dfs, log
