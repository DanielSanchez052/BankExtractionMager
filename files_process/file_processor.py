import pandas as pd
from files_process.etls.etl import ETL
import constants


class FileProcessor:
    def __init__(self, settings):
        self.settings = settings
        self.etl = ETL(settings)
        self.banks = {
            "bbva": self._process_bbva,
            "nequi": self._process_nequi
        }

    def process_file(self, bank_name, *args, **kwargs) -> pd.DataFrame:
        """Process a file for a given bank.

        This method will call the corresponding bank processor and return the log
        DataFrame.

        Args:
            bank_name: The name of the bank to process.
            *args: Arguments to pass to the ETL pipeline.
            **kwargs: Keyword arguments to pass to the ETL pipeline.

        Returns:
            pd.DataFrame: A DataFrame with the log of the processing.
        """
        log = pd.DataFrame(columns=["identifier", "message"])
        if bank_name.strip().lower() not in self.banks:
            return log

        return self.banks[bank_name.strip().lower()](*args, **kwargs)

    def _process_bbva(self, *args, **kwargs):
        """Process BBVA files.

        This method will run both the transactions and resume pipelines and
        concatenate the logs into a single DataFrame.

        Args:
            *args: Arguments to pass to the ETL pipeline.
            **kwargs: Keyword arguments to pass to the ETL pipeline.

        Returns:
            pd.DataFrame: A DataFrame with the concatenated logs.
        """
        log = self.etl.run(constants.PROCESS_BBVA_TRANSACTIONS_ETL, *args, **kwargs)
        return log

    def _process_nequi(self, *args, **kwargs):
        """Process Nequi files.

        This method will run both the transactions and resume pipelines and
        concatenate the logs into a single DataFrame.

        Args:
            *args: Arguments to pass to the ETL pipeline.
            **kwargs: Keyword arguments to pass to the ETL pipeline.

        Returns:
            pd.DataFrame: A DataFrame with the concatenated logs.
        """
        log = pd.DataFrame(columns=["identifier", "message"])
        log_transactions = self.etl.run(constants.PROCESS_NEQUI_TRANSACTIONS_ETL, *args, **kwargs)
        # log_resume = self.etl.run(constants.PROCESS_RESUME_ETL, *args, **kwargs)
        log = pd.concat([log_transactions], ignore_index=True)
        return log
