# BankExatractor

## Description

BankExatractor is a Python-based project designed to extract, transform, and load (ETL) financial transaction data from various sources, including BBVA and Nequi, into a structured database. It also includes a Discord bot for managing and monitoring the ETL processes.

## Installation

1.  Clone the repository:

    ```bash
    git clone https://github.com/DanielSanchez052/BankExtractionMager
    ```

2.  Install the dependencies:

    ```bashsum(credits) 
    pip install -r requirements.txt
    ```

3.  Configure the settings:

    *   Modify the `settings.json` file to configure database connections, API keys, and other settings.
    *   Update the `constants.py` file with any project-specific constants.

## Usage

1.  Run the main script:

    ```bash
    python main.py
    ```

2.  Interact with the Discord bot:

    *   Invite the bot to your Discord server.
    *   Use the bot commands to manage and monitor the ETL processes.

## Project Structure

*   `.gitignore`: Specifies intentionally untracked files that Git should ignore.
*   `constants.py`: Defines project-specific constants.
*   `logger.py`: Configures logging for the project.
*   `main.py`: The main script to run the ETL processes and Discord bot.
*   `requirements.txt`: Lists the project's dependencies.
*   `settings.json`: Contains configuration settings for the project.
*   `settings.py`: Loads settings from `settings.json`.
*   `discord_bot/`: Contains the Discord bot implementation.
    *   `bot.py`: The main Discord bot script.
    *   `manage_extact_channel.py`: Manages the extraction channel in Discord.
*   `files_process/`: Contains modules for processing files.
    *   `file_processor.py`: Processes different types of files.
    *   `etls/`: Contains ETL-related modules.
        *   `etl.py`: Defines the base ETL class.
        *   `tasks.json`: Defines ETL tasks.
        *   `utils.py`: Provides utility functions for ETL processes.
        *   `extract/`: Contains modules for extracting data.
            *   `extract_csv.py`: Extracts data from CSV files.
            *   `extract_pdf.py`: Extracts data from PDF files.
        *   `load/`: Contains modules for loading data.
            *   `to_sql.py`: Loads data into a SQL database.
        *   `pipeline/`: Contains modules for defining ETL pipelines.
            *   `pipeline.py`: Defines the ETL pipeline class.
        *   `steps/`: Contains modules for defining ETL steps.
            *   `execute_query.py`: Executes SQL queries.
            *   `execute_sp.py`: Executes stored procedures.
            *   `post_load.py`: Performs post-load operations.
            *   `transform_bbva_transactions.py`: Transforms BBVA transaction data.
            *   `transform_nequi_transactions.py`: Transforms Nequi transaction data.
            *   `transform_resume.py`: Transforms resume data.
*   `files_process/extractors/`: Contains modules for extracting data from different file formats.
    *   `pdf_extractor.py`: Extracts text from PDF files.
*   `files_process/files/`: Contains input files to be processed.
*   `files_process/processed/`: Contains processed files.
*   `files_process/to_process/`: Contains files waiting to be processed.
*   `logs/`: Contains log files.
*   `testfiles/`: Contains test files.

## License

[Specify the license here]
