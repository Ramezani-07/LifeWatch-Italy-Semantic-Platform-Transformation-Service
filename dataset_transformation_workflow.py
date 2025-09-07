import pandas as pd
from logging_config import setup_logger
from dataset_transformation_functions import data_preparation, measurement_attribute_preparation, \
    taxonomy_attribution_preparation, save_dataframe_to_csv, normalize_empty_values


# Set up the logger
logger = setup_logger()


def run_dataset_transformation_mapping_workflow(attribute_df):
    try:
        logger.info("Starting the dataset transformation workflow.")

        # Step 1: data preparation
        logger.info("Step 1: Preparing the general fields...")
        df = data_preparation(attribute_df)
        logger.info("General fields prepared successfully.")

        logger.info("Step 2: Preparing the observation and measurement related fields...")
        df = measurement_attribute_preparation(df, attribute_df)
        logger.info("Observation and measurement related fields prepared successfully.")

        logger.info("Step 3: Preparing the taxonomy related fields...")
        df = taxonomy_attribution_preparation(df)
        logger.info("Taxonomy related fields prepared successfully.")

        df = normalize_empty_values(df)

        if df is not None:
            logger.info("DataFrame is ready!")
            pd.set_option('display.max_columns', None)
            logger.info(f"DataFrame preview:\n{df.head()}")
            logger.info("Step 4: Saving the transformed DataFrame to a CSV file....")
            save_dataframe_to_csv(df)
            return df
        else:
            logger.warning("No DataFrame returned. File processing might have failed.")

    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
    except PermissionError as e:
        logger.error(f"Permission error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during the biotic dataset workflow: {e}")
