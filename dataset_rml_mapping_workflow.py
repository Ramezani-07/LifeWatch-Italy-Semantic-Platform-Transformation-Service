from logging_config import setup_logger
from dataset_rml_mapping_functions import (
    dataset_rml_mapping_manipulator,
    dataset_config,
    dataset_rml_mapper,
    validate_class_attribute_connection_file
)

# Set up the logger
logger = setup_logger()


def run_dataset_rml_mapping_workflow(df, attribute_df):
    """
    Executes the workflow to generate dataset RML mapping, create a temporary
    configuration file, and materialize RDF triples.
    """
    try:
        # Step 1: Generate the Dataset RML Mapping
        logger.info("Step 1: Generating Dataset RML Mapping...")
        validate_class_attribute_connection_file(df)
        dataset_rml_mapping_manipulator()
        logger.info("Dataset RML mapping successfully created.")

        # Step 2: Create the Dataset Configuration File
        logger.info("Step 2: Creating Dataset Configuration File...")
        config_file_path = dataset_config()
        logger.info(f"Temporary configuration file created at: {config_file_path}")

        # Step 3: Materialize RDF Data using Dataset RML Mapper
        logger.info("Step 3: Materializing RDF Data...")
        dataset_rml_mapper(attribute_df, config_file_path)
        logger.info("RDF data materialization completed successfully.")

    except FileNotFoundError as e:
        logger.error(f"File not found error in dataset RML mapping workflow: {e}")
        raise
    except ValueError as e:
        logger.error(f"Value error in dataset RML mapping workflow: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in dataset RML mapping workflow: {e}")
        raise
