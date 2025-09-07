from logging_config import setup_logger
from metadata_functions import (
    process_metadata_xml,
    metadata_config,
    metadata_rml_mapper,
    metadata_rml_mapping,
    add_unique_ids_to_xml,
)

# Set up the logger
logger = setup_logger()


def run_metadata_workflow():
    """
    Executes the workflow to generate metadata RML mapping, create a temporary
    configuration file, and materialize RDF triples.

    Returns:
        pd.DataFrame: A DataFrame containing the attributes names, Labels, etc.
    """
    try:
        # Step 0: Process the metadata XML and get the attribute DataFrame
        logger.info("Step 0: Processing the metadata file (XML version)...")
        attribute_df, xml_path = process_metadata_xml()
        add_unique_ids_to_xml(xml_path)
        logger.info("Metadata processed successfully.")

        # Step 1: Generate the Metadata RML Mapping
        logger.info("Step 1: Generating Metadata RML Mapping...")
        metadata_rml_mapping()
        logger.info("Metadata RML mapping successfully created.")

        # Step 2: Create the Metadata Configuration File
        logger.info("Step 2: Creating Metadata Configuration File...")
        config_file_path = metadata_config()
        logger.info(f"Temporary configuration file created at: {config_file_path}")

        # Step 3: Materialize RDF Data using Metadata RML Mapper
        logger.info("Step 3: Materializing RDF Data...")
        metadata_rml_mapper(attribute_df, config_file_path)
        logger.info("RDF data materialization completed successfully.")

        return attribute_df

    except FileNotFoundError as e:
        logger.error(f"File error during metadata workflow: {e}")
        raise
    except ValueError as e:
        logger.error(f"Value error during metadata workflow: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in metadata workflow: {e}")
        raise
