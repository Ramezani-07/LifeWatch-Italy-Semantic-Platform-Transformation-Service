from metadata_rml_mapping_workflow import run_metadata_workflow
from dataset_transformation_workflow import run_dataset_transformation_mapping_workflow
from dataset_rml_mapping_workflow import run_dataset_rml_mapping_workflow
from logging_config import setup_logger

# Set up the logger
logger = setup_logger()


def main():
    try:
        logger.info("Starting the main workflow...")

        # Run the metadata workflow and retrieve the attribute list
        attribute_df = run_metadata_workflow()
        logger.info("Metadata rml mapping workflow completed. Proceeding to the dataset workflow...")

        # Pass the attribute list to the dataset workflow
        df = run_dataset_transformation_mapping_workflow(attribute_df)
        logger.info("Dataset transformation workflow completed. Proceeding to the dataset rml mapping workflow...")

        run_dataset_rml_mapping_workflow(df, attribute_df)
        logger.info("Dataset rml mapping workflow completed.")
        logger.info("Main workflow completed successfully.")

    except FileNotFoundError as e:
        logger.error(f"File not found error in main workflow: {e}")
    except ValueError as e:
        logger.error(f"Value error in main workflow: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in main workflow: {e}")
        raise


if __name__ == "__main__":
    main()
