import configparser
import tempfile
import morph_kgc
import json
import os
from logging_config import setup_logger

# Set up logger
logger = setup_logger()


def validate_class_attribute_connection_file(df):
    """
    Validates that all attribute values in the class_attribute_connection file exist in the DataFrame's
    column names.
    """
    try:
        class_attribute_connection_file_path = "./class_attribute_connection.json"
        logger.info(f"Validating class_attribute_connection.json at {class_attribute_connection_file_path}...")

        with open(class_attribute_connection_file_path, "r") as json_file:
            mappings = json.load(json_file)

        df_columns = set(df.columns)
        for key, values in mappings.items():
            for value in values:
                if value.strip() not in df_columns:
                    logger.error(f"Invalid attribute '{value}' found in {class_attribute_connection_file_path}")
                    raise ValueError(
                        f"Please check the class_attribute_connection.json file. "
                        f"You have provided an attribute '{value}' in this file which does not exist among the "
                        f"dataset fields."
                    )
        logger.info("Validation of class_attribute_connection.json completed successfully.")

    except json.JSONDecodeError as json_error:
        logger.error(f"Invalid JSON format in class_attribute_connection.json: {json_error}")
        raise ValueError(f"Invalid JSON file format: {json_error}")
    except FileNotFoundError as fnf_error:
        logger.error(f"JSON file not found: {fnf_error}")
        raise FileNotFoundError(f"JSON file not found: {fnf_error}")


def dataset_rml_mapping_manipulator():
    """
    Manipulates an RML TTL file based on the class_attribute_connection provided.
    """
    ttl_input_path = "./dataset_rml_template.ttl"
    ttl_output_path = "./Stage_dir/rml_dataset_mapping.ttl"
    json_file_path = "./class_attribute_connection.json"

    try:
        logger.info("Starting dataset RML mapping manipulation...")

        # Load the class_attribute_connection file
        with open(json_file_path, "r") as json_file:
            mappings = json.load(json_file)

        # Load the TTL file
        with open(ttl_input_path, "r") as ttl_file:
            ttl_content = ttl_file.readlines()

        logger.debug("TTL template and JSON mappings loaded successfully.")

        updated_ttl_content = []
        current_segment = []
        in_segment = False
        segment_name = None

        # Process the TTL file line by line
        for line in ttl_content:
            stripped_line = line.strip()

            if stripped_line.startswith("<#") and ">" in stripped_line:
                if in_segment and current_segment and segment_name:
                    for key, values in mappings.items():
                        if segment_name == key:
                            for value in values:
                                if value.strip():
                                    current_segment.insert(-1, f"""
    rr:predicateObjectMap [
        rr:predicate lw:{value};
        rr:objectMap [ rr:column "{value}" ];
    ];""")
                    updated_ttl_content.extend(current_segment)
                    current_segment = []

                in_segment = True
                segment_name = stripped_line.split("<#")[1].split(">")[0]
                current_segment.append(line)
            elif in_segment:
                current_segment.append(line)
                if stripped_line == ".":
                    for key, values in mappings.items():
                        if segment_name == key:
                            for value in values:
                                if value.strip():
                                    current_segment.insert(-1, f"""
    rr:predicateObjectMap [
        rr:predicate lw:{value};
        rr:objectMap [ rr:column "{value}" ];
    ];""")
                    updated_ttl_content.extend(current_segment)
                    current_segment = []
                    in_segment = False
                    segment_name = None
            else:
                updated_ttl_content.append(line)

        # Handle last segment
        if in_segment and current_segment and segment_name:
            for key, values in mappings.items():
                if segment_name == key:
                    for value in values:
                        if value.strip():
                            current_segment.insert(-1, f"""
    rr:predicateObjectMap [
        rr:predicate lw:{value};
        rr:objectMap [ rr:column "{value}" ];
    ];""")
            updated_ttl_content.extend(current_segment)

        os.makedirs(os.path.dirname(ttl_output_path), exist_ok=True)
        with open(ttl_output_path, "w") as ttl_file:
            ttl_file.writelines(updated_ttl_content)

        logger.info(f"Dataset RML mapping successfully written to {ttl_output_path}")

    except json.JSONDecodeError as json_error:
        logger.error(f"Invalid JSON file format: {json_error}")
        raise ValueError(f"Invalid JSON file format: {json_error}")
    except FileNotFoundError as fnf_error:
        logger.error(f"File not found: {fnf_error}")
        raise FileNotFoundError(f"Error: File not found - {fnf_error}")
    except Exception as e:
        logger.error(f"Unexpected error during dataset RML manipulation: {e}")
        raise


def dataset_config():
    config = configparser.ConfigParser()
    config['DataSource1'] = {
        'mappings': "./Stage_dir/rml_dataset_mapping.ttl"
    }

    try:
        logger.info("Creating dataset configuration temporary file...")
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_config:
            config.write(temp_config)
            logger.info(f"Dataset configuration file created at: {temp_config.name}")
            return temp_config.name
    except FileNotFoundError:
        logger.error("Temporary file could not be created. File system error.")
        raise IOError("Error: Temporary file could not be created. Check your file system.")
    except PermissionError:
        logger.error("Permission denied while creating/writing the temporary file.")
        raise IOError("Error: Permission denied while creating or writing to the temporary file.")
    except Exception as e:
        logger.error(f"Unexpected error creating dataset config: {e}")
        raise IOError(f"An unexpected error occurred: {e}")


def dataset_rml_mapper(attribute_df, config):
    try:
        logger.info("Starting RDF materialization process...")

        if not os.path.exists(config):
            logger.error(f"Configuration file not found at {config}")
            raise FileNotFoundError(f"Error: Configuration file not found at {config}")

        graph = morph_kgc.materialize(config)
        if not graph:
            logger.error("Failed to materialize the RDF graph.")
            raise ValueError("Error: Failed to materialize the RDF graph. Check the dataset RML configuration.")

        graph.bind("lw", "https://kos.lifewatch.eu/ontologies/lw/")

        dataset_label = attribute_df['datasetLabel'].iloc[0]
        output_dir = os.path.join("./Output_dir", dataset_label)
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, "rdf_dataset.ttl")
        graph.serialize(destination=output_file, format="turtle")

        logger.info(f"RDF data successfully written to {output_file}")

        return graph

    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        raise
    except PermissionError:
        logger.error("Permission denied while writing RDF output.")
        raise PermissionError("Error: Permission denied while accessing or writing the output file.")
    except ValueError as e:
        logger.error(f"Value error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during RDF materialization: {e}")
        raise IOError(f"An unexpected error occurred: {e}")
