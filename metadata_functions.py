import configparser
import tempfile
import morph_kgc
import pandas as pd
import xml.etree.ElementTree as ET
import random
import os
import shutil
from logging_config import setup_logger

# Set up logger
logger = setup_logger()


def process_metadata_xml(input_dir="./Input_dir"):
    """
    Process the metadata.xml file, validate its presence and name,
    and parse its content into a Pandas DataFrame.

    Args:
        input_dir (str): Path to the directory containing the XML file.

    Returns:
        tuple: (pd.DataFrame, str) A DataFrame with parsed data, and the XML path.
    """
    try:
        logger.info(f"Looking for metadata.xml in directory: {input_dir}")

        if not os.path.isdir(input_dir):
            raise FileNotFoundError(f"Input directory '{input_dir}' does not exist.")

        xml_files = [file for file in os.listdir(input_dir) if file.endswith(".xml")]
        logger.info(f"Found {len(xml_files)} XML file(s) in input directory.")

        if len(xml_files) == 0:
            raise FileNotFoundError("No XML file found in the input directory.")
        elif len(xml_files) > 1:
            raise ValueError("More than one XML file found in the input directory.")

        xml_filename = xml_files[0]
        xml_path = os.path.join(input_dir, xml_filename)
        logger.info(f"Using XML file: {xml_filename}")

        tree = ET.parse(xml_path)
        root = tree.getroot()

        ns = {'eml': 'https://eml.ecoinformatics.org/eml-2.2.0'}
        dataset_id = root.attrib.get('packageId')
        dataset_label = root.findtext('dataset/title', namespaces=ns)

        if not dataset_id and dataset_label:
            raise ValueError("Dataset ID or title not found in metadata.xml.")

        data = [{"datasetId": dataset_id, "datasetLabel": dataset_label}]
        attribute_df = pd.DataFrame(data)

        logger.info(f"Parsed datasetId: {dataset_id}, datasetLabel: {dataset_label}")

        return attribute_df, xml_path

    except FileNotFoundError as e:
        logger.error(f"File error in process_metadata_xml: {e}")
        raise
    except ET.ParseError as e:
        logger.error(f"Error parsing XML file: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in process_metadata_xml: {e}")
        raise


def add_unique_ids_to_xml(xml_path):
    """
    Adds a unique 10-digit id to specific tags if they don't have one.
    Operates on hardcoded input and output directories.
    """
    source = xml_path
    target = "./Stage_dir/transformed_metadata_prepared_for_mapping.xml"

    try:
        logger.info(f"Adding catalog ID to XML file: {source}")

        ET.register_namespace("eml", "https://eml.ecoinformatics.org/eml-2.2.0")
        ET.register_namespace("stmml", "http://www.xml-cml.org/schema/stmml-1.2")
        ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

        tree = ET.parse(source)
        root = tree.getroot()

        root.set('id', '61175251-1672-4795-b23b-b35b0dd0a294')

        tags_to_check = ['project', 'contact', 'creator', 'metadataProvider', 'associatedParty', 'publisher']
        updated_count = 0

        for tag in tags_to_check:
            for elem in root.findall(f".//{tag}"):
                if 'id' not in elem.attrib:
                    unique_id = ''.join(random.choices('0123456789', k=10))
                    elem.set('id', unique_id)
                    updated_count += 1

                user_id_elem = elem.find("userId")
                if user_id_elem is not None:
                    directory = user_id_elem.get("directory")
                    user_id_value = user_id_elem.text.strip() if user_id_elem.text else ""
                    if directory and user_id_value:
                        user_id_elem.text = f"{directory}{user_id_value}"
                        user_id_elem.attrib.pop("directory", None)

        os.makedirs(os.path.dirname(target), exist_ok=True)
        tree.write(target, encoding='utf-8', xml_declaration=True)

        logger.info(f"Added unique IDs to {updated_count} elements.")
        logger.info(f"Upgraded XML saved to {target}. Added IDs to {updated_count} elements.")

    except ET.ParseError as e:
        logger.error(f"XML Parsing Error: {e}")
        raise
    except FileNotFoundError:
        logger.error(f"Source file {source} not found.")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in add_unique_ids_to_xml: {e}")
        raise


def metadata_rml_mapping(template_file_path="./metadata_rml_template.ttl"):
    try:
        logger.info(f"Generating metadata RML mapping from template: {template_file_path}")

        with open(template_file_path, 'r') as template_file:
            rml_template = template_file.read()

        output_file_path = './Stage_dir/rml_metadata_mapping.ttl'
        with open(output_file_path, 'w') as output_file:
            output_file.write(rml_template)

        logger.info(f"Metadata RML mapping file created at: {output_file_path}")
    except FileNotFoundError:
        logger.error(f"Template file not found at {template_file_path}")
        raise
    except PermissionError:
        logger.error(f"Permission denied when accessing {template_file_path}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in metadata_rml_mapping: {e}")
        raise


def metadata_config():
    config = configparser.ConfigParser()
    config['DataSource1'] = {'mappings': "./Stage_dir/rml_metadata_mapping.ttl"}

    try:
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_config:
            config.write(temp_config)
            logger.info(f"Temporary config file created: {temp_config.name}")
            return temp_config.name
    except FileNotFoundError:
        logger.error("Temporary file could not be created. Check your file system.")
        raise
    except PermissionError:
        logger.error("Permission denied while creating/writing to the temporary file.")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in metadata_config: {e}")
        raise


def metadata_rml_mapper(attribute_df, config):
    try:
        logger.info(f"Starting RDF materialization using config: {config}")

        if not os.path.exists(config):
            raise FileNotFoundError(f"Configuration file not found at {config}")

        graph = morph_kgc.materialize(config)
        if not graph:
            raise ValueError("Failed to materialize the RDF graph. Check the RML configuration.")

        graph.bind("lw", "https://kos.lifewatch.eu/ontologies/lw/")
        graph.bind("dwc", "http://rs.tdwg.org/dwc/terms/")
        graph.bind("dct", "http://purl.org/dc/terms/")

        dataset_label = attribute_df['datasetLabel'].iloc[0]
        output_dir = os.path.join("./Output_dir", dataset_label)

        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
            logger.warning(f"Existing output directory removed: {output_dir}")

        os.makedirs(output_dir)
        output_file = os.path.join(output_dir, "rdf_metadata.ttl")

        graph.serialize(destination=output_file, format="turtle")
        logger.info(f"RDF data successfully written to {output_file}")

        return graph

    except FileNotFoundError as e:
        logger.error(f"File error in metadata_rml_mapper: {e}")
        raise
    except PermissionError:
        logger.error("Permission denied while accessing/writing output file.")
        raise
    except ValueError as e:
        logger.error(f"Value error in metadata_rml_mapper: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in metadata_rml_mapper: {e}")
        raise
