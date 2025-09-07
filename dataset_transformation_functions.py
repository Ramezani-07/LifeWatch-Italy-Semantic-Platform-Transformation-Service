import os
import pandas as pd
import json
import uuid
from logging_config import setup_logger

# Set up the logger
logger = setup_logger()


def data_preparation(attribute_df):
    try:
        logger.info("Starting the data preparation process.")

        # Locate the CSV file in the "./Input_dir" directory
        input_dir = "./Input_dir"
        csv_files = [file for file in os.listdir(input_dir) if file.endswith('.csv')]

        if not csv_files:
            logger.error("No CSV file found in the directory './Input_dir'.")
            raise FileNotFoundError("No CSV file found in the directory './Input_dir'.")
        if len(csv_files) > 1:
            logger.error("Multiple CSV files found in the directory './Input_dir'. Ensure there is only one CSV file.")
            raise FileExistsError(
                "Multiple CSV files found in the directory './Input_dir'. Ensure there is only one CSV file.")

        file_path = os.path.join(input_dir, csv_files[0])
        logger.info(f"CSV file found: {file_path}")

        # Detect the delimiter
        with open(file_path, 'r') as f:
            first_line = f.readline()
            if ';' in first_line:
                delimiter = ';'
            elif '\t' in first_line:
                delimiter = '\t'
            elif '|' in first_line:
                delimiter = '|'
            else:
                delimiter = ','

        df = pd.read_csv(file_path, delimiter=delimiter, engine='python')
        logger.info(f"CSV file loaded successfully with delimiter '{delimiter}'.")

        # Step 1: Add dataset Id and label to df
        dataset_id = attribute_df['datasetId'].iloc[0]
        df['datasetId'] = dataset_id
        dataset_label = attribute_df['datasetLabel'].iloc[0]
        df['datasetLabel'] = dataset_label
        logger.info(f"Added datasetId: {dataset_id} and datasetLabel: {dataset_label} to DataFrame.")

        # Step 2: Strip leading and trailing whitespace
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()
        logger.info("Stripped leading/trailing whitespace from string columns.")

        # Step 3: Create 'date' column
        df = create_date_column(df)

        # Step 4: Process coordinates
        df = process_coordinates(df)

        # Step 5: Create 'FeatureOfInterestId' column
        df = feature_of_interest_id_generator(df)

        return df

    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
    except FileExistsError as e:
        logger.error(f"Error: {e}")
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing the file: {e}")
    except KeyError as e:
        logger.error(f"Missing expected column: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


def fix_event_date(date):
    # Function to fix two-digit years and format eventDate properly
    try:
        if "/" in date:  # Convert from dd/mm/yy or dd/mm/yyyy to yyyy-mm-dd
            parts = date.split('/')
            if len(parts[2]) == 4:  # Already a four-digit year
                year, month, day = parts[2], parts[1], parts[0]
            else:  # Two-digit year case
                day, month, year = parts
        elif "-" in date:  # Might have a two-digit year
            parts = date.split('-')
            if len(parts[0]) == 4:  # Already in yyyy-mm-dd
                return date
            year, month, day = parts
        else:
            return date  # Unexpected format

        # Fix two-digit year cases
        year = int(year)
        if year < 40:
            year = f"20{year:02d}"
        elif year < 100:
            year = f"19{year:02d}"
        else:
            year = str(year)  # Keep four-digit years

        return f"{year}-{month}-{day}"  # Convert to yyyy-mm-dd format

    except Exception as e:
        logger.error(f"Error processing eventDate value: {date}. Error: {e}")
        return "InvalidDate"


def replace_missing_month_day_year(df):
    """
    Ensures 'month' and 'day' columns exist, fills missing values with '01', and formats them as two-digit strings.
    Normalizes 'year' to a four-digit string or sets it to None if missing or invalid.
    """

    # Ensure 'month' and 'day' columns exist, fill missing values with 1, format as '01', '02', etc.
    for col in ['month', 'day']:
        df[col] = df.get(col, pd.Series([1] * len(df), index=df.index))  # create column if missing
        df[col] = df[col].fillna(1).astype(int).astype(str).str.zfill(2)

    # Normalize 'year' column to four-digit string
    def normalize_year(y):
        if pd.isna(y):
            return None
        try:
            y = int(float(y))
            if 0 <= y < 40:
                return f"20{y:02d}"
            elif 40 <= y < 100:
                return f"19{y:02d}"
            else:
                return f"{y:04d}"
        except:
            return None

    df['year'] = df['year'].apply(normalize_year) if 'year' in df.columns else pd.Series([None] * len(df),
                                                                                         index=df.index)
    return df


def create_date_column(df):

    if 'eventDate' in df.columns:
        # Process the eventDate column if it exists
        df['eventDate'] = df['eventDate'].astype(str).apply(fix_event_date)
        df.rename(columns={'eventDate': 'date'}, inplace=True)
        logger.info("eventDate exists and renamed to 'date' after processing.")
    else:
        df = replace_missing_month_day_year(df)
        df['date'] = ""
        df['date'] += df['year']
        df['date'] += '-' + df['month']
        df['date'] += '-' + df['day']

        # Drop the 'year', 'month', and 'day' columns after creating the date
        df.drop(columns=['year', 'month', 'day'], inplace=True, errors='ignore')
        logger.info("eventDate not exist and 'year', 'month', 'day' columns used to create 'date'")

    # Convert 'date' column to datetime format, handling errors
    # df['date'] = pd.to_datetime(df['date'], errors='coerce')

    return df


def process_coordinates(df):
    # Process latitude and longitude columns in a DataFrame:
    # Create a 'point' column combining latitude and longitude with custom handling of missing values.

    no_coordinate = pd.NA
    no_lat = "NaN, "
    no_long = ", NaN"

    # Ensure columns exist and handle conversion of comma-separated decimals
    if 'decimalLatitude' in df.columns:
        df['decimalLatitude'] = df['decimalLatitude'].apply(
            lambda x: float(str(x).replace(',', '.')) if pd.notnull(x) else None
        )

    if 'decimalLongitude' in df.columns:
        df['decimalLongitude'] = df['decimalLongitude'].apply(
            lambda x: float(str(x).replace(',', '.')) if pd.notnull(x) else None
        )

        # Create 'point' column with specified rules
        def create_point(lat, lon):
            if lat == "" and lon == "":
                return no_coordinate
            elif lat == "":
                return no_lat + str(lon)
            elif lon == "":
                return str(lat) + no_long
            else:
                return f"{lat}, {lon}"

        df['point'] = df.apply(lambda row: create_point(row['decimalLatitude'], row['decimalLongitude']), axis=1)
        logger.info("Created 'point' column based on latitude and longitude.")

    else:
        if "locality" in df.columns:
            df['point'] = df["locality"]
        else:
            df['point'] = no_coordinate
            logger.info("No coordinates found. Assigned default value 'no_specified_coordinate'.")

    # Add a minus to all values of a column named 'depth' if it exists
    if 'depth' in df.columns:
        try:
            df['depth'] = pd.to_numeric(df['depth'], errors='coerce')
            df['depth'] = -df['depth'].abs()
            logger.info("Converted 'depth' column values to negative.")
        except Exception as e:
            logger.error(f"Error processing 'depth' column: {e}")

    return df


def feature_of_interest_id_generator(df, column_name="featureOfInterestId"):
    try:
        # Validate input
        if not isinstance(df, pd.DataFrame):
            raise TypeError("The input must be a pandas DataFrame.")
        if column_name in df.columns:
            raise ValueError(f"The column '{column_name}' already exists in the DataFrame.")

        # Generate unique IDs
        df[column_name] = [str(uuid.uuid4())[:18] for _ in range(len(df))]

        logger.info(f"Generated '{column_name}' IDs and stored in the DataFrame.")

        return df

    except TypeError as e:
        logger.error(f"TypeError: {e}")
        return df  # Return the original DataFrame unmodified
    except ValueError as e:
        logger.error(f"ValueError: {e}")
        return df  # Return the original DataFrame unmodified
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return df  # Return the original DataFrame unmodified


# Function to clean and infer measurement values
def clean_and_infer_measurement_values(df: pd.DataFrame, keep_strings: bool = True) -> pd.DataFrame:
    try:
        logger.info("Starting to clean and infer 'measurementValue' records.")

        # Step 1: Standardize known invalid placeholders to actual NaN
        invalid_values = [None, "NaN", "NAN", "none", "empty", "Null", ""]
        df["measurementValue"] = df["measurementValue"].replace(invalid_values, pd.NA)

        # Step 2: Count original entries before cleaning
        original_length = len(df)

        # Step 3: Try parsing values as numeric
        numeric_series = pd.to_numeric(df["measurementValue"], errors='coerce')

        # Step 4: If not keeping strings, drop non-numeric ones
        if not keep_strings:
            cleaned_df = df[numeric_series.notna()].copy()
            cleaned_df["measurementValue"] = numeric_series[numeric_series.notna()]
        else:
            # Keep numeric where possible, original string otherwise
            df["numericValue"] = numeric_series
            df["measurementValue_cleaned"] = df.apply(
                lambda row: row["numericValue"] if pd.notna(row["numericValue"]) else row["measurementValue"], axis=1
            )
            cleaned_df = df.dropna(subset=["measurementValue_cleaned"]).copy()
            cleaned_df["measurementValue"] = cleaned_df["measurementValue_cleaned"]
            cleaned_df.drop(columns=["numericValue", "measurementValue_cleaned"], inplace=True)

        # Step 5: Logging
        cleaned_count = len(cleaned_df)
        removed_count = original_length - cleaned_count
        logger.info(f"Removed {removed_count} invalid 'measurementValue' records out of {original_length}.")

        type_counts = cleaned_df["measurementValue"].map(type).value_counts()
        for dtype, count in type_counts.items():
            logger.info(f"Detected {count} values of type {dtype.__name__} in 'measurementValue'.")

        return cleaned_df

    except Exception as e:
        logger.error(f"Error during 'measurementValue' cleaning: {e}")
        return df  # fallback: return original DataFrame


# Function to process measurement attributes (already defined, but placing it next to
# clean_and_infer_measurement_values)
def measurement_attribute_preparation(df: pd.DataFrame, attribute_df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Starting the measurement attributes preparation process.")

        # Step 1: Load the trait list from the JSON file
        trait_dict_path = "trait_dict.json"
        if not os.path.exists(trait_dict_path):
            logger.error(f"Trait dict file not found at: {trait_dict_path}")
            raise FileNotFoundError(f"Trait dict file not found at: {trait_dict_path}")

        with open(trait_dict_path, 'r') as f:
            trait_dict = json.load(f)
            logger.info(f"Trait dict loaded successfully with {len(trait_dict)} traits.")

        # Step 2: Create measurement_list and non_measurement_list
        measurement_list = [col for col in df.columns if col in trait_dict]
        non_measurement_list = [col for col in df.columns if col not in trait_dict]

        # Log the lists for debugging
        logger.info(f"Measurement List: {measurement_list}")
        logger.info(f"Non-Measurement List: {non_measurement_list}")

        # Step 3: Perform the melt operation only if measurement_list is not empty
        if not measurement_list:
            if 'measurementValue' in non_measurement_list:
                logger.warning(
                    "Measurement list is empty, but measurementValue exist in df.")
                df = observation_id_generator(df)
                df = df.rename(columns={
                    'measurementType': 'propertyName',
                    'measurementTypeID': 'propertyUrl'
                })
                df['propertyLabel'] = df['propertyName']
                df["propertyDefinition"] = pd.NA
                return df

            else:
                logger.warning(
                    "Measurement list is empty. Returning the original DataFrame with added observationId field and "
                    "None value for other columns.")
                df = observation_id_generator(df)
                df["propertyName"] = pd.NA
                df["measurementValue"] = pd.NA
                df["propertyLabel"] = pd.NA
                df["propertyDefinition"] = pd.NA
                df["measurementUnit"] = pd.NA
                df["propertyUrl"] = pd.NA
                return df

        melted_df = pd.melt(
            df,
            id_vars=non_measurement_list,
            value_vars=measurement_list,
            ignore_index=False,
            var_name='propertyName',
            value_name='measurementValue'
        )

        logger.info("Melt operation completed successfully.")

        # Step 4: Convert trait_dict into a DataFrame for merging
        trait_list = []
        for key, values in trait_dict.items():
            trait_list.append({
                "propertyName": key,
                "propertyLabel": values[0]["label"],
                "propertyDefinition": values[0]["definition"],
                "measurementUnit": values[0]["unit"],
                "propertyUrl": values[0]["type"]
            })

        trait_df = pd.DataFrame(trait_list)

        merged_df = melted_df.merge(trait_df, on="propertyName", how="left")
        logger.info("Merged trait data with melted DataFrame.")

        # Step 5: Clean the merged DataFrame and generate observation ids
        clean_df = clean_and_infer_measurement_values(merged_df)
        df = observation_id_generator(clean_df)

        logger.info("Measurement attribute preparation completed successfully.")
        return df

    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        return df  # Return the original DataFrame in case of an error
    except json.JSONDecodeError as e:
        logger.error(f"Error loading the JSON file: {e}")
        return df  # Return the original DataFrame in case of an error
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return df  # Return the original DataFrame in case of an error


def observation_id_generator(df, column_name="observationId"):
    try:
        # Validate input
        if not isinstance(df, pd.DataFrame):
            raise TypeError("The input must be a pandas DataFrame.")
        if column_name in df.columns:
            raise ValueError(f"The column '{column_name}' already exists in the DataFrame.")

        df = df.copy()
        # Generate unique IDs
        df.loc[:, column_name] = [str(uuid.uuid4())[:18] for _ in range(len(df))]

        # Logging for debugging
        logger.info(f"Observation IDs generated and stored in column '{column_name}'.")

        return df

    except TypeError as e:
        logger.error(f"TypeError: {e}")
        return df  # Return the original DataFrame unmodified
    except ValueError as e:
        logger.error(f"ValueError: {e}")
        return df  # Return the original DataFrame unmodified
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return df  # Return the original DataFrame unmodified


def taxonomy_attribution_preparation(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Step 1: Load the taxonomy list from the JSON file
        taxonomy_list_path = "./taxonomy_list.json"
        if not os.path.exists(taxonomy_list_path):
            raise FileNotFoundError(f"Taxonomy list file not found at: {taxonomy_list_path}")

        with open(taxonomy_list_path, 'r') as f:
            taxonomy_list = json.load(f)

        logger.info("Taxonomy list loaded successfully.")

        # Step 2: Check if the taxonomic classification reported in this fields structure: scientificName - taxonRank
        if 'taxonRank' in df.columns:
            df["taxonRank"] = df["taxonRank"].str.lower()

            if 'acceptedNameUsage' in df.columns:
                df['taxon'] = df['acceptedNameUsage']
                logger.info(
                    "Duplicated 'acceptedNameUsage' into 'taxon' to report the taxonomic classification in this "
                    "structure: acceptedNameUsage - taxonRank.")

                taxonomy_columns = []
                non_taxonomy_columns = []

                for value in df.columns:
                    if value in taxonomy_list:
                        taxonomy_columns.append(value)
                    else:
                        non_taxonomy_columns.append(value)

                logger.info(f"Taxonomy Columns: {taxonomy_columns}")
                logger.info(f"Non-Taxonomy Columns: {non_taxonomy_columns}")

                if not taxonomy_columns:
                    return df

                melted_df = pd.melt(
                    df,
                    id_vars=non_taxonomy_columns,
                    value_vars=taxonomy_columns,
                    ignore_index=False,
                    var_name='temp_taxonRank',
                    value_name='temp_taxon'
                )
                melted_df = melted_df.drop(columns=['taxon', 'taxonRank'])
                # Rename melted columns to standard ones
                melted_df = melted_df.rename(columns={
                    'temp_taxonRank': 'taxonRank',
                    'temp_taxon': 'taxon'
                })

                df = df.drop(columns=taxonomy_columns)
                # Concatenate without conflicting columns
                df = pd.concat([df, melted_df], ignore_index=True)

                logger.info("Handled taxonomic classification by appending melted results to the original DataFrame.")

            else:
                raise ValueError("The DataFrame does not contain a column named 'acceptedNameUsage'.")
        else:

            taxonomy_columns = []
            non_taxonomy_columns = []

            for value in df.columns:
                if value in taxonomy_list:
                    taxonomy_columns.append(value)
                else:
                    non_taxonomy_columns.append(value)

            logger.info(f"Taxonomy Columns: {taxonomy_columns}")
            logger.info(f"Non-Taxonomy Columns: {non_taxonomy_columns}")

            if not taxonomy_columns:
                df["taxonRank"] = pd.NA
                df["taxon"] = pd.NA
                logger.warning("No taxonomic classification reported . Returning the original DataFrame with added "
                               "<Na> for taxon and taxonRank columns.")
                return df

            df = pd.melt(
                df,
                id_vars=non_taxonomy_columns,
                value_vars=taxonomy_columns,
                ignore_index=False,
                var_name='taxonRank',
                value_name='taxon'
            )

            logger.info("handled taxonomic classification.")

        return df

    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Error loading the JSON file: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


def normalize_empty_values(df):
    try:
        return df.map(lambda x: pd.NA if pd.isna(x) or x == "" else x)
    except Exception as e:
        logger.error(f"Error normalizing DataFrame: {e}")
        return df  # Return the original DataFrame in case of an error


def save_dataframe_to_csv(df):
    try:
        file_path = "./Stage_dir/transformed_dataset_prepared_for_mapping.csv"
        df.to_csv(file_path, index=False)
        logger.info(f"DataFrame successfully saved to {file_path}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return str(e)
