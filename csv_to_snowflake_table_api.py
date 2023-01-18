import snowflake.connector
from snowflake_data_load import *

### the API to be provided to the client
def load_csvs_to_table(
    fully_qualified_snowflake_table_name: str,
    csv_files_directory_path: str
): 
    # reading the credentials and connection information provided by the end user
    conf = read_config("snowflake_config.json")
    
    # create the snowflake connection object
    conn = snowflake.connector.connect(
                    user = conf["snowflake_username"],
                    password = conf["snowflake_password"],
                    account = conf["snowflake_account"],
                    warehouse = conf["snowflake_warehouse"],
                    database = conf["snowflake_database"],
                    schema = conf["snowflake_schema"]
                )

    # listing the file names under the directory path provided by the end user
    # TODO: file type validation is required
    file_paths_list = [file for file in os.listdir(csv_files_directory_path) \
                       if os.path.isfile(os.path.join(csv_files_directory_path, file))]
    
    # generating the relative file path for the csv files
    absolute_file_paths_list = map( \
                                   lambda path: os.path.join(csv_files_directory_path, path), \
                                   file_paths_list)
    
    # invoking the function to load the csv files data into Snowflake table
    load_csvs_to_snowflake_table(conn, fully_qualified_snowflake_table_name, absolute_file_paths_list)