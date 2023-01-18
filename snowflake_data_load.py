import snowflake.connector
import logging
import os
import sys

### LOGGER ###
###
### taken from www.codegrepper.com
### logger library
###
rotating_file_handler = logging.handlers.RotatingFileHandler(
    # the filename can be parameterized
    filename = 'snowflake_data_load.log', 
    mode = 'a',
    maxBytes = 20*1024*1024,
    backupCount = 2,
    encoding = None,
    delay = 0
)

logging.basicConfig(
    # setting the logging level to DEBUG, it must be changed to ERROR for the production
    level = logging.DEBUG,
    format = "%(asctime)s %(name)-25s %(levelname)-8s %(message)s",
    datefmt = "%y-%m-%d %H:%M:%S",
    handlers = [
        rotating_file_handler
    ]
)

### END of LOGGER ###

### UTILS ###
# helper function to read the configuration files
def read_config(config_file_name: str):
    logger = logging.getLogger('read_config')
    
    try:
        contents = open(config_file_name).read()
        config = eval(contents)
        logger.debug(config)
        return config
    except OSError as err:
        logger.error("Error({0}): {1}".format(err.errno, err.strerror))
        sys.exit()

    
### END of UTILS ###

# This function loads csv data from a list of csv files into one Snowflake table.
# arguments:
#     - Snowflake connection object
#     - destination Snowflake table name in the form of <database_name>.<schema_name>.<table_name>
#     - absolute paths of the source csv files
# all the required commands are stored in the commands.config
# the purpose of the configuration file is to keep this function more readable and maintainable
def load_csvs_to_snowflake_table(
   conn: snowflake.connector.connection.SnowflakeConnection,
   fully_qualified_table_name: str,
   csv_file_paths: list[str]
):
    logger = logging.getLogger('load_csvs_to_snowflake_table')
    
    # reading the commands config file
    commands = read_config("commands.config")
    
    logger.debug(commands)
    
    # create the cursor
    cur = conn.cursor()
    
    # preparing steps before loading the data into Snowflake
    
    # creating the input csv file format object
    cur.execute(commands["csv_file_format"])
    # creating the stage object
    cur.execute(commands["csv_stage"])
    
    # preparing the staging commands for the input csv files
    # a command line template is used 
    stage_files_command = commands["stage_files"]
    updated_stage_files_commands = map(lambda x: stage_files_command.format(csv_file_path = x), csv_file_paths)
    
    
    # staging the csv files
    for cmd in updated_stage_files_commands:
        logger.debug(cmd)
        cur.execute(cmd)
    
    # for debugging
    # this is just to check the staged files
    cur.execute(commands["list_staged_files"])
    for col in cur:
        logger.debug(col)
    
    # cur.execute(commands["create_error_table"])
    
    # using a command line template to generate the copy command
    load_csv_command = commands["copy_csv_data_into_snowflake"].format(table_name = fully_qualified_table_name)
    
    # for debugging
    logger.debug(load_csv_command)

    # copying the data from the staged csv files into the target Snowflake table
    cur.execute(load_csv_command)
    
    logger.info("data loading into snowflake table done")
    
    # removing the successfully loaded csv files from the internal stage
    cur.execute(commands["remove_staged_files"])
    logger.info("staged files removed")

    # closing the cursor
    cur.close()
