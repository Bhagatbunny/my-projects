import os
from pathlib import Path
import sys
import logging
from datetime import datetime



class FileUtils:
    @staticmethod
    def check_and_create(directory, logger):
        # this_method_name = sys._getframe().f_code.co_name
        try:
            if not os.path.exists(directory):
                Path(directory).mkdir(parents=True, exist_ok=True)
        except Exception as ex:
            logger.error(str(sys.exc_info()[0]) + f" Unable to read file with error: " + str(ex))
            sys.exit(1)

    @staticmethod
    def set_logger(log_name):
        global logger
        # this_method_name = sys._getframe().f_code.co_name
        try:
            # Fetching environment variables
            LOCAL_PATH = os.getenv("LOCAL_PATH")
            
            # Initializing logger
            logger = logging.getLogger(log_name)
            formatter = logging.Formatter("%(asctime)s - %(levelname)-5s - %(name)-5s - %(message)s")
            # formatter = logging.Formatter("%(asctime)s - %(levelname)-5s - %(name)-5s - %(methodName)-5s - %(message)s")
            today = datetime.now().strftime("%Y-%m-%d")
            
            # Fetching/Constructing log file path
            log_path = f'{LOCAL_PATH}/logs/'
            
            FileUtils.check_and_create(log_path, logger)
            log_file = os.path.join(log_path + log_name + '_' + today + '.log')
            
            # Setting up file handler
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.setLevel(logging.INFO)
        
        except Exception as ex:
            logger.error("Unable to set the log: " + str(ex))
            sys.exit(1)        
        return logger
       
