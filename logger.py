import os
import logging
from datetime import datetime


class Logger:
    def __init__(self, project_name, version_number):
        self.project_name = project_name
        self.version_number = version_number
        self.setup_logging()

    def setup_logging(self):
        os.makedirs('logs', exist_ok=True)
        log_filename = f"logs/{self.project_name}_v{self.version_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger()

    def output(self, message, log_level="info"):
        log_method = getattr(self.logger, log_level.lower(), self.logger.info)
        log_method(message)
        print(message)