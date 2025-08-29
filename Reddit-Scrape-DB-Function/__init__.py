import logging
import azure.functions as func
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # add project root to path
from AzureDBPopulate import main as populator  # We call the main function from AzureDBPopulate

def main(mytimer: func.TimerRequest) -> None:
    logging.info("Python function started.")

    populator()

    if mytimer.past_due:
        logging.warning("Lookd like the function timed out :(")

    logging.info("Python script finished!")