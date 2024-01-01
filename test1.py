import argparse
import json
import os
from datetime import datetime
import statistics
import numpy as np
import logging

logging.basicConfig(filename='script.log', level=logging.INFO)


def process_directory(directory, option):
    # Create a dictionary to store the timestamps for each state
    state_timestamps = {}

    # Create a dictionary to store the runtimes for each pipeline step
    pipeline_runtimes = {}
    processing_times = {}

    logging.info('Start processing directory: %s', directory)
    # Get a list of all the JSON files in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            logging.info('Processing file: %s', filename)
            try:
                # Open and parse the JSON file
                with open(os.path.join(directory, filename)) as json_file:
                    data = json.load(json_file)
                    id = data["id"]

                # Extract the timestamp and state
                for i in range(1, len(data['stats'])):
                    current_state = data['stats'][i]['state']
                    previous_state = data['stats'][i - 1]['state']
                    current_timestamp = data['stats'][i]['utcTimeStamp']
                    previous_timestamp = data['stats'][i - 1]['utcTimeStamp']

                    runtime = current_timestamp - previous_timestamp
                    processing_times[id] = processing_times.get(id, 0) + runtime

            except Exception as e:
                logging.error("Error processing file: %s. Error: %s", filename, str(e))

    # Task2
    sorted_times = sorted(processing_times.items(), key=lambda x: x[1], reverse=(option == "slowest"))
    print("\nTask 2\nShow the top 5 {} IDs & total processing time between the states:".format(option))
    for i in range(5):
        if i >= len(sorted_times):
            print("IndexError: list index out of range")
            break
        id, time = sorted_times[i]
        print("ID:", id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process json files in a directory and present statistics about the runtimes per pipeline step.')
    parser.add_argument('-i', help='The directory to process.', required=True)
    parser.add_argument("-s", help="sort by slowest or fastest", choices=["slowest", "fastest"], required=True)
    args = parser.parse_args()
    process_directory(args.i, args.s)

