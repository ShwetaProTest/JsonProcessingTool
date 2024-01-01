##############################################################################################################################################################################
#Python
#This script is a command-line tool that processes JSON files in a given directory. The script first creates two dictionaries to store timestamps for each state and runtimes for each
# pipeline step.It calculates statistics such as the minimum, maximum, median, and mean runtimes for each pipeline step and prints them.

#The script also has a Task 2 which sorts the processing times by value in descending order and show the top 5 slowest IDs or sorts the processing times by value in ascending order
# and show the top 5 fastest IDs based on the option passed in the command line.

#The script takes two command-line arguments:
#-d: The directory to process.
#-o : sort by slowest or fastest.
# eg python <script.py> -d /path/to/directory -o <slowest/fastest>
##################################################################################################################################################################################
import argparse
import json
import os
from datetime import datetime
import statistics
import numpy as np
import logging
import time

start_time = time.time()

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
                    data = json.load(json_file) # Takes string and return json object
                    id = data["id"]

                    # Initialize the processing time for this id to 0
                    processing_times[id] = 0

                # Extract the timestamp and state
                for stat in data['stats']:
                    timestamp = stat['utcTimeStamp']
                    state = stat['state']
                    # Add the timestamp to the list for the corresponding state
                    if state not in state_timestamps:
                        state_timestamps[state] = []
                    state_timestamps[state].append(timestamp)

                    if state == "PRE_PROCESSING":
                        start_time = timestamp
                    elif state == "PIPELINE_FINISHED":
                        end_time = timestamp
                        processing_times[id] += end_time - start_time

            except Exception as e:
                logging.error("Error processing file: %s. Error: %s", filename, str(e))

    # Calculate the runtimes for each pipeline step
    for i in range(1, len(state_timestamps)):
        current_state = list(state_timestamps.keys())[i]
        previous_state = list(state_timestamps.keys())[i-1]
        runtimes = [state_timestamps[current_state][j] - state_timestamps[previous_state][j] for j in range(len(state_timestamps[current_state]))]
        pipeline_runtimes[previous_state + ' -> ' + current_state] = runtimes

    # Calculate the statistics for each pipeline step
    print(
        "Task 1:\nProcess all files and present a statistic about the runtimes per pipeline step (min, max, 10%/50%/90% percentile and mean).\n")
    for pipeline_step, runtimes in pipeline_runtimes.items():
        min_runtime = min(runtimes)
        max_runtime = max(runtimes)
        mean_runtime = statistics.mean(runtimes)
        percentile10 = np.percentile(runtimes, 10)
        percentile50 = np.percentile(runtimes, 50)
        percentile90 = np.percentile(runtimes, 90)
        print(f'{pipeline_step} : {min_runtime}s {max_runtime}s {mean_runtime}s {percentile10}s {percentile50}s {percentile90}s')

    #Task2
    sorted_times = sorted(processing_times.items(), key=lambda x: x[1], reverse=(option == "slowest"))
    print("\nTask 2:\nShow the top 5 {} IDs & total processing time between the states:".format(option))
    for i in range(5):
        if i >= len(sorted_times):
            print("IndexError: list index out of range")
            break
        id, time = sorted_times[i]
        print(f'{"ID"}:{id}, {"PROCESSING_TIME"}:{time}')
        # print("ID:", id)
        # print("Total processing time :", time)
        # print("UTC timestamp: ", datetime.utcfromtimestamp(time / 1000).strftime('%Y-%m-%d %H:%M:%S'))
        # print("\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process json files in a directory and present statistics about the runtimes per pipeline step.')
    parser.add_argument("-d", "--directory", help='The directory to process.', required=True)
    parser.add_argument("-o", "--option", help="sort by slowest or fastest", choices=["slowest", "fastest"], required=True)
    args = parser.parse_args()
    process_directory(args.directory, args.option)
    end_time = time.time()
    time_taken = end_time - start_time
    print("Total Time Taken To Execute this Code: {:.2f} seconds".format(time_taken))