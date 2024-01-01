import os
import pandas as pd
import argparse
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", help="Directory containing pipeline stats")
parser.add_argument("-o", "--option", choices=["slowest", "fastest"], help="Select slowest or fastest pipeline runs")
args = parser.parse_args()

folder = args.directory
if os.path.exists(folder):
    inputDF = pd.DataFrame()
    for filename in os.listdir(folder):
        if filename.endswith(".json"):
            file_path = os.path.join(folder, filename)
            fileDF = pd.read_json(file_path)
            inputDF = pd.concat([inputDF, fileDF])
    statsDF = pd.json_normalize(inputDF["stats"].tolist())
    metricDF = statsDF.groupby(["id", "state"]).first().reset_index()
    metricDF = metricDF[["id", "utcTimeStamp", "state"]].pivot(index="id", columns="state",
                                                               values="utcTimeStamp").reset_index()
else:
    print("Folder not found at the given path: ", folder)

pipelinesMap = [("NEW", "QUEUED_FOR_PROCESSING"),
                ("QUEUED_FOR_PROCESSING", "PRE_PROCESSING"),
                ("PRE_PROCESSING", "FILE_TO_TIFF_CONVERSION_PROCESSING"),
                ("FILE_TO_TIFF_CONVERSION_PROCESSING", "FILE_TO_TIFF_CONVERSION_FINISHED"),
                ("FILE_TO_TIFF_CONVERSION_FINISHED", "OCR_PROCESSING"),
                ("OCR_PROCESSING", "OCR_FINISHED"),
                ("OCR_FINISHED", "PRE_PROCESSING_FINISHED"),
                ("PRE_PROCESSING_FINISHED", "PIPELINE_PROCESSING"),
                ("PIPELINE_PROCESSING", "PIPELINE_FINISHED"),
                ("PIPELINE_FINISHED", "POST_PROCESSING"),
                ("POST_PROCESSING", "DOCUMENT_PROCESSED")]

pipelineDF = metricDF
for (v1, v2) in pipelinesMap:
    try:
        pipelineDF[f"{v1}____{v2}"] = pipelineDF[v2] - pipelineDF[v1]
    except KeyError:
        print(f"Columns {v1} and/or {v2} do not exist in the dataframe. Skipping calculation.")

pipelineDF = pipelineDF[["id"] + [f"{v1}____{v2}" for (v1, v2) in pipelinesMap]]

pipelineStatsDF = pipelineDF.melt(id_vars=["id"], var_name="pipeline", value_name="time")
pipelineStatsDF["pipeline"] = pipelineStatsDF["pipeline"].apply(lambda x: x.replace("____", " -> "))
print(pipelineStatsDF["pipeline"])

aggColumn = "Time"

print("Task 1:\nProcess all files and present a statistic about the runtimes per pipeline step (min, max, 10%/50%/90% percentile and mean).")
pipeline_stats = pipelineStatsDF.groupby("PIPELINE").agg({
    "Time": [np.min, np.max, np.mean, lambda x: np.percentile(x, q=0.1),
             lambda x: np.percentile(x, q=0.5), lambda x: np.percentile(x, q=0.9)]
})
print(pipeline_stats)

pipeline_stats = pipeline_stats.reset_index()
pipeline_stats.columns = ['PIPELINE', 'min', 'max', 'mean', 'percentile10', 'percentile50', 'percentile90']
pipeline_stats.sort_values(by=['PIPELINE'], inplace=True)
print(pipeline_stats)

print("Task 2a:\n Show the top 5 slowest/fastest IDs per processing step. (Implement as switch via commandline option)")

if args.option == "fastest":
    pipeline_stats.sort_values(by=['Time'], inplace=True)
    pipeline_stats = pipeline_stats.head(5)
    pipeline_stats.sort_values(by=['PIPELINE'], inplace=True)
elif args.option == "slowest":
    pipeline_stats.sort_values(by=['Time'], inplace=True, ascending=False)
    pipeline_stats = pipeline_stats.head(5)
    pipeline_stats.sort_values(by=['PIPELINE'], inplace=True)
else:
    raise ValueError(f"Invalid option: {option}")

print(pipeline_stats)

print("Task 2b:\n Show the total processing time(ms) between the states: PRE_PROCESSING -> PIPELINE_FINISHED")
metric_df = metricDF[["ID", "PRE_PROCESSING", "PIPELINE_FINISHED"]]
metric_df['PROCESSING_TIME'] = metric_df["PIPELINE_FINISHED"] - metric_df["PRE_PROCESSING"]
print(metric_df)
