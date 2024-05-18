import pandas as pd
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
import os
import time
import json

def data_loader():
    month_data = {'January': [], 'February': [], 'March': []}
    for part_num in range(4):
        partition_file = f'/files/partition-{part_num}.json'
        if os.path.exists(partition_file):
            with open(partition_file, 'r') as file:
                partition_info = json.load(file)
                for month, month_stats in partition_info.items():
                    if month in month_data:
                        for year, stats in month_stats.items():
                            month_data[month].append({
                                'year': int(year),
                                'temperature': stats['avg']
                            })
    return month_data

def plot_and_save(month_data):
    month_series_data = {}
    for month, year_data in month_data.items():
        recent_year = None
        recent_temperature = None
        for entry in year_data:
            if recent_year is None or entry['year'] > recent_year:
                recent_year = entry['year']
                recent_temperature = entry['temperature']
        if recent_year is not None:
            month_series_data[f"{month}-{recent_year}"] = recent_temperature

    month_series = pd.Series(month_series_data)
    print(month_series)
    fig, ax = plt.subplots()
    month_series.plot.bar(ax=ax)
    ax.set_ylabel('Avg. Max Temperature')
    ax.set_title('Month Averages')
    plt.tight_layout()
    if os.path.exists("/files/month.svg"):
        os.remove("/files/month.svg")
   
    plt.savefig("/files/month.svg")
   
    print("Updated month.svg with new plot data")

if __name__ == "__main__":
    month_data = data_loader()
    plot_and_save(month_data)