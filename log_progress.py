# This file contains a function that logs each step of the ETL process by creating a timestamp.

from datetime import datetime

def log_progress(file_path, message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() #get current timestamp
    timestamp = now.strftime(timestamp_format) #convert the specified date-time format into a string
    
    # Input file path to store log messages. Every time the function is called, a new log entry is appended to the file.
    with open(file_path,'a') as f:
        f.write(timestamp + ',' + message + '\n')