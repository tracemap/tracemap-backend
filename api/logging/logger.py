import json
import os

log_folder_path = './logs/'

def save_log(log_object, filename):
    """Takes the log params and appends them as a
    json object to the according jsonl file"""
    if os.path.exists(log_folder_path):
        with open("%s%s.jsonl" % (log_folder_path, filename), "a") as log_file:
            log_file.write(json.dumps(log_object) + "\n")
        return {"logging": "success"}
    else:
        return {"error": "%s folder path does not exist on the server" % log_folder_path}
    