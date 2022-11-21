import json
import pandas as pd
from calculate_duration import InstanceEvent, transform_features


def get_task_id_to_duration(durations_df):
    task_id_to_duration = {}

    for _, row in durations_df.iterrows():
        task_id_to_duration[row["task_id"]] = row["duration"]
    
    return task_id_to_duration


def format_timeseries(instance_events_fname, durations_fname):
    instance_events_df = pd.read_json(instance_events_fname, lines=True)
    # add new columns
    cpus, memory = transform_features(instance_events_df)
    instance_events_df["cpus"] = cpus
    instance_events_df["memory"] = memory
    # drop old columns no longer needed after creating new features
    instance_events_df.drop(columns=["resource_request"])
    instance_events_df.sort_values(by="Time", inplace=True)

    durations_df = pd.read_csv(durations_fname)
    task_id_to_duration = get_task_id_to_duration(durations_df)
    
    timeseries = dict()  # timestep -> {task_id -> duration, priority, resource_request}
    scheduled_tasks = set()  # record here every task_id that has been added to the timeseries

    for _, instance_event in instance_events_df.iterrows():
        if ((not instance_event["type"] == InstanceEvent.SCHEDULE) or  # the first SCHEDULE event for any given task ID is the only one we keep
            (instance_event["task_id"] in scheduled_tasks)):  # there may be more than one SCHEDULE event for a single task
            continue

        time = instance_event["time"]
        task_id = instance_event["task_id"]
        task_data = {
            "duration": task_id_to_duration[task_id],
            "priority": instance_event["priority"],
            "cpus": instance_event["cpus"]
        }
        if not time in timeseries:
            timeseries[time] = {}
        
        timeseries[time][task_id] = task_data
        scheduled_tasks.add(task_id)
    
    # save timeseries
    json_obj = json.dumps(timeseries, indent=4)
    with open("data/datacenter_gym_timeseries_data.json") as f:
        f.write(json_obj)


if __name__ == "__main__":
    format_timeseries("data/instance_events_naive_collection_ids_less_12335081865_w_task_id.json",
                      "data/task_durations.csv")