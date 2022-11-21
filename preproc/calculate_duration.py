"""Calculate duration of each task"""

import pandas as pd
from enum import Enum


class InstanceEvent(Enum):
    SUBMIT = 0
    QUEUE = 1
    ENABLE = 2
    SCHEDULE = 3
    EVICT = 4
    FAIL = 5
    FINISH = 6
    KILL = 7
    LOST = 8
    UPDATE_PENDING = 9
    UPDATE_RUNNING = 10


def is_start_event(event_type):
    return event_type == InstanceEvent.SCHEDULE


def is_pause_event(event_type):
    return event_type == InstanceEvent.QUEUE or event_type == InstanceEvent.EVICT


def is_end_event(event_type):
    return (event_type == InstanceEvent.FINISH or
            event_type == InstanceEvent.KILL or
            event_type == InstanceEvent.FAIL or
            event_type == InstanceEvent.LOST)


def save_durations(task_id_to_duration):
    with open('data/task_durations.csv', 'w') as f:
        for task_id in task_id_to_duration:
            f.write("%s,%s"%(task_id, task_id_to_duration[task_id]))


def transform_features(df):
    """
    Transform column of dicts 'resource_request' into two separate columns 'cpus' and 'memory'.
    """
    print("Begin feature transformation...")
    cpus = []
    memory = []
    cpu_error_count = 0
    memory_error_count = 0
    for _, row in df.iterrows():
        try:
            cpus.append(row["resource_request"]["cpus"])
        except TypeError:  # float is not subscriptable, sometimes row["resource_request"] is nan
            cpus.append(None)
            cpu_error_count += 1
        try:
            memory.append(row["resource_request"]["memory"])
        except TypeError:
            memory.append(None)
            memory_error_count += 1
    print("ERROR REPORT: Transform features:")
    print(f"\t - CPU errors: {cpu_error_count}/{len(df)}={100*cpu_error_count/len(df)}%")
    print(f"\t - Memory errors: {memory_error_count}/{len(df)}={100*memory_error_count/len(df)}%")
    return cpus, memory


def calculate_task_duration(fname):
    df = pd.read_json(fname, lines=True)
    print("Loaded dataframe...")

    # add new columns
    cpus, memory = transform_features(df)
    df["cpus"] = cpus
    df["memory"] = memory
    # drop old columns no longer needed after creating new features
    df.drop(columns=["resource_request"])

    task_ids = df.task_id.unique()

    task_id_to_duration = {}

    ten_percent_milestone = len(task_ids) // 10
    count = 0

    for task_id in task_ids:
        print(f"Task id #{count}/{len(task_ids)}")
        if count % ten_percent_milestone == 0:
            print(f"Task id #{count}/{len(task_ids)}")
        count += 1

        task_events = df.loc[df["task_id"] == task_id]
        task_events.sort_values(by="time")

        is_running = False
        is_done = False
        start_time = None
        duration = 0
        for _, task_event in task_events.iterrows():
            assert not is_done
            if not is_running:
                if not is_start_event(task_event["type"]):
                    continue
                start_time = task_event["time"]  # continue what to do when time = 0
                is_running = True
            else:
                assert is_pause_event(task_event["type"]) or is_end_event(task_event["type"])  # might have to continue if schedule while running for some reason

                assert not start_time is None
                duration += task_event["time"] - start_time

                if is_end_event(task_event["type"]):
                    is_done = True
        
            task_id_to_duration[task_id] = duration
    
    save_durations(task_id_to_duration)



if __name__ == "__main__":
    calculate_task_duration("data/instance_events_naive_collection_ids_less_12335081865_w_task_id_only_relevant_events.json")
