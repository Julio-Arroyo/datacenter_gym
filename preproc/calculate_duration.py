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


class InstanceEventTypes(Enum):
    START = 0
    PAUSE = 1
    END = 2


def transform_features(df):
    """
    - Create task_id as a concatenation of collection_id and instance_index.
    - Transform column of dicts 'resource_request' into two separate columns 'cpus' and 'memory'.
    """
    task_ids = []
    cpus = []
    memory = []
    for _, row in df.iterrows():
        task_ids.append(row["collection_id"] + row["instance_index"])
        cpus.append(row["resource_request"]["cpus"])
        memory.append(row["resource_request"]["memory"])
    return task_ids, cpus, memory


def calculate_task_duration(fname):
    df = pd.read_json(fname, lines=True)

    # add new columns
    task_ids, cpus, memory = transform_features(df)
    df["task_id"] = task_ids
    df["cpus"] = cpus
    df["memory"] = memory
    # drop old columns no longer needed after creating new features
    df.drop(columns=["collection_id", "instance_index", "resource_request"])

    task_ids = df.task_ids.unique()
    # TODO: calculate duration for each task id


if __name__ == "__main__":
    calculate_task_duration("data/instance_events_naive_collection_ids_less_12335081865.json")
