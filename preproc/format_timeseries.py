import pandas as pd


def format_timeseries(instance_events_fname, durations_fname):
    instance_events_df = pd.read_json(instance_events_fname, lines=True)
    durations_df = pd.read_csv(durations_fname)
    
    timeseries = dict()  # timestep -> {task_id -> duration, priority, resource_request}

    for _, instance_event in instance_events_df.iterrows():
        



if __name__ == "__main__":
    format_timeseries("data/instance_events_naive_collection_ids_less_12335081865_w_task_id.json",
                      "data/task_durations.csv")