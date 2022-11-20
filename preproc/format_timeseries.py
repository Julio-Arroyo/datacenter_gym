import pandas as pd


def format_timeseries(instance_events_fname):
    df = pd.read_json(instance_events_fname, lines=True)
    
    timeseries = dict()  # timestep -> {task_id -> duration, priority, resource_request}
    



if __name__ == "__main__":
    format_timeseries("data/instance_events_naive_collection_ids_less_12335081865_w_task_id.json")