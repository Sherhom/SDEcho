# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import argparse
import logging
from XplainerDataModel import XplainerDataModel
from Search import *
from XLearner import *

def read_diff_query(json_path):
    """
    Example JSON file:
    {
        "fp": {
            "cols": [],
            "vals": []
        },
        "dp": {
            "col": "X",
            "vals": [1, 0]
        },
        "measure": {
            "col": "Z",
            "func": "sum"
        },
        "prior_cols": []
    }
    """
    with open(json_path, "r") as f:
        diff_query :dict= json.load(f)
    
    fp = FixedPredicate(diff_query["fp"]["cols"], diff_query["fp"]["vals"])
    dp = DiffPredicate(diff_query["dp"]["col"], diff_query["dp"]["vals"])
    measure = Measure(diff_query["measure"]["col"], diff_query["measure"]["func"])

    return fp, dp, measure, diff_query.get("prior_cols", None)


def run(scale, sql, noise):

    sql_path = f'SDEcho/benchmark/two_step/scale_{scale}/sql_{sql}'
    data_path = os.path.join(sql_path, f'noise_{noise}/data.csv')
    query_json_path = os.path.join(sql_path, 'query.json')
    save_path = os.path.join(sql_path, f'noise_{noise}','graph.json')
    use_xlearner = True

    logging.basicConfig(level=logging.INFO)

    logging.info(f"Reading data from {data_path}")
    dm = XplainerDataModel(data_path)

    logging.info(f"Reading diff query from {query_json_path}")
    fp, dp, measure, prior_col = read_diff_query(query_json_path)
    if measure.func == "avg":
        dq = AvgDiffQuery(fp, dp, measure, dm.col_names)
        dq.create_scope(dm.conn)
        raw_diff = dq.get_interventional_diff(FixedPredicate([],[]), dm.conn)
        search = AvgSearch(dq, dm.value_set, dm.conn)
    else:
        dq = DiffQuery(fp, dp, measure, dm.col_names)
        dq.create_scope(dm.conn)
        raw_diff = dq.get_interventional_diff(FixedPredicate([],[]), dm.conn)
        search = SumSearch(dq, dm.value_set, dm.conn)

    if use_xlearner:
        logging.info(f"Running XLearner with default SL algorithm")
        xl = XLearner(data_path, edges_json_file = save_path)
        exp_cols, sem = xl.get_explainable_cols(dq)
        logging.info(f"Explainable columns: {exp_cols}")
        logging.info(f"Semantics: {sem}")
    else:
        logging.info(f"Disable XLearner; using prior columns {prior_col} in query JSON")
        exp_cols = prior_col
    
    search.run(exp_cols)
    for exp, resp in search.explanation:
        print(exp, resp)


if __name__ == '__main__':
    run(0.001, 1, 2)