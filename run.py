#!/usr/bin/env python

import sys
import os
import json

sys.path.insert(0, 'src')

from etl import get_data
from chem_feature import *
from action_feature import *
from link_feature import *
from query_feature import *

# data = get_data()
# act = save_action_csvs(data[0], data[1])
# save_link_csvs(act)
# save_queries()

def main(targets):
    '''
    Runs the main project pipeline on the given targets.
    Targets are "data", "features", "graph"
    
    'main' should run the targets in order:
    'data' -> 'features' -> 'graph'
    '''
    if 'test' in targets:
        targets = ['data', 'features', 'graph']
    
    if 'data' in targets:
        data = get_data()
    
    if 'features' in targets:
        save_chem_csvs(data[0], 'b19')
        
        action_dfs = save_action_csvs(data[0], data[1])
        
        save_link_csvs(action_dfs)
        
    if 'graph' in targets:
        save_queries()
        
    return
        
if __name__ == '__main__':
    targets = sys.argv[1:]
    main(targets)