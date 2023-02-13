from os import listdir, remove
from os.path import isfile, join
from json import load, loads
from tifffile import imread,imwrite
from re import search, findall

import numpy as np
import pandas as pd

def find_columns(csv_file):
    """
    Helper method to find all the columns
    
    :param csv_file: Takes in the csv file string, e.g:
        'WBG Repeat, Batch 4 (Experiment 1)_sample12_action.csv'
        
    :return: Returns a list all the columns
    """
    df = pd.read_csv(csv_file)
    return df.columns.to_list()

def find_local_csv_files():
    """Find all the csv files ending with the name sample_chem.csv, sample_link.csv, 
    sample_action.csv within the current directory and group by sample"""
    csv_files = [x for x in listdir() if x.endswith('.csv')]
    unique_samples = list(set(findall('_(sample\d+)_', ' '.join(csv_files))))

    file_dict = {}
    file_list = []

    for file in csv_files:   
        for sample in unique_samples:
            if sample not in file_dict:
                file_dict[sample] = {}

            if '_{}_chem.csv'.format(sample) in file:
                file_dict[sample]['chem'] = file
                file_list.append(file)
            elif '_{}_action.csv'.format(sample) in file:
                file_dict[sample]['action'] = file
                file_list.append(file)
            elif '_{}_link.csv'.format(sample) in file:
                file_dict[sample]['link'] = file
                file_list.append(file)
        
    return file_dict, file_list

def create_nodes(filepath, node_type, cols, stored_folder = ''):
    query = "LOAD CSV WITH HEADERS FROM \"file:///"
    query += stored_folder
    
    if stored_folder != '':
        query += '/'
        
    query += "{}\" ".format(filepath)
    if node_type == 'chem':
        query += "AS row CREATE (c:Chemical {"
    elif node_type == 'action':
        query += "AS row CREATE (a:Action {"
    
    # add the columns into the query
    query_columns = (["{}: row['{}'], ".format(cols[i], cols[i]) if i < len(cols) - 1 
           else "{}: row['{}']".format(cols[i], cols[i]) for i in range(len(cols))])
    query += ''.join(query_columns)
    
    # close the brackets and return
    return query + "});"

def create_links(filepath, stored_folder = ''):
    query_start = "LOAD CSV WITH HEADERS FROM \"file:///"
    query_start += stored_folder
    
    if stored_folder != '':
        query_start += '/'
    
    query_start += "{}\" ".format(filepath)
    
    query_1 = "AS row MATCH (c:Chemical {chemical_id: row['chemical_from'], sample_id: row['sample_id'], \
batch_id: row['batch_id']}), (a:Action {step_id:row['step_to'], sample_id: row['sample_id'], \
batch_id: row['batch_id']}) CREATE (c)-[:GOES_INTO]->(a);"
    
    query_2 = "AS row MATCH (a1:Action {action: 'dissolve', step_id: row['step_from'], sample_id: row['sample_id'], \
batch_id: row['batch_id']}),(c1:Chemical {chemical_id:row['chemical_to'], sample_id: row['sample_id'], \
batch_id: row['batch_id']}) CREATE (a1)-[:OUTPUTS]->(c1);"
    
    query_3 = "AS row MATCH (a3:Action {action:'drop',step_id: row['step_from'], sample_id: row['sample_id'], \
batch_id: row['batch_id']}),(c4:Chemical {chemical_id:row['chemical_to'], sample_id: row['sample_id'], \
batch_id: row['batch_id']}) CREATE (a3)-[:NEXT]->(c4);"
    
    query_4 = "AS row MATCH (a3:Action {step_id: row['step_from'], sample_id: row['sample_id'], \
batch_id: row['batch_id']}),(a4:Action {step_id:row['step_to'], sample_id: row['sample_id'], \
batch_id: row['batch_id']}) CREATE (a3)-[:NEXT]->(a4);"
    

    queries = []
    for i in [query_1, query_2, query_3, query_4]:
        query_str = query_start + i
        queries.append(query_str)
    
    return queries

def query_maker(chem_filepath, action_filepath, link_fileid, stored_folder = ''):
    """
    Calls the other query functions in this one function, given the necessary file ids"""
    queries = []
    
    chem_cols = find_columns(chem_filepath)
    queries.append(create_nodes(chem_filepath, 'chem', chem_cols, stored_folder))
    
    action_cols = find_columns(action_filepath)
    queries.append(create_nodes(action_filepath, 'action', action_cols, stored_folder))
    
    queries = queries + create_links(link_fileid, stored_folder)
    return queries

def save_queries():
    file_dict, file_list = find_local_csv_files()
    queries = []
    neo4j_stored_folder = ''

    for sample in file_dict:
        # the input file_id is in the order of chem.csv, action.csv, link.csv
        queries.append(query_maker(file_dict[sample]['chem'], 
                                    file_dict[sample]['action'], 
                                    file_dict[sample]['link'], 
                                    stored_folder = neo4j_stored_folder))
    
    # saving the file as .cypher file
    output = open('output.cypher', 'w')
    for q in queries:
        for query in q:
            output.write(query)
    output.close()
    return