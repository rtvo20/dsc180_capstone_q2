from os import listdir, remove
from os.path import isfile, join
from json import load, loads
from tifffile import imread,imwrite
from re import search, findall

import numpy as np
import pandas as pd

def step_helper(worklists):
    """
    Helper function to get the step names for each step in worklist(s).
    Takes either a single worklist or a list of worklists in as input and returns
    a list of lists, with each list corresponding to an input worklist.
    """
    steps = []
        
    # iterate through worklists and get steps e.g. destination, drops, etc
    for w in worklists:
        w_steps = []
        for step in w:
            w_steps.append(list(step['details'].keys())[0])
        steps.append(w_steps)
    
    return steps

def dissolve_helper(drop_list, step_num = None):
    drop_steps = drop_list['details']['drops']
    
    num_chems = len(drop_steps[0]['solution']['solutes'].split('_')) + \
    len(drop_steps[0]['solution']['solvent'].split('_'))
    
    actions = []
    for i in range(1, num_chems+1):
        row = {}
        row['step_id'] = i
        row['action'] = 'dissolve'
        row['chemical_from'] = i
        actions.append(row)
    return actions    

def drop_helper(drop_list, step_num):
    drop_list = drop_list['details']['drops']
    
    # exclude volume and solution information, as this is handled when 
    # creating the chem csvs/nodes
    exclude_attributes = ['volume', 'solution']
    row_attributes = [i for i in drop_list[0].keys() if i not in exclude_attributes]
    drop_rows = []
    i = 1
    for drop in drop_list:
        row = {}
        for j in row_attributes:
            row['step_id'] = step_num * 2 + i
            row['action'] = 'drop'
            row['chemical_from'] = step_num + i
            row["drop_"+j] = drop.get(j)
        i += 1
        drop_rows.append(row)
    return drop_rows

def spin_helper(spin_list, step_num):
    spin_rows = []
    spin_details = spin_list['details']['steps']
    for spin in spin_details:
        row = {}
        row['step_id'] = step_num
        row['action'] = 'spin'
        for i in spin:
            row["spin_"+i] = spin.get(i)
        step_num += 2
        
        attributes = ['start', 'start_actual', 'finish_actual', 'liquidhandler_timings', 'spincoater_log']
        for i in attributes:
            if i in spin_list.keys():
                if (i == 'spincoater_log') and ('rpm' in spin_list[i]):
                    log_attr = spin_list[i].keys()
                    for j in log_attr:
                        row['spin_log_'+j] = spin_list[i][j]
                else:
                    row[i] = spin_list[i]
        spin_rows.append(row)
    return spin_rows

def anneal_helper(anneal_list, step_num):
    row = []
    anneal_info = anneal_list['details']
    anneal_row = {'step_id':step_num, 'action': 'anneal'}
    for i in anneal_info:
        anneal_row['anneal_'+i] = anneal_info.get(i)
        
    attributes = [i for i in anneal_list.keys() if i not in ['precedent', 'id', 'details']]
    for i in attributes:
        anneal_row[i] = anneal_list[i]
    row.append(anneal_row)
    return row

def rest_helper(rest_step, step_num):
    rest_row = {'step_id': step_num, 'action':'rest'}
    rest_row['rest_duration'] = rest_step['details']['duration']
    
    attributes = [i for i in rest_step.keys() if i not in ['precedent', 'id', 'details']]
    for i in attributes:
        rest_row[i] = rest_step[i]
    return [rest_row]

def char_helper(char_list, step_num):
    char_rows = []
    char_details = char_list['details']['characterization_tasks']
    for char in char_details:
        char_params = [i for i in list(char['details'].keys())]
        char_info = [i for i in char if i != 'details']
        row = {
            'step_id':step_num, 
            'action':'char'
        }
        for i in char_params:
            row['char_'+i] = char['details'][i]
        for i in char_info:
            if type(char[i]) == str:
                if i == 'name':
                    row['char_'+i] = char[i].lower().split("_")[0]
                else:
                    row['char_'+i] = char[i].lower()
            else:
                row['char_'+i] = char[i]
                
        attributes = [i for i in char_list.keys() if i not in ['precedent', 'id', 'details', 'name', 'sample', ]]
        for i in attributes:
            row[i] = char_list[i]
        char_rows.append(row)
        step_num += 2
        
    return char_rows

# PIPELINE: function map used in action_table to call helper functions
func_map = {
    "dissolve": dissolve_helper,
    "drops": drop_helper,
    "spin": spin_helper,
    "anneal": anneal_helper,
    "duration": rest_helper,
    "characterization_tasks": char_helper
}

# PIPELINE
def action_table(worklists, sample_id=np.nan, batch_id=np.nan):
    """
    The action_table function takes in one or more worklists. 
    If more than one worklist, include it in a list.
    """
    if type(worklists[0]) != list:
        worklists = [worklists]
        
    # obtain steps from worklists
    steps = step_helper(worklists)
    
    rows = []
    step_num = 1
    for i in range(len(worklists)):
        curr_worklist = worklists[i]
        curr_steplist = steps[i]
        for j in range(len(curr_steplist)):
            if curr_steplist[j] == 'destination':
                continue
            else:
                if curr_steplist[j] == 'drops':
                    rows = rows + func_map['dissolve'](curr_worklist[j], step_num)
                    step_num = rows[-1]['step_id']
                    rows = rows + func_map['drops'](curr_worklist[j], step_num)
                    step_num = rows[-1]['step_id']+3
                    rows = rows + func_map['spin'](curr_worklist[j], step_num)
                    step_num = rows[-1]['step_id']+2
                else:
                    if curr_steplist[j] == 'duration' and curr_worklist[j]['name'] == 'anneal':
                        rows = rows + func_map['anneal'](curr_worklist[j], step_num)
                    else:
                        rows = rows + func_map[curr_steplist[j]](curr_worklist[j], step_num)
                    step_num = rows[-1]['step_id']+2
    
    res = pd.DataFrame(rows)
    res['sample_id'] = [sample_id] * res.shape[0]
    res['batch_id'] = [batch_id] * res.shape[0]
    
    return res

# PIPELINE
# helper function used in char_outputs.
def load_image(fid):
    img = imread(fid) * 64 * 255
    img = img.astype(np.float32)
    img = np.dot(img[...,:3], [0.2989, 0.5870, 0.1140]) #convert to single channel/greyscale??
    return img.astype(int) #truncate

# PIPELINE
def char_outputs(folder, sample):
    path = folder + '/' + sample + "/characterization0"
    fids = [f for f in listdir(path)]
    
    data = []
    ids = []
    for fid in fids:
        if '.tif' in fid:
            data.append(load_image(path+"/"+fid))
        elif '.csv' in fid:
            data.append(pd.read_csv(path+"/"+fid).drop(0,axis=0).to_dict())
        else:
            print('haven\'t had to deal w this filetype yet')

        ids.append(fid.split('_', 1)[1].split('.')[0])    
    df = pd.DataFrame({'join_on': ids, 'fid':fids, 'output':data})

    return(df)

# PIPELINE
def append_outputs(output_df, action_df):
    step_id = action_df.iloc[-1]['step_id']+2
    
    row_template = action_df.iloc[-1].copy()
    row_template.loc[:]=np.nan
    row_template['action'] = 'char'
    row_template['sample_id'] = action_df['sample_id'].iloc[0]
    row_template['batch_id'] = action_df['batch_id'].iloc[0]
    
    output_rows = []
    for r in range(output_df.shape[0]):
        output_row = output_df.iloc[r]
        row = row_template.copy()
        row['step_id'] = step_id
        row['char_name'] = output_row['join_on']
        row['fid'] = output_row['fid']
        row['output'] = output_row['output']
        output_rows.append(row)
        step_id += 1
        
    action_df = action_df.append(output_rows)
    
    return action_df

def save_action_csvs(process_data, char_data):
    # PIPELINE
    # run to save all as csvs
    action_dfs = []
    batch_id = 'b19'
    folder = 'test/testdata/Characterization_B19'

    for s in process_data:
        a_df = pd.DataFrame(action_table([process_data[s]['worklist'], char_data[s]['worklist']], s, batch_id))
        output_df = char_outputs(folder, s)
        a_df = append_outputs(output_df, a_df)
        a_df = a_df.astype({'chemical_from':'Int64'})
        action_dfs.append(a_df)
        fname = batch_id + '_' + s + '_action.csv'
        fname = fname.replace(' ', '_')
        a_df.to_csv(fname,index=False)

    return action_dfs