from os import listdir, remove
from os.path import isfile, join
from json import load, loads
from tifffile import imread,imwrite
from re import search, findall

import numpy as np
import pandas as pd

def split_chemicals(input_string):
    result = []
    for i in input_string.split('_'):
        first_digit = search(r'\d+.\d+', i).start()
        result.append((i[:first_digit], float(i[first_digit:])))
    return result

def create_new_row(**kwargs):
    return dict(zip(kwargs, kwargs.values()))

def check_name_format(chemicals_string_list):
    """Function to check if the chemicals from drop step follows 
    the format 'First0.75_Second0.10_Third0.5_Fourth0.5' 
    return True if yes and False if no
    """
    return search(r'[A-Za-z]+\d+.?\d+_', chemicals_string_list) != None

def chem_table(sample, batch_id):
    chem_cols = ['chemical_id', 'batch_id', 'content', 'concentration', 'molarity', 'volume', 'chem_type']
    chem = pd.DataFrame(columns = chem_cols)
    
    # the first chemical has the id of 1, the first mix has the id of 1
    chemical_id = 1
    mix_id = 1
    
    sample_id = sample['name']
    worklist = sample['worklist']
    for step in worklist:
        # check if the 'details' is a key in worklist and check if steps is in the drops
        # if yes, go check the content for the chemical
        # if not, then move on since there is no chemical for mixing
        if 'details' in step and 'drops' in step['details']:
            for droplet in step['details']['drops']:
                # if it has both solvent and solute
                if 'solution' in droplet and droplet['solution']['solutes'] != '' and droplet['solution']['solvent'] != '':
                    # check if the solutes string follows the format to further breaking 
                    # it down using check_name_format helper function
                    # if yes, break the string down using the split_chemicals to get the name and the concentration
                    if check_name_format(droplet['solution']['solutes']):
                        for solute in split_chemicals(droplet['solution']['solutes']):
                            content, concentration = solute
                            if ((chem['content'] == content) & (chem['concentration'] == concentration)).sum() == 0:
                                new_row = create_new_row(chemical_id = chemical_id, batch_id = batch_id, 
                                                         content = content, concentration = concentration, 
                                                         chem_type = 'solute', sample_id = sample_id)

                                chem = chem.append(new_row, ignore_index=True)
                                chemical_id += 1
                    # if no, use the solute recipe name as the content (such as 'Xu-Recipe-PSK')
                    else:
                            new_row = create_new_row(chemical_id = chemical_id, batch_id = batch_id, 
                                                     content = droplet['solution']['solutes'], 
                                                     chem_type = 'solute', sample_id = sample_id)

                            chem = chem.append(new_row, ignore_index=True)
                            chemical_id += 1
                    # check if the antisolvent string follows the format to further breaking 
                    # it down using check_name_format helper function
                    # if yes, break the string down using the split_chemicals to get the name and the concentration
                    if check_name_format(droplet['solution']['solvent']):
                        for solvent in split_chemicals(droplet['solution']['solvent']):
                            content, concentration = solvent
                            if ((chem['content'] == content) & (chem['concentration'] == concentration)).sum() == 0:
                                new_row = create_new_row(chemical_id = chemical_id, batch_id = batch_id, 
                                                         content = content, concentration = concentration, 
                                                         chem_type = 'solvent', sample_id = sample_id)
                                chem = chem.append(new_row, ignore_index=True)
                                chemical_id += 1
                    # if no, use the solute recipe name as the content (such as 'Xu-Recipe-PSK')
                    else:
                        new_row = create_new_row(chemical_id = chemical_id, batch_id = batch_id, 
                                                 content = droplet['solution']['solutes'], 
                                                 chem_type = 'solvent', sample_id = sample_id)

                        chem = chem.append(new_row, ignore_index=True)
                        chemical_id += 1
                    # adding the mix (or solution) from the previous solvents and solutes
                    new_row = create_new_row(chemical_id = chemical_id, batch_id = batch_id, 
                                             content = 'Mix'+str(mix_id), volume = droplet['volume'], 
                                             molarity = droplet['solution']['molarity'],
                                             chem_type = 'solution', sample_id = sample_id)

                    chem = chem.append(new_row, ignore_index=True)
                    mix_id += 1
                    chemical_id += 1
                # check if the drop is an antisolvent (no solvent and no solute present)
                if 'solution' in droplet and droplet['solution']['solutes'] == '':
                    # check if the antisolvent is already in the df, add to the df if not in the df
                    if (chem['content'] == droplet['solution']['solvent']).sum() == 0:
                        new_row = create_new_row(chemical_id = chemical_id, batch_id = batch_id, 
                                                 content = droplet['solution']['solvent'],
                                                 molarity = droplet['solution']['molarity'],
                                                 chem_type = 'antisolvent', sample_id = sample_id)
                        chem = chem.append(new_row, ignore_index=True)
                        chemical_id += 1
                        
                    # adding the mix
                    new_row = create_new_row(chemical_id = chemical_id, batch_id = batch_id, 
                                             content = 'Mix'+str(mix_id),
                                             volume = droplet['volume'],
                                             chem_type = 'solution', sample_id = sample_id)

                    chem = chem.append(new_row, ignore_index=True)
                    mix_id += 1
                    chemical_id += 1
    return chem

def save_chem_csvs(samples, batch_id):
    """Takes in the dictionary of samples and run chem_table and save the resulting csv files.
    Replaces the whitespace in the name with underscore for Neo4J compatibility"""
    for sample in samples:
        filename = batch_id + '_' + sample + '_' + 'chem.csv'
        filename = filename.replace(' ', '_')
        chem_table(samples[sample], batch_id).to_csv(filename, index=False)
