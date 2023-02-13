from os import listdir, remove
from os.path import isfile, join
from json import load, loads
from tifffile import imread,imwrite
from re import search, findall
import numpy as np
import pandas as pd

def get_data():
    '''
    Reads and returns data from the test/testdata directory
    '''
    data_list = []
    fp = "test/testdata/"
    files = [
        'test_process.json',
        'test_char.json']
    for i in files:
        f = open(fp + i, 'r')
        data = load(f)
        data_list.append(data)
        f.close()
    
    data_list.append(['Characterization_B19', 'sample0'])
    return data_list


        