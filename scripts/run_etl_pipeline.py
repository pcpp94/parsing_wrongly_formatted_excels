import sys
import os
import glob
sys.path.insert(0,'\\'.join(os.path.dirname(__file__).split('\\')[:-1]))

from src.parse_data import parse_and_load_data
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    parse_and_load_data()
