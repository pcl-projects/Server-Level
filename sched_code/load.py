from cpu_load_generator import load_single_core, load_all_cores
import sys
import random

# cpu_core = int(sys.argv[1])

import csv
import pandas as pd
import numpy as np

df = pd.read_csv("dataset.csv", header=None)

while(True):
	load_all_cores(duration_s=30, target_load=0.6)  # generates load on all cores
	# load = round(random.uniform(0.2, 0.7),2)
	# load_single_core(core_num=cpu_core, duration_s=26, target_load=load)  # generate load on single core (0)
# load_all_cores(duration_s=30, target_load=0.2)  # generates load on all cores
# from_profile(path_to_profile_json=r"c:\profiles\profile1.json")
# print("done")