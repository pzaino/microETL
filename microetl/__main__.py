########################################################
#    Name: ETLEng Main
# Release: 0.0.1
# Purpose: Minimal Data Transformation ETL Engine
#          main function, use this to have a fully 
#          functional ETL Engine and pipeline
#  Author: Paolo Fabio Zaino 
#   Usage: Check docs/ETLEng.md
########################################################

# Import the required modules:
import os
import sys
import getopt

import core as metl

def main(argv):
  # Set the default paths:
  base_path = os.getcwd()
  cfg_path = os.path.join(base_path, 'jobs')
  out_path = os.path.join(base_path, 'out_data')
  inp_path = os.path.join(base_path, 'inp_data')

  opts, args = getopt.getopt(argv,"hj:b:",["help","jobs=","base=","inp=","out="])
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      print ('microetl -j <jobs_configs_path> -i <inp_data_path> -o <out_data_path> -b <base_path>')
      sys.exit()
    elif opt in ("-j", "--jobs"):
      cfg_path = arg
    elif opt in ("-b", "--base"):
      base_path = arg
    elif opt in ("-i", "--inp"):
      inp_path = arg
    elif opt in ("-o", "--out"):
      out_path = arg

  # Load YAML test configuration:
  for filename in os.listdir(cfg_path):
    if filename.lower().endswith('.yaml') or filename.lower().endswith('.yml'):
      metl.etleng_run_pipeline_from_config(filename, str(base_path), str(cfg_path), str(inp_path), str(out_path))

if __name__ == "__main__":
    main(sys.argv[1:])