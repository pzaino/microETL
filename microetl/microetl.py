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
  cfg_path = os.getcwd() + '/jobs/'
  out_path = os.getcwd() + '/out_data/'
  inp_path = os.getcwd() + '/inp_data/'

  opts, args = getopt.getopt(argv,"hj:",["help","jobs="])
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      print ('microetl -i <configs_path>')
      sys.exit()
    elif opt in ("-j", "--jobs"):
      cfg_path = arg


if __name__ == "__main__":
    main(sys.argv[1:])