import sys
from datapipeline.loader import process_executor as pe
process_name = str(sys.argv[1])
print (process_name)
config_file_path = str(sys.argv[2])
print (config_file_path)
print ("Process:"+process_name+" Start")
pe.main(process_name=process_name,config_file=config_file_path)
print ("Process:"+process_name+" End")