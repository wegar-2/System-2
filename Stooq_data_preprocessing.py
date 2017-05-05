################################################################################
################################################################################
####################       STOOQ_DATA_PREPROCESSING         ####################
################################################################################
import os
import sys

################################################################################
############## 0. Prepare the stage for the data unpacking  ####################
################################################################################

print("##########################################")
print("##########################################")
print("Executing Stooq_data_preprocessing.py ....")
print("##########################################")
print("##########################################")
print("os.getcwd() = ", os.getcwd())
print("Changing directory to the data directory...")
head_dir, tail_dir = os.path.split(os.getcwd())
os.chdir(head_dir)
print("Inside the main folder of the application...")
print("List of elements inside the main directory of the application: ")
for iter1, elem1 in enumerate(os.listdir(os.getcwd())):
    print(str(iter1), ": ", elem1)
# change directory to the data directory
try:
    os.chdir(head_dir + "/financial_data")
except FileNotFoundError:
    print("Folder 'financial_data' not found. ")
    print("Please create it to proceed. ")
    # stop execution if the folder financial_data does not exist
    sys.exit()


print("##########################################")
print("Inside the 'financial_data' directory. ")
print("##########################################")
print("os.getcwd() = ", os.getcwd())
print("##########################################")
print("List of contents of the current directory:")
print("##########################################")
list_of_contents = os.listdir(os.getcwd())
# display the content in loop
for iter1, el in enumerate(list_of_contents):
    print(str(iter1), ": ", el)

# hardcoded list of archives that are expected to be found in the directory
expected_archives_list = [
    # Germany
    'd_de_txt.zip',
    # Honk Kong
    'd_hk_txt.zip',
    # Japan
    'd_jp_txt.zip',
    # macroeconomic data
    'd_macro_txt.zip',
    # United States
    'd_us_txt.zip',
    # World - umbrella term
    'd_world_txt.zip'
]

# check the archives that are in the directory financial_data
archive_presence_checklist = []

for el in expected_archives_list:
    archive_presence_checklist.append(el in list_of_contents)
archives_presence = dict(zip(expected_archives_list, archive_presence_checklist))

# display presence check conclusions
print("#######################################################################")
print("Data archives found: ")
print("#######################################################################")
for el in archives_presence.keys():
    print(el, "\t\t\t\t\t\t", archives_presence[el])

# stop the execution if one of the archives is missing
if False in archives_presence.values():
    print("At least one of the data archives is missing. ")
    print("Make sure that all the archives are in the financial_data folder! ")
    print("Script execution stopped!!!")
    sys.exit()

################################################################################
############## 1. Unpack the data sets in an orderly manner  ###################
################################################################################

print("#########################")
print("#########################")
print("Unpacking the archives...")
print("#########################")
print("#########################")
print("\n\n")
for archive_name in expected_archives_list:
    print("Unpacking archive: ", archive_name)
    unpack_command = 'unzip ' + archive_name
    os.system(unpack_command)

################################################################################
############## 2. Load the data dictionaries for 'World' #######################
################################################################################