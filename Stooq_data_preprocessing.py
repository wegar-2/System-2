################################################################################
################################################################################
####################       STOOQ_DATA_PREPROCESSING         ####################
################################################################################
import os
import sys
import pandas as pd
import time

################################################################################
############## 0. Prepare the stage for the data unpacking  ####################
################################################################################
print("#######################################################################")
print("#### Countdown to start of Stooq_data_processing.py execution... ######")
print("#######################################################################")
for secs_left in range(10,0,-1):
    print("Seconds to start: ", secs_left)
    time.sleep(1)
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
    # Poland
    'd_pl_txt.zip',
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

time.sleep(5)

################################################################################
############## 1. Unpack the data sets in an orderly manner  ###################
################################################################################

# 1.1. Unzip the archives
print("#########################")
print("#########################")
print("Unpacking the archives...")
print("#########################")
print("#########################")
print("\n\n")


# define function that awaits for user
# acceptance before loading the next data set

for archive_name in expected_archives_list:
    print("Unpacking archive: ", archive_name)
    time.sleep(3)
    unpack_command = 'unzip ' + archive_name
    os.system(unpack_command)

print("Unzipping finished; additional data adjustments under way...")
for secs_left in range(5,0,-1):
    print("Moving on in ", secs_left, " seconds...")
    time.sleep(1)

# 1.2. Do the renaming of the directories inside the unpacked data
print("Current directory...")
print("os.getcwd(): ", os.getcwd())
# make directory for the data
os.system('mkdir Stooq_daily_data')
# copy the data into the Stooq_data directory
data_folders_list = [
    'de',
    'hk',
    'jp',
    'macro',
    'pl',
    'us',
    'world'
]

# move the data to new directory
for data_dir_name in data_folders_list:
    # move data to ne directory
    current_dir_path = os.getcwd() + '/data/daily/' + data_dir_name
    temp_command = "cp -R " + current_dir_path + ' ./Stooq_daily_data'
    os.system(temp_command)
    # remove data from the old directory
    temp_command = 'rm -rf ' + current_dir_path
    os.system(temp_command)

# making a list of old categories names to new categories names
world_old_categories_names = [
    'currencies\\major',
    'currencies\\other',
    'money market',
    'stooq stocks indices'
]

world_new_categories_names = [
    'currencies_major',
    'currencies_minor',
    'money_market',
    'stooq_stocks_indices'
]

world_old_new_names_dict = dict(zip(world_old_categories_names,
                                    world_new_categories_names))


# renaming the directories inside Stooq_daily_data/world
print("os.getcwd(): ", os.getcwd())
for iter_counter, iter_key in enumerate(world_old_new_names_dict.keys()):
    print("Changing name ", str(iter_counter), ": ")
    print("\t\t", "OLD: ", iter_key)
    print("\t\t", "NEW: ", world_old_new_names_dict[iter_key])
    old_name_dir = os.getcwd() + "/Stooq_daily_data/world/" + iter_key
    new_name_dir = os.getcwd() + "/Stooq_daily_data/world/" + \
                   world_old_new_names_dict[iter_key]
    temp_command = "mv " + "'"  + old_name_dir + "'" + ' ' + "'" + \
                   new_name_dir + "'"
    os.system(temp_command)


################################################################################
################## 2. Prepare the data dictionaries ############################
################################################################################

print("###########################################################")
print("######## Data dictionaries preparation starts in... #######")
print("###########################################################")
for secs_left in range(10,0,-1):
    print(secs_left, " second(s)...")
    time.sleep(1)

### 2.0. Load function(s) and do some preparatory stuff

# prepare directory for data dictionaries storage
os.system('mkdir Stooq_data_dictionaries')
for iter_elem in data_folders_list:
    os.system('mkdir ./Stooq_data_dictionaries/' + iter_elem)

# function that does the Stooq dictionaries parsing
def parse_stooq_dict(df_in):
    list_symbols = []
    list_names = []
    for k in range(len(df_in)):
        # fetch consecutive line
        temp_string = df_in.ix[k, 0]
        # remove blank leading an trailing spaces
        temp_string = temp_string.strip()
        # get symbol
        symbol_string = temp_string[0:temp_string.index(' ')]
        # get name
        name_string = temp_string[temp_string.index(' '):].strip()
        # save symbol_string and name_string to lists
        list_symbols.append(symbol_string)
        list_names.append(name_string)
    df_out = pd.DataFrame({'Symbol': list_symbols,
                           'Name': list_names})
    return df_out


### 2.1. Prepare (ticker, instrument name) dictionaries for 'World'
world_categories = [
    'bonds',
    'commodities',
    'currencies_major',
    'currencies_minor',
    'indices',
    'lme',
    'money_market',
    'stooq_stocks_indices'
]

world_categories_url_ids = [
    # bonds
    '53',
    # commodities
    '2',
    # currencies - major
    '3',
    # currencies - minor
    '52',
    # indices
    '1',
    # London Metals Exchange
    '31',
    # money market
    '39',
    # stooq stock indices
    '25'
]

world_categories_dict_url = dict(zip(world_categories, world_categories_url_ids))

world_list_of_dictionaries = []

# download dictionaires from urls:
for key_iter in world_categories_dict_url.keys():
    # print info on current iteration
    print("iteration: ", key_iter)
    # prepare the address to the data
    temp_url = "https://stooq.com/db/l/?g=" + world_categories_dict_url[key_iter]
    # get the dictionary from the URL address
    temp_dict = pd.read_csv(temp_url)
    temp_dict = parse_stooq_dict(temp_dict)
    # add the dictionary to the list world_list_of_dictionaries
    world_list_of_dictionaries.append(temp_dict)


# dictionary of dictionaries
world_dict_of_dicts = dict(zip(world_categories, world_list_of_dictionaries))

# save data dictionaries to /financial_data/Stooq_data_dictionaries/world directory
data_dicts_dir = os.getcwd() + "/Stooq_data_dictionaries/world/"
for df_iter in world_dict_of_dicts.keys():
    print("Saving data dictionary: ", df_iter)
    temp_df = world_dict_of_dicts[df_iter]
    temp_dir = data_dicts_dir + df_iter + ".csv"
    temp_df.to_csv(temp_dir)


