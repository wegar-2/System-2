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

# 1.2. Do the renaming of the directories with the data
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

str1 = "directory tree of the /financial_data/Stooq_daily_data"
str1.upper()

################################################################################
################################################################################
######### FULL DIRECTORY TREE OF THE /FINANCIAL_DATA/STOOQ_DAILY_DATA ##########
################################################################################
################################################################################
########### directories that are loaded to the database are marked  ############
################################################################################
# .
# ├── de
# │   └── xetra stocks                                          [LOADED TO DB]
# ├── hk
# │   ├── hkex cbbcs
# │   ├── hkex corporate bonds
# │   ├── hkex drs
# │   ├── hkex dws
# │   ├── hkex etfs
# │   ├── hkex reits
# │   ├── hkex stocks                                           [LOADED TO DB]
# │   └── hkex treasury bonds                                   [LOADED TO DB]
# ├── jp
# │   ├── tse corporate bonds
# │   ├── tse etfs
# │   ├── tse futures                                           [LOADED TO DB]
# │   ├── tse indices                                           [LOADED TO DB]
# │   ├── tse options
# │   ├── tse stocks                                            [LOADED TO DB]
# │   └── tse treasury bonds
# ├── macro
# │   ├── au
# │   ├── ca
# │   ├── ch
# │   ├── cn
# │   ├── de
# │   ├── eu
# │   ├── fr
# │   ├── it
# │   ├── jp
# │   ├── no
# │   ├── pl
# │   ├── uk
# │   └── us
# ├── pl
# │   ├── funds absolute return
# │   ├── funds commodity
# │   ├── funds foreign bond
# │   ├── funds foreign capital protection
# │   ├── funds foreign equity
# │   ├── funds foreign mixed
# │   ├── funds foreign money market
# │   ├── funds polish bond
# │   ├── funds polish capital protection
# │   ├── funds polish equity
# │   ├── funds polish mixed
# │   ├── funds polish money market
# │   ├── funds private equity
# │   ├── funds real estate
# │   ├── funds securitization
# │   ├── nc indices
# │   ├── nc indices indicators
# │   ├── nc pre-emptive rights
# │   ├── nc stocks
# │   ├── nc stocks indicators
# │   ├── wse bonds
# │   ├── wse certificates
# │   ├── wse etfs
# │   ├── wse futures
# │   ├── wse indices
# │   ├── wse indices indicators
# │   ├── wse options
# │   ├── wse pre-emptive rights
# │   ├── wse stocks                                            [LOADED TO DB]
# │   └── wse stocks indicators
# ├── us
# │   ├── nasdaq etfs
# │   ├── nasdaq stocks                                         [LOADED TO DB]
# │   ├── nyse etfs
# │   ├── nysemkt etfs
# │   ├── nysemkt stocks                                        [LOADED TO DB]
# │   └── nyse stocks                                           [LOADED TO DB]
# └── world
#     ├── bonds                                                 [LOADED TO DB]
#     ├── commodities                                           [LOADED TO DB]
#     ├── currencies_major                                      [LOADED TO DB]
#     ├── currencies_minor                                      [LOADED TO DB]
#     ├── indices                                               [LOADED TO DB]
#     ├── lme                                                   [LOADED TO DB]
#     ├── money_market                                          [LOADED TO DB]
#     └── stooq_stocks_indices                                  [LOADED TO DB]
#
#


# 1.3. renaming of the elements in the directory - continued
# de
de_old_categories_names = [
    'xetra stocks'
]
de_new_categories_names = [
    'xetra_stocks'
]
de_old_new_names_dict = dict(zip(de_old_categories_names,
                                 de_new_categories_names))

# hk
hk_old_categories_names = [
    'hkex stocks'
]
hk_new_categories_names = [
    'hkex_stocks'
]
hk_old_new_names_dict = dict(zip(hk_old_categories_names,
                                 hk_new_categories_names))

# jp
jp_old_categories_names = [
    'tse futures',
    'tse indices',
    'tse stocks'
]
jp_new_categories_names = [
    'tse_futures',
    'tse_indices',
    'tse_stocks'
]
jp_old_new_names_dict = dict(zip(jp_old_categories_names, jp_new_categories_names))


# pl
pl_old_categories_names = [
    'wse stocks'
]
pl_new_categories_names = [
    'wse_stocks'
]
pl_old_new_names_dict = dict(zip(pl_old_categories_names,
                                 pl_new_categories_names))

# us
us_old_categories_names = [
    'nasdaq stocks',
    'nysemkt stocks',
    'nyse stocks'
]
us_new_categories_names = [
    'nasdaq_stocks',
    'nysemkt_stocks',
    'nyse_stocks'
]
us_old_new_names_dict = dict(zip(us_old_categories_names,
                                 us_new_categories_names))

# world
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
curr_dir = os.getcwd()
print("curr_dir: ", curr_dir)

def adjust_naming_in_data_directories(region_name, region_old_new_names_dict,
                                      curr_dir):
    """
    Function adjust_naming_in_data_directories() is used to change names in the
    directory tree of the downloaded data. 
    :param region_name: name of the region for which the names in the directory 
    tree are changes 
    :param region_old_new_names_dict: dictionary of the names to be changed 
    :param curr_dir: current directory - argument that is required to build 
    commands inside the functions
    :return: no returns...
    """
    print("\n")
    print("Inside adjust_naming_in_data_directories() function...")
    print("Argument categ_name passed to this function: ", region_name)
    print("Argument curr_dir passed to this function: ", curr_dir)
    print("Starting renaming...")
    for iter_counter, iter_key in enumerate(region_old_new_names_dict.keys()):
        print("Changing name ", str(iter_counter), ": ")
        print("\t\t", "OLD: ", iter_key)
        print("\t\t", "NEW: ", region_old_new_names_dict[iter_key])
        old_name_dir = curr_dir + "/Stooq_daily_data/" + region_name + "/" + iter_key
        new_name_dir = curr_dir + "/Stooq_daily_data/" + region_name + "/" + \
                       region_old_new_names_dict[iter_key]
        temp_command = "mv " + "'" + old_name_dir + "'" + ' ' + "'" + \
                       new_name_dir + "'"
        os.system(temp_command)
        #sleep to let the user have a quick look
        time.sleep(1.5)
    print("Leaving function: adjust_naming_in_data_directories()...")

# run the function adjust_naming_in_data_directories() for different regions

# 1.3.1.'de' - renaming
adjust_naming_in_data_directories('de', de_old_new_names_dict, curr_dir)
# 1.3.2. 'hk' - renaming
adjust_naming_in_data_directories('hk', hk_old_new_names_dict, curr_dir)
# 1.3.3. 'jp' - renaming
adjust_naming_in_data_directories('jp', jp_old_new_names_dict, curr_dir)
# 1.3.4. 'pl' - renaming
adjust_naming_in_data_directories('pl', pl_old_new_names_dict, curr_dir)
# 1.3.5. 'us' - renaming
adjust_naming_in_data_directories('us', us_old_new_names_dict, curr_dir)
# 1.3.6. 'world' -renaming
adjust_naming_in_data_directories('world', world_old_new_names_dict, curr_dir)

# 1.4. Additional operations on the data directories: \
# merging the subfolders inside /financial_data/Stooq_daily_data/...
# 1.4.1. us/nyse_stocks/ <--- 1-4
# iterating over folders 1 to 4 in the us/nyse_stocks/ directory
print("Ordering directories in /us/nyse_stocks...")
for subfolder_name in range(1,5,1):
    print("\n\n")
    source_dir = os.getcwd() + "/Stooq_daily_data/us/nyse_stocks/" + \
                 str(subfolder_name)
    target_dir = os.getcwd() + "/Stooq_daily_data/us/nyse_stocks/"
    temp_command = "cp " + source_dir + "/* " + target_dir
    print("Executing command: ", temp_command)
    os.system(temp_command)
    temp_command = "rm -rf " + source_dir
    print("Tidying up the source...")
    print("Executing command: ", temp_command)
    os.system(temp_command)

# 1.4.2. jp/tse_stocks/ <---- 1-2
print("Ordering directories in /jp/tse_stocks/")
for subfolder_name in range(1,3,1):
    print("\n\n")
    source_dir = os.getcwd() + "/Stooq_daily_data/jp/tse_stocks/" + \
                 str(subfolder_name)
    target_dir = os.getcwd() + "/Stooq_daily_data/jp/tse_stocks/"
    temp_command = "cp " + source_dir + "/* " + target_dir
    print("Executing command: ", temp_command)
    os.system(temp_command)
    temp_command = "rm -rf " + source_dir
    print("Tidying up the source...")
    print("Executing command: ", temp_command)
    os.system(temp_command)



################################################################################
################## 2. Prepare the data dictionaries ############################
################################################################################
print("###########################################################")
print("######## Data dictionaries preparation starts in... #######")
print("###########################################################")
for secs_left in range(5,0,-1):
    print(secs_left, " second(s)...")
    time.sleep(1)


# 2.0. Load function(s)
# 2.0.1. prepare directory for data dictionaries storage
os.system('mkdir Stooq_data_dictionaries')
for iter_elem in data_folders_list:
    os.system('mkdir ./Stooq_data_dictionaries/' + iter_elem)


# 2.0.1. function that does the Stooq dictionaries parsing
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


# 2.0.2. prepare_data_dictionaries - function that does the heavylifting related
# to dictionaries preparation
def prepare_data_dictionaries(region_name, region_categories,
                              region_categories_dict_url, curr_dir):
    print("Inside function prepare_data_dictionaries()...")
    print("Argument - region_name: ", region_name)

    region_list_of_dictionaries = []

    for key_iter in region_categories_dict_url.keys():
        # sleep for a short instant
        time.sleep(2)
        # print info on current iteration
        print("iteration: ", key_iter)
        # prepare the address to the data
        temp_url = "https://stooq.com/db/l/?g=" + region_categories_dict_url[
            key_iter]
        print("temp_url: ", temp_url)
        # get the dictionary from the URL address
        temp_dict = pd.read_table(temp_url)
        temp_dict = parse_stooq_dict(temp_dict)
        # add the dictionary to the list world_list_of_dictionaries
        region_list_of_dictionaries.append(temp_dict)

    # dictionary of dictionaries
    region_dict_of_dicts = dict(
        zip(region_categories, region_list_of_dictionaries))

    # save data dictionaries to /financial_data/Stooq_data_dictionaries/world
    # directory
    data_dicts_dir = curr_dir + "/Stooq_data_dictionaries/" + region_name + "/"
    for df_iter in region_dict_of_dicts.keys():
        #stop for a short intance
        time.sleep(2)
        print("Saving data dictionary: ", df_iter)
        temp_df = region_dict_of_dicts[df_iter]
        temp_dir = data_dicts_dir + df_iter + ".csv"
        temp_df.to_csv(temp_dir)

# 2.1. Prepare (ticker, instrument name) dictionaries
# 2.1.1.'de'
de_categories = [
    'xetra_stocks'
]
de_categories_url_ids = [
    '29'
]
de_categories_dict_url = dict(zip(de_categories, de_categories_url_ids))

# 2.1.2. 'hk'
hk_categories = [
    'hkex_stocks'
]
hk_categories_url_ids = [
    '73'
]
hk_categories_dict_url = dict(zip(hk_categories, hk_categories_url_ids))

# 2.1.3. 'jp'
jp_categories = [
    'tse_futures',
    'tse_indices',
    'tse_stocks'
]
jp_categories_url_ids = [
    '35',
    '33',
    '32'
]
jp_categories_dict_url = dict(zip(jp_categories, jp_categories_url_ids))

# 2.1.4. 'pl'
pl_categories = [
    'wse_stocks'
]
pl_categories_url_ids = [
    # stocks quoted at Warsaw Stock Exchange
    '6'
]
pl_categories_dict_url = dict(zip(pl_categories, pl_categories_url_ids))

# 2.1.5. 'us'
us_categories = [
    'nasdaq_stocks',
    'nysemkt_stocks',
    'nyse_stocks'
]
us_categories_url_ids = [
    # NASDAQ stocks
    '27',
    '26',
    # NYSE stocks
    '28'
]
us_categories_dict_url = dict(zip(us_categories, us_categories_url_ids))

# 2.1.6. 'world'
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

# 2.2. execute prepare_data_dictionaries() function to make dictionaries for
# different regions
curr_dir = os.getcwd()
print("os.getcwd(): ", os.getcwd())
print("curr_dir: ", curr_dir)
# 2.7.1. de
prepare_data_dictionaries('de', de_categories, de_categories_dict_url, curr_dir)
# 2.7.2. hk
prepare_data_dictionaries('hk', hk_categories, hk_categories_dict_url, curr_dir)
# 2.7.3. jp
prepare_data_dictionaries('jp', jp_categories, jp_categories_dict_url, curr_dir)
# 2.7.4. pl
prepare_data_dictionaries('pl', pl_categories, pl_categories_dict_url, curr_dir)
# 2.7.5. us
prepare_data_dictionaries('us', us_categories, us_categories_dict_url, curr_dir)
# 2.7.6. world
prepare_data_dictionaries('world', world_categories, world_categories_dict_url,
                          curr_dir)


print("#######################################################################")
print("#######################################################################")
print("########## Stooq_data_processing.py execution finished... #############")
print("#######################################################################")
print("#######################################################################")