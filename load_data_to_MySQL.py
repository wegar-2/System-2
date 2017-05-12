###########################################################################################
###########################################################################################
########################       LOAD DATA TO MYSQL DATABASE         ########################
###########################################################################################

# imports
import pandas as pd
import mysql.connector
import mysql.connector.errorcode as err_cd
import os
import time
import sys
import progress.bar
import numpy as np
import datetime

print("##################################################################")
print("####### Script load_data_to_MySQL.py starts running in ... #######")
print("##################################################################")
for secs_left in range(10,0,-1):
    print(secs_left, "second(s)...")
    time.sleep(1)

################################################################################
################################################################################
######################## 0. main variables setup   #############################
################################################################################
################################################################################
# 0.1. data_root_dir - directory in which folders "data_world" and "tickers_dict_world"
#       are stores
print("os.getcwd(): ", os.getcwd())
time.sleep(1)
print("Moving one level up in the files hierarchy...")
head_dir, tail_dir = os.path.split(os.getcwd())
os.chdir(head_dir)
print("os.getcwd(): ", os.getcwd())
print("Moving into the financial_data directory...")
try:
    os.chdir(head_dir + "/financial_data")
except FileNotFoundError:
    print("Folder 'financial_data' not found! ")
    print("Execution of the script is put on hold! ")
    # stop execution if the folder financial_data does not exist
    sys.exit()

print("os.getcwd(): ", os.getcwd())
time.sleep(3)
print("Main directory with the resources successfully reached!")


### 0. Test connection to the database
print("Checking connection to the database as the client-user in...")
for secs_left in range(3,0,-1):
    print(secs_left, "second(s)...")
try:
    test_cnx = mysql.connector.connect(
        user="fin_db_client",
        password="test1234",
        host="127.0.0.1",
        database="pl_fin_db"
    )
# catch error in mysql.connector.Error
except mysql.connector.Error as err:
    if err.errno == err_cd.ER_ACCESS_DENIED_ERROR:
        print("Connection attempt: failed")
        print("User name/password related error...")
    elif err.errno == err_cd.ER_BAD_DB_ERROR:
        print()
else:
    # executed if there is no error in the 'try' block
    print("Connection attempt: success!")
    test_cnx.close()


regions_list = [
        'de',
        'hk',
        'jp',
        'pl',
        'us',
        'world'
]
# list of lists ofcategories for different regions
categories_list_of_lists = [
    # de
    [
        'xetra_stocks'
    ],
    # hk
    [
        'hkex_stocks'
    ],
    # jp
    [
        'tse_futures',
        'tse_indices',
        'tse_stocks'
    ],
    # pl
    [
        'wse_stocks'
    ],
    # us
    [
        'nasdaq_stocks',
        'nyse_stocks',
        'nysemkt_stocks'
    ],
    # world
    [
        'bonds',
        'commodities',
        'currencies_major',
        'currencies_minor',
        'indices',
        'lme',
        'money_market',
        'stooq_stocks_indices'
    ]
]
dict_regions_to_categories = dict(zip(regions_list, categories_list_of_lists))

################################################################################
################################################################################
##### 1. load functions that will be used to export data to MySQL Server #######
################################################################################
################################################################################


# 1.0. helper function to patrse ticker symbol
def parse_ticker_symbol(symbol_to_parse):
    # changing "." ===> "_" in the list of symbols
    symbol_to_parse = symbol_to_parse.replace(".", "_")
    # changing "^" ===> "hat_" in the list of symbols
    symbol_to_parse = symbol_to_parse.replace("^", "hat_")
    # changing "-" ===> "_dash_" in the list of symbols
    symbol_to_parse = symbol_to_parse.replace("-", "_dash_")
    return symbol_to_parse


# 1.1. function that data dictionary DataFrame with data to table in the server
def upload_datadict_dataframe_to_mysql_server(dataframe_in, target_table,
                                          connection_dictionary):
    # 1. open the connection and create a cursor
    temp_cnx = mysql.connector.connect(**connection_dictionary)
    temp_cursor = temp_cnx.cursor()
    # 2. do the data inserts into the table in a loop
    # 2.1. insert dataframes rows into table with INSERT INTO
    for iter_ind, df_iterator in dataframe_in.iterrows():
        # build the command root
        temp_command = "INSERT INTO " + target_table + "(NAME, SYMBOL) VALUES ("
        # values to be inserted
        iter_name_val = str(df_iterator['Name'])
        # dropping single quotes from the instrument name
        iter_name_val = iter_name_val.replace("'", "")
        iter_symbol_val = str(df_iterator['Symbol'])
        # parse ticker symbol: replacement of special characeters
        iter_symbol_val = parse_ticker_symbol(iter_symbol_val)
        temp_command += "'" + iter_name_val + "', " + "'"+ \
                        iter_symbol_val + "' );"
        # ensure all is uppercase
        temp_command = temp_command.upper()
        # execute the command for iteration
        print(temp_command)
        temp_cursor.execute(temp_command)
    # 2.2. commit the inserts
    temp_cnx.commit()
    # 3. close: cursor and connection
    temp_cursor.close()
    temp_cnx.close()


# 1.2. function that uploads DataFrame with data to table in the server
def upload_data_dataframe_to_mysql_server(dataframe_in, target_table,
                                          connection_dictionary):
    # 1. open the connection and create a cursor
    temp_cnx = mysql.connector.connect(**connection_dictionary)
    temp_cursor = temp_cnx.cursor()
    # 2. do the data inserts into the table in a loop
    # 2.1. insert dataframes rows into table with INSERT INTO
    for iter_ind, df_iterator in dataframe_in.iterrows():
        # build the command root
        temp_command = "INSERT INTO " + target_table + "(Date, Close) VALUES ("
        # values to be inserted
        iter_date_val = str(df_iterator['Date'])[0:10]
        iter_close_val = str(df_iterator['Close'])
        temp_command += "'" + iter_date_val + "', " + iter_close_val + ");"
        temp_command = temp_command.upper()
        # execute the command for iteration
        temp_cursor.execute(temp_command)
    # 2.2. commit the inserts
    temp_cnx.commit()
    # 3. close: cursor and connection
    temp_cursor.close()
    temp_cnx.close()


################################################################################
# 2. Loading the data dictionaries to the MySQL Server database "data_dicts_db"#
################################################################################
################################################################################

print("#######################################################################")
print("###########  Loading data dictionaries to the server ...  #############")
print("#######################################################################")

# current directory here is "../financial_data"

# loop through regions

datadicts_connection_dict = {
    'user': 'FIN_DB_CLIENT',
    'password': 'test1234',
    'host': '127.0.0.1',
    'database': 'DATA_DICTS_DB'
}

for iter_key_region in dict_regions_to_categories.keys():
    # loop through categories for a given region
    for iter_reg_categ in dict_regions_to_categories[iter_key_region]:
        # load the dataframe with the dictionary for (region, category)
        data_dict_dir = os.getcwd() + "/Stooq_data_dictionaries/" + str(iter_key_region) + "/" + \
                        str(iter_reg_categ) + ".csv"
        print("Reading in data dictionary from: ", data_dict_dir)
        temp_df_in = pd.read_csv(data_dict_dir)
        temp_target_table = str(iter_reg_categ).upper()
        upload_datadict_dataframe_to_mysql_server(dataframe_in= temp_df_in,
                                                  target_table=temp_target_table,
                                                  connection_dictionary=
                                                  datadicts_connection_dict
                                                  )


print("#######################################################################")
print("####### Finished loading data dictionaries to the server ...  #########")
print("#######################################################################")


##############################################################################
##############################################################################
########## 3. Loading the data into databases "XXX_fin_db"  ##################
##############################################################################
##############################################################################

print("#######################################################################")
print("##############  Loading time series to the server ...  ################")
print("#######################################################################")


# current directory is assumed to be .../financial_data/.
print("\n\n\n")
print("os.getcwd(): ", os.getcwd())
print("\n\n\n")

data_connect_dict = {
    'user': 'FIND_DB_CLIENT',
}

################################################################################
###########################    REGION-level loop    ############################
################################################################################
for iter_key_region in dict_regions_to_categories.keys():
    print("\n\n\n\n\n")
    print("Data load for region: ", str(iter_key_region))
    for secs_left in range(5,0,-1):
        print(str(secs_left), "second(s) to start...")
        time.sleep(1)
    ############################################################################
    ################    CATEGORY-level loop for a REGION    ####################
    ############################################################################
    for iter_reg_categ in dict_regions_to_categories[iter_key_region]:
        print("\n")
        print("Data load for region: ", str(iter_key_region), ", category: ",
              str(iter_reg_categ))
        for secs_left in range(3, 0, -1):
            print(str(secs_left), "second(s) to start...")
            time.sleep(1)
        # prepare connection dictionary

        # 1. load the data dictionary from csv file
        data_dict_dir = os.getcwd() + "/Stooq_data_dictionaries/" + str(iter_key_region) + "/" + \
                        str(iter_reg_categ) + ".csv"
        current_data_dict = pd.read_csv(data_dict_dir)
        print("Loaded data dictionary from: ", data_dict_dir)
        # add column with parsed names (names that are consistent with database
        print("Category: ", str(iter_reg_categ))
        # prepare root of the directory to dataframes with data
        temp_df_dir_root = os.getcwd() + "/Stooq_data_dictionaries/" + \
                      str(iter_reg_categ) + "/" + str(iter_reg_categ) + "/"
        # iterate through the loaded data dictionary and load data
        for iter_index, iter_vals in current_data_dict.iterrows():
            # make full dir to data
            temp_df_dir = temp_df_dir_root + str(iter_vals['Symbol'])
            # load the data from a dataframe
            temp_df_in = pd.read_csv(temp_df_dir, parse_dates=[0],
                                     usecols=['Date', 'Close'])
            # target_table - parse symbol...
            temp_target_table = parse_ticker_symbol(iter_vals['Symbol'])
            # call the loader function
            upload_data_dataframe_to_mysql_server(dataframe_in=temp_df_in,
                                                  target_table=temp_target_table,
                                                  connection_dictionary=
                                                  data_connect_dict
                                                  )



print("#######################################################################")
print("########## Finished loading time series to the server ...  ############")
print("#######################################################################")


################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################