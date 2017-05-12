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
    symbol_to_parse.replace(".", "_")
    # changing "^" ===> "hat_" in the list of symbols
    symbol_to_parse.replace("^", "hat_")
    # changing "-" ===> "_dash_" in the list of symbols
    symbol_to_parse.replace("-", "_dash_")
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
        iter_symbol_val = str(df_iterator['Symbol'])
        # parse ticker symbol: replacement of special characeters
        iter_symbol_val = parse_ticker_symbol(iter_symbol_val)
        temp_command += "'" + iter_name_val + "', " + "'"+ \
                        iter_symbol_val + "' );"
        # ensure all is uppercase
        temp_command = temp_command.upper()
        # execute the command for iteration
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


# test_file_dir = "/home/wegar/github_repos/financial_data/Stooq_daily_data/de/xetra_stocks/zo1.de.txt"
# df_loaded = pd.read_csv(test_file_dir, parse_dates=[0], usecols=['Date', 'Close'])
# sample_dataframe_in = df_loaded
# sample_target_table = "zo1_de"
# sample_connection_dictionary = {
#     'user': 'FIN_DB_CLIENT',
#     'password': 'test1234',
#     'host': '127.0.0.1',
#     'database': 'DE_FIN_DB'
# }
#
# #test run
# time_start = time.perf_counter()
# upload_dataframe_to_mysql_server(dataframe_in=sample_dataframe_in,
#                                  target_table=sample_target_table,
#                                  connection_dictionary=sample_connection_dictionary)
# time_end = time.perf_counter()
# print("Time passed: ", str(time_end - time_start))

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


###############################################################################
###############################################################################
########### 3. Loading the data into databases "XXX_fin_db"  ##################
###############################################################################
###############################################################################
#
# print("#######################################################################")
# print("###########  Loading data dictionaries to the server ...  #############")
# print("#######################################################################")
#
#
#
#
# print("#######################################################################")
# print("####### Finished loading data dictionaries to the server ...  #########")
# print("#######################################################################")

#
# ################################################################################
# ################################################################################
# ################################################################################
# ################################################################################
# ################################################################################
# ################################################################################
#
# # test load of a data table
# test_csv_dir = "/home/wegar/github_repos/financial_data/Stooq_daily_data/de/xetra_stocks/zo1.de.txt"
# df_loaded = pd.read_csv(test_csv_dir, usecols=['Date', 'Close'], parse_dates=[0])
#
# # check data types in the dataframe:
# for iter_col_name in df_loaded.columns:
#     print(type(df_loaded.loc[0, iter_col_name]))
#     isinstance(df_loaded.loc[0, iter_col_name], np.float64)
#
# # test load of a dictionary table
# test_datadict_csv_dir = "/home/wegar/github_repos/financial_data/Stooq_data_dictionaries/de/xetra_stocks.csv"
# temp_datadict_df = pd.read_csv(test_datadict_csv_dir, index_col=0)
#
# sample_dataframe_in = temp_datadict_df
# sample_target_table = 'XETRA_STOCKS'
# sample_connection_dictionary = {
#     'user': 'FIN_DB_CLIENT',
#     'password': 'test1234',
#     'host': '127.0.0.1',
#     'database': 'DATA_DICTS_DB'
# }
#
# upload_datadict_dataframe_to_mysql_server(dataframe_in=sample_dataframe_in,
#                                           target_table=sample_target_table,
#                                           connection_dictionary=
#                                           sample_connection_dictionary)
