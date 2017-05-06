###########################################################################################
###########################################################################################
########################       LOAD DATA TO MYSQL DATABASE         ########################
###########################################################################################

### imports ###
import glob
import pandas as pd
import mysql.connector
import re
import os
import time
import sys

print("##################################################################")
print("####### Script load_data_to_MySQL.py starts running in ... #######")
print("##################################################################")
for secs_left in range(10,0,-1):
    print(secs_left, "second(s)...")
    time.sleep(1)

#########################
# 0. main variables setup
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

### 1. Loading data dictionaries to the database

### 2. Loading the data to the database


# ######################
# # 1. load dictionaries
# # 1.1. setup directory to the tickers dictionaries
# tickers_dict_dir = data_root_dir + "tickers_dict_world/world"
# # 1.2. load the elements of the directory with dictionaries
# dictionaries_files = glob.glob(tickers_dict_dir+"*")
# # 1.3. display info on dictionary files that have been found
# print("\n\n\n")
# print("Located dictionaries of ticker symbols: ")
# for k in range(len(dictionaries_files)):
#     print("Dictionary number ", str(k+1), ":\t\t", dictionaries_files[k])
#
#
#
# # 1.4. loop through the elements in the dictionary and load them
# # 1.4.1. function to load the two columns from dictionaries
# def load_tickers_dictionary(temp_dir):
#     temp_file = open(temp_dir, 'r')
#     temp_file_contents = temp_file.readlines()
#     temp_file.close()
#     # create an empty DataFrame with contents
#     df_out = pd.DataFrame(index=list(range(len(temp_file_contents) - 1)),
#                           columns=['Symbol', 'Name'])
#     # fill in the DataFrame with the dictionary content
#     for k in range(len(temp_file_contents)):
#         if k == 0:
#             pass
#         else:
#             temp_file_contents[k] = temp_file_contents[k].rstrip("\n")
#             temp_symbol = temp_file_contents[k][0:(temp_file_contents[k].find(" "))]
#             temp_name = temp_file_contents[k][temp_file_contents[k].find(" "):].strip()
#             # remove special character from the string temp_name
#             temp_name = re.sub(r'([^\s\w]|_)+', '', temp_name)
#             # save the transformations result into the DataFrame
#             df_out.loc[k - 1, 'Symbol'] = temp_symbol
#             df_out.loc[k - 1, 'Name'] = temp_name
#     return df_out
# # 1.4.2. loading the two dictionaries into DataFrames
# dict_of_tickers_dicts = {}
# for k in range(len(dictionaries_files)):
#     print(dictionaries_files[k])
#     temp_file = dictionaries_files[k]
#     df_temp = load_tickers_dictionary(temp_file)
#     name_temp = temp_file[temp_file.rfind("/")+1:(len(str(temp_file))-4)]
#     dict_of_tickers_dicts[name_temp] = df_temp
#
# print("####################")
# print("####################")
# print("Loaded dictionaries:")
# print("####################")
# print("\n\n")
#
# for el in dict_of_tickers_dicts.keys():
#     print("\n\n\n")
#     print("Dictionary name: ", str(el))
#     print("Content of the dictionary: ")
#     print(dict_of_tickers_dicts[el])
#
# # 1.5. Load the dictionaries into the MySQL DB
#
# # 1.5.1. loop through the dictionary of tickers dictionaries and create
# # tables for them in the DB
#
# # 1.5.1.1. prepare commands
# dict_of_queries = {}
# for my_iter in dict_of_tickers_dicts.keys():
#     temp_query = (
#         "CREATE TABLE " + str(my_iter) + " ("
#         "   Symbol      varchar(40)     NOT NULL,     "
#         "   Name        varchar(40)     NOT NULL     "
#         ") ENGINE=InnoDB")
#     # save the query in dictionary
#     dict_of_queries[my_iter] = temp_query
# # 1.5.1.2. execute commands
# for my_iter in dict_of_tickers_dicts.keys():
#     temp_query = dict_of_queries[my_iter]
#     cnx1 = mysql.connector.connect(user="wegar", password="test1234",
#                               host="127.0.0.1",
#                               database="financial_mdb")
#     # create a cursor to be able to execute commands
#     cursor1 = cnx1.cursor()
#     # print information on the command that it executed
#     print("Executing: ", temp_query)
#     # execute command
#     cursor1.execute(temp_query)
#     # close the cursor
#     cursor1.close()
#     # close the connection
#     cnx1.close()
#
# # 1.5.2. loop through the dictionaries of tickers dictionaries and
# # load them into the previously prepared tables in DB
# # 1.5.2.1. function that populates indicated table with values from a specified DataFrame
# def populate_MySQL_table(connection_dict, df_in, table_name):
#     # dsiplay info on what is happening...
#     print("\n\n")
#     print("##########################################################")
#     print("##########################################################")
#     print("Populating rows of dictionary table : ", table_name)
#     print("##########################################################")
#     # main body of the command
#     command1_body = "INSERT INTO " + table_name + "(Symbol, Name) VALUES "
#     # open the connection
#     my_con1 = mysql.connector.connect(**connection_dict)
#     # create a cursor
#     my_cur1 = my_con1.cursor()
#     # open connection
#     temp_cnx = mysql.connector.connect(**connection_dict)
#     # loop through the elements of the dictionary and add insert them into DB tables
#     # open cursor
#     temp_cursor = temp_cnx.cursor()
#     for k in range(len(df_in)):
#         command1_values = " ( '" + df_in.loc[k, 'Symbol'] + "' , '" + df_in.loc[k, 'Name'] + "' ) "
#         command1 = command1_body + command1_values
#         # print the command
#         print("Executing: ", command1)
#         # execute the created command
#         my_cur1.execute(command1)
#         my_con1.commit()
#     # display information on the status
#     print("Finished populating table: ", table_name)
#     print("Moving on...")
#     # close cursor
#     temp_cursor.close()
#     # close the connection to DB
#     temp_cnx.close()
#
# # sample function parameters
# # connection_dict = {'user': 'wegar', 'password': 'test1234',
# #                    'host': '127.0.0.1', 'database': 'financial_mdb'}
# # table_name = list(dict_of_tickers_dicts.keys())[0]
# # df_in = dict_of_tickers_dicts[table_name]
# # populate_MySQL_table(connection_dict, df_in, table_name)
#
# # 1.5.2.2. walk through the different ticker tables and populate them
#
# connection_dict = {'user': 'wegar', 'password': 'test1234',
#                    'host': '127.0.0.1', 'database': 'financial_mdb'}
#
# # table_name = list(dict_of_tickers_dicts.keys())[0]
# # df_in = dict_of_tickers_dicts[table_name]
#
# for k in range(len(list(dict_of_tickers_dicts.keys()))):
#     # prepare parameters for the function populate_MySQL_table
#     table_name = list(dict_of_tickers_dicts.keys())[k]
#     df_in = dict_of_tickers_dicts[table_name]
#     # execute the command that populates the tables
#     populate_MySQL_table(connection_dict, df_in, table_name)
#
#
# # 1.6. Walk through the elements of dictionaries to load the time series into the MySQL DB
#
# # one of the data dictionaries - choose first one in the row
# i = 0
# iter_dict = list(dict_of_tickers_dicts.keys())[i]
# # prepare a list of all names that are in the dictionary
# symbols_list = list(dict_of_tickers_dicts[iter_dict].loc[:, 'Symbol'])
# # walk through the symbols list and load the data
# for k in range(len(symbols_list)):
#     # I. LOAD DATA INTO PYTHON
#     # extract the symbol name
#     iter_symbol = symbols_list[k]
#     # EXTRACT CURRENT INSTRUMENT NAME
#     # first occurrence of underscore in string
#     first_underscore = iter_dict.find("_")
#     # second occurrence of underscore in string
#     second_underscore = iter_dict[(first_underscore+1):].find("_")
#     iter_instrument_name = iter_dict[(first_underscore+1):(second_underscore+first_underscore+1)]
#     # special treatment of currencies - 'majors' and 'minors'
#     if iter_instrument_name == "currencies":
#         # add 'major'/'minor'
#         residual_of_iter_dict = iter_dict[(second_underscore + first_underscore + 2):]
#         residual_first_underscore = residual_of_iter_dict.find('_')
#         currency_identifier = residual_of_iter_dict[0:residual_first_underscore]
#         iter_instrument_name = iter_instrument_name + currency_identifier
#     else:
#         pass
#     # prepare directory to the data
#     iter_directory_to_data = data_root_dir + "data_world/" + iter_instrument_name + "/" + iter_symbol.lower() + ".txt"
#     df_iter = pd.read_csv(iter_directory_to_data, parse_dates=[0], usecols=['Date', 'Close'])
#     # II. prepare a table for the data in the MySQL DB
