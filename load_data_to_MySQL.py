###########################################################################################
###########################################################################################
########################       LOAD DATA TO MYSQL DATABASE         ########################
###########################################################################################

### imports ###
import glob
import pandas as pd
import mysql.connector
import mysql.connector.errorcode as err_cd
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


################################################################################
################################################################################
##### 1. load functions that will be used to export data to MySQL Server #######
################################################################################
################################################################################


# 1.1. function to create tables in the MySQL Server
def create_table_mysql_server(table_name, database_name,
                              table_columns_dict, connection_dictionary):
    print("Inside function create_table_mysql_server...")
    print("Creating table ", table_name, " in the database ", database_name,"...")
    # 1. open connection with database
    temp_cnx = mysql.connector.connect(**connection_dictionary)
    # 2. create cursor
    temp_cursor = temp_cnx.cursor()
    # 3. prepare command
    temp_command = "CREATE TABLE IF NOT EXISTS " + database_name + "." + \
                   table_name + " (" + "\n"
    # loop through table_columns_dict and add columns to the command
    for iter_counter, iter_key in enumerate(table_columns_dict.keys()):
        string_to_append = "\t\t" + str(iter_key) + " " + \
                           str(table_columns_dict[iter_key])
        # add the comma at the end of the string_to_append
        if iter_counter != (len(table_columns_dict)-1):
            string_to_append = string_to_append + ","
        # in any case, add newline
        string_to_append = string_to_append + "\n"
        temp_command = temp_command + string_to_append
    temp_command = temp_command + ");"
    # 4. execute the command
    print("Executing command: ")
    print(temp_command)
    temp_cursor.execute(temp_command)
    # 5.close cursor and terminate connection
    temp_cursor.close()
    temp_cnx.close()
    print("Leaving function create_table_mysql_server()...")
    print("\n")


# 1.2. function to load DataFrames to the indicated table at MySQL server



################################################################################
################################################################################
##### 2. Creating tables for data dictionaries to database: "data_dicts_db" ####
################################################################################
################################################################################
# 2.0. prepare arguments for the create_table_mysql_server() function
connection_dict_in = {
    'user': 'fin_db_client',
    'password': 'test1234',
    'host': '127.0.0.1',
    'database': 'data_dicts_db'
}
database_name_in = "data_dicts_db"
table_columns_dict_in = {
    "Symbol": "varchar(40)",
    "Name": "varchar(40)"
}
print("\n")
print("#######################################################################")
print("###########  Creating tables for data dictionaries... #################")
print("#######################################################################")
print("\n")
# 2.1. 'de'
# 2.1.1. xetra_stocks
table_name_in = "xetra_stocks"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.2. 'hk'
# 2.2.1. hkex_stocks
table_name_in = "hkex_stocks"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.3. 'jp'
# 2.3.1. tse_futures
table_name_in = "tse_futures"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.3.2. tse_indices
table_name_in = "tse_indices"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.3.3. tse_stocks
table_name_in = "tse_stocks"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.4. 'pl'
# 2.4.1. wse_stoks
table_name_in = "wse_stocks"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.5. 'us'
# 2.5.1. nasdaq_stocks
table_name_in = "nasdaq_stocks"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.5.2. nyse_stocks
table_name_in = "nyse_stocks"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.5.3. nysemkt_stocks
table_name_in = "nysemkt_stocks"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.6.  'world'
# 2.6.1. bonds
table_name_in = "bonds"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.6.2. commodities
table_name_in = "commodities"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.6.3. currencies_major
table_name_in = "currencies_major"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.6.4. currencies_minor
table_name_in = "currencies_minor"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.6.5. indices
table_name_in = "indices"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.6.6. lme
table_name_in = "lme"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.6.7. money_market
table_name_in = "money_market"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)
# 2.6.8. stooq_stock_indices
table_name_in = "stooq_stock_indices"
create_table_mysql_server(table_name_in, database_name_in,
                          table_columns_dict_in, connection_dict_in)

print("\n")
print("#######################################################################")
print("######## Finished creating tables for data dictionaries... ############")
print("#######################################################################")
print("\n")

################################################################################
################################################################################
##################### 3. Creating tables for the data itself ###################
################################################################################
################################################################################
# 3.1. 'de'
# 3.2. 'hk'
# 3.3. 'jp'
# 3.4. 'pl'
# 3.5. 'us'
# 3.6.  'world'



################################################################################
################################################################################
# 4. Loading the data dictionaries to the MySQL Server database "data_dicts_db"#
################################################################################
################################################################################
# 4.1. 'de'
# 4.2. 'hk'
# 4.3. 'jp'
# 4.4. 'pl'
# 4.5. 'us'
# 4.6.  'world'



################################################################################
################################################################################
############ 5. Loading the data into databases "XXX_fin_db"  ##################
################################################################################
################################################################################
# 5.1. 'de'
# 5.2. 'hk'
# 5.3. 'jp'
# 5.4. 'pl'
# 5.5. 'us'
# 5.6.  'world'



################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
### two main functions:
#
#
# # connection_dictionary = {'user': 'fin_db_client', 'password': 'test1234',
# #                    'host': '127.0.0.1', 'database': 'hk_fin_db'}
#
# # 1) create_table_in_MySQL_Server
# def create_tables_in_mysql_server(list_of_tables_names, connection_dictionary):
#     # 1. establish the connection
#     cnx_to_server = mysql.connector.connect(connection_dictionary)
#     # 2. create tables in a loop:
#     for iter_table_name in list_of_tables_names:
#         # create a temp cursor
#         temp_cursor = cnx_to_server.cursor()
#         temp_query = (
#             "CREATE TABLE " + str(iter_table_name) + " ("
#             "   Symbol      varchar(50)     NOT NULL,     "
#             "   Name        varchar(50)     NOT NULL     "
#             ") ENGINE=InnoDB")
#         print("Executing query: ", temp_query)
#         temp_cursor.execute(temp_query)
#         # close the temp_cursor
#         temp_cursor.close()
#     # 3. close the cursor and terminate the connection
#     cnx_to_server.close()
#
# # test the function create_table_in_mysql_server()
# connection_dictionary = {
#     'user': 'fin_db_client',
#     'password': 'test1234',
#     'host': '127.0.0.1',
#     'database': 'hk_fin_db'
# }
#
#
#
# create_tables_in_mysql_server(list_of_tables_names, connection_dictionary)
#
# # 2) populate_table_in_MySQL_Server
# def populate_tables_in_mysql_server(dict_of_tables, connection_dictionary):
#
#     # 1. establish the connection
#
#     # 2.
#
#
#
# ### 1. Loading data dictionaries to the database
#
#
# ### 2. Loading the data to the database
#
#
#
#
# # 1.5. Load the dictionaries into the MySQL DB
#
# # 1.5.1. loop through the dictionary of tickers dictionaries and create
# # tables for them in the DB
#
# # 1.5.1.1. prepare commands
# dict_of_queries = {}
# for my_iter in dict_of_tickers_dicts.keys():
#     # save the query in dictionary
# # 1.5.1.2. execute commands
# for my_iter in dict_of_tickers_dicts.keys():
#     temp_query = dict_of_queries[my_iter]
#     cnx1 = mysql.connector.connect(user="wegar", password="test1234",
#                               host="127.0.0.1",
#                               database="financial_mdb")
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
