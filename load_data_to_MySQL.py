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
    # print("Inside function create_table_mysql_server...")
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
    # print("Executing command: ")
    # print(temp_command)
    temp_cursor.execute(temp_command)
    # 5.close cursor and terminate connection
    temp_cursor.close()
    temp_cnx.close()
    # print("Leaving function create_table_mysql_server()...")
    # print("\n")


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
# 2.4.1. wse_stocks
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
table_name_in = "stooq_stocks_indices"
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
print("os.getcwd(): ", os.getcwd())
print("\n\n")
print("Contents of the current directory: ")
for iter_count, iter_value in enumerate(os.listdir(os.getcwd())):
    print(str(iter_count+1), ": ", iter_value)
print("\n\n")

# moving into directory with data dictionaries
os.chdir(os.getcwd()+"/Stooq_data_dictionaries")

# os.chdir("/home/wegar/github_repos/financial_data")
# print(os.getcwd())

# new set of columns for tables with the times series
table_columns_dict_in = {
    "Date": "date",
    "Close": "float(14,4)"
}

# wrapper function that creates tables


def create_data_tables_in_mysql_server(region_name, category_name,
                                       database_name_in, connection_dict_in):
    # a) display info
    print("\n\n")
    print("Inside create_data_tables_in_mysql_server() function...")
    print("\t\tRegion: ", region_name)
    print("\t\tCategory: ", category_name)
    # b) getting list of tables names
    temp_path = os.getcwd() + "/" + region_name + "/" + category_name + ".csv"
    df_tables_names = pd.read_csv(temp_path)
    temp_symbols_list = [str(el) for el in
                         pd.Series(df_tables_names.loc[:, 'Symbol'])]
    # changing "." ===> "_" in the list of symbols
    temp_symbols_list = [el.replace(".", "_") for el in temp_symbols_list]
    # changing "^" ===> "hat_" in the list of symbols
    temp_symbols_list = [el.replace("^", "hat_") for el in temp_symbols_list]
    # changing "-" ===> "_dash_" in the list of symbols
    temp_symbols_list = [el.replace("-", "hat_") for el in temp_symbols_list]
    # c) display list of symbols\
    print("\n\n")
    print("List of symbols for region: ", region_name,
          ", category: ", category_name)
    for iter_count, iter_symbol in enumerate(temp_symbols_list):
        print(str(iter_count+1), ": ", iter_symbol)
    # d) create tables
    for iter_symbol in temp_symbols_list:
        create_table_mysql_server(iter_symbol, database_name_in,
                                  table_columns_dict_in, connection_dict_in)

# 3.1. 'de'
# 3.1.1. xetra_stocks
region_name = "de"
database_name_in = "de_fin_db"
print("Creating tables for region: ", region_name, "; 1/6")
print("Running in...")
for secs_left in range(5,0,-1):
    print(str(secs_left), " second(s)...")
    time.sleep(1)
category_name = "xetra_stocks"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.2. 'hk'
# 3.2.1. hkex_stocks
region_name = "hk"
database_name_in = "hk_fin_db"
print("Creating tables for region: ", region_name, "; 2/6")
print("Running in...")
for secs_left in range(5,0,-1):
    print(str(secs_left), " second(s)...")
    time.sleep(1)
category_name = "hkex_stocks"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.3. 'jp'
# 3.3.1. tse_futures
region_name = "jp"
database_name_in = "jp_fin_db"
print("Creating tables for region: ", region_name, "; 3/6")
print("Running in...")
for secs_left in range(5,0,-1):
    print(str(secs_left), " second(s)...")
    time.sleep(1)
category_name = "tse_futures"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.3.2. tse_indices
category_name = "tse_indices"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.3.3. tse_stocks
category_name = "tse_stocks"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.4. 'pl'
# 3.4.1. wse_stocks
region_name = "pl"
database_name_in = "pl_fin_db"
print("Creating tables for region: ", region_name, "; 4/6")
print("Running in...")
for secs_left in range(5,0,-1):
    print(str(secs_left), " second(s)...")
category_name = "wse_stocks"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.5. 'us'
# 3.5.1. nasdaq_stocks
region_name = "us"
database_name_in = "us_fin_db"
print("Creating tables for region: ", region_name, "; 5/6")
print("Running in...")
for secs_left in range(5,0,-1):
    print(str(secs_left), " second(s)...")
    time.sleep(1)
category_name = "nasdaq_stocks"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.5.2. nyse_stocks
category_name = "nyse_stocks"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.5.3. nysemkt_stocks
category_name = "nysemkt_stocks"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.6.  'world'
# 3.6.1. bonds
region_name = "world"
database_name_in = "world_fin_db"
print("Creating tables for region: ", region_name, "; 6/6")
print("Running in...")
for secs_left in range(5,0,-1):
    print(str(secs_left), " second(s)...")
    time.sleep(1)
category_name = "bonds"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.6.2. commodities
category_name = "commodities"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.6.3. currencies_major
category_name = "currencies_major"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.6.4. currencies_minor
category_name = "currencies_minor"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.6.5. indices
category_name = "indices"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.6.6. lme
category_name = "lme"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.6.7. money_market
category_name = "money_market"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)
# 3.6.8. stooq_stock_indices
category_name = "stooq_stocks_indices"
print("\t\tCategory: ", category_name)
print("\t\tRunning in...")
for secs_left in range(3,0,-1):
    print("\t\t\t", str(secs_left), " second(s)...")
    time.sleep(1)
create_data_tables_in_mysql_server(region_name, category_name,
                                   database_name_in, connection_dict_in)



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