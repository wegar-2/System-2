################################################################################
################################################################################
################       CREATE TABLES AT MYSQL SERVER         ###################
################################################################################

# imports
import pandas as pd
import mysql.connector
import mysql.connector.errorcode as err_cd
import os
import time
import sys
import progress.bar

print("##################################################################")
print("### Script create_tables_at_MySQL_Server starts running in ... ###")
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
    # print("Creating table ", table_name, " in the database ", database_name,"...")
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


# 1.2. wrapper function to create data tables
def create_data_tables_in_mysql_server(region_name, category_name,
                                       database_name_in, connection_dict_in):
    # a) display info
    print("\n\n")
    # print("Inside create_data_tables_in_mysql_server() function...")

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
    print("Creating tables for: ")
    print("\t\tRegion: ", region_name)
    print("\t\tCategory: ", category_name)
    # d) create tables
    bar1 = progress.bar.Bar("Progress:  ", max=len(temp_symbols_list))
    for iter_symbol in temp_symbols_list:
        create_table_mysql_server(iter_symbol, database_name_in,
                                  table_columns_dict_in, connection_dict_in)
        bar1.next()
    bar1.finish()



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

# list of regions in the database
regions_list = \
    [
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

# loop through regions
for iter_key_region in dict_regions_to_categories.keys():
    print("Region: ", str(iter_key_region))
    # loop through different categories assigned to a region
    for iter_value_category in dict_regions_to_categories[iter_key_region]:
        print("\tCategory: ", str(iter_value_category))
        table_name_in = iter_value_category
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

print("#######################################################################")
print("##################  Creating tables for data ... ######################")
print("#######################################################################")

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

# loop through keys of the dict_regions_to_categories
for iter_key_region in dict_regions_to_categories.keys():
    print("Region: ", str(iter_key_region))
    database_name_in = str(iter_key_region) + "_fin_db"
    connection_dict_in['database'] = database_name_in
    # for each key loop through the list to which its pointing
    for iter_value_category in dict_regions_to_categories[iter_key_region]:
        print("\tCategory: ", str(iter_value_category))
        # run the data tables creation in the MySQL Server
        create_data_tables_in_mysql_server(iter_key_region, iter_value_category,
                                           database_name_in, connection_dict_in)

print("#######################################################################")
print("##############  Finished Creating tables for data ... #################")
print("#######################################################################")