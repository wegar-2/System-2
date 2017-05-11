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


################################################################################
######################## UPDATE README to README.md  ###########################
################################################################################


################################################################################
# 2. Loading the data dictionaries to the MySQL Server database "data_dicts_db"#
################################################################################
################################################################################

print("#######################################################################")
print("###########  Loading data dictionaries to the server ...  #############")
print("#######################################################################")


print("#######################################################################")
print("####### Finished loading data dictionaries to the server ...  #########")
print("#######################################################################")


################################################################################
################################################################################
############ 3. Loading the data into databases "XXX_fin_db"  ##################
################################################################################
################################################################################
print("#######################################################################")
print("###########  Loading data dictionaries to the server ...  #############")
print("#######################################################################")


print("#######################################################################")
print("####### Finished loading data dictionaries to the server ...  #########")
print("#######################################################################")


################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################