################################################################################
################################################################################
################################################################################
##################################   MAIN.PY  ##################################
################################################################################
################################################################################



################################################################################
#############################    I. IMPORTS    #################################
################################################################################
import os
import mysql.connector
import mysql.connector.errorcode as err_cd
import time



################################################################################
############################   II. PREREQUISITES    ############################
################################################################################
# 0. Prepare the data that will be used to build the database
# Download the "world" data in ASCII format and unpack them into a directory
# from the website: https://stooq.pl/db/h/

# 1. It is assumed that MySQL DB, version 5.5 is installed on the user system
# If it is not, install it

# 2. Set the System2_app_dir below;
# The structure of the directory is expected to be the following:
#    ==>   System2_app_dir
#          ==>  System-2 (the downloaded repo...)
#               ==> ...
#               ==> main.py
#               ==> ...
#          ==>  financial_data
#               ==> d_world_txt.zip
#               ==> d_us_txt.zip
#               ==> d_jp_txt.zip
#               ==> d_hk_txt.zip
#               ==> d_de_txt.zip
#               ==> d_macro_txt.zip
#
# Link to the data historical DBs on Stooq.pl: https://stooq.pl/db/h/
#

# 3. Execute the script "prepare_MySQL_Server" using the command:
# ...$ ./prepare_MySQL_Server




################################################################################
#########################   III. SCRIPTS EXECUTION    ##########################
################################################################################
if __name__ == "__main__":
    print("######################################################")
    print("######################################################")
    print("########### Executing main.py script... ##############")
    print("######################################################")
    print("######################################################")
    print("######################################################")
    print("\n")
    print("__file__ = ", __file__)
    print("\n")
    print("####################################################################")
    print("####################################################################")
    print("##### Current directory: ", os.getcwd())
    print("####################################################################")
    print("####################################################################")
    # 0. Preparatory changes - creation of databases in the MySQL Server
    print("Creating databases in the MySQL Server - type in root user password:")
    os.system('./prepare_MySQL_Server')
    print("Check connection in...")
    for secs_left in range(3,0,-1):
        print(secs_left, " second(s)...")
        time.sleep(1)
    try:
        test_cnx = mysql.connector.connect(user="fin_db_client",
                                           password="test1234",
                                           host="127.0.0.1",
                                           database="world_fin_db"
                                           )
    except mysql.connector.Error as err:
        if err.errno == err_cd.ER_ACCESS_DENIED_ERROR:
            print("Connection test: failed")
            print("Access denied error.")
            print("Something is wrong with user name and/or password. ")
        elif err.errno == err_cd.ER_BAD_DB_ERROR:
            print("Connection test: failed")
            print("Database error when tryinh to connect. ")
            print("The database you are trying to connect to may not exist. ")
        else:
            print("Connection test: failed")
            print("Neither user/password nor access error; ")
            print("Error message: ")
            print(err)
    else:
        # the code below is executed if the "try" block does
        # not raise an exception
        test_cnx.close()
        print("Connection test: success")

    # 1. Preprocessing of the downloaded data
    os.system('python3 Stooq_data_preprocessing.py')
    print("####################################################################")
    print("####################################################################")
    print("Back to main.py script...")
    print("Quick check of current directory - os.getcwd(): ", os.getcwd())
    print("Moving on to data load into MySQL server...")
    print("####################################################################")
    print("####################################################################")
    # 2. Data load into MySQL Server
    # os.system('python3 load_data_to_MySQL.py')