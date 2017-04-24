################################################################################
################################################################################
####################       STOOQ_DATA_PREPROCESSING         ####################
################################################################################
import os

print("##########################################")
print("##########################################")
print("Executing Stooq_data_preprocessing.py ....")
print("##########################################")
print("##########################################")
print("os.getcwd() = ", os.getcwd())
print("Changing directory to the data directory...")
os.chdir('/home/wegar/github_repos/financial_data')
print("os.getcwd() = ", os.getcwd())
print("##########################################")
print("List of contents of the current directory:")
print("##########################################")
list_of_contents = os.listdir(os.getcwd())
# display the content in loop
for iter, el in enumerate(list_of_contents):
    print(str(iter), ": ", el)

#####################################
# 1. Unzip the file "d_world_txt.zip"
#####################################
print("#########################")
print("#########################")
print("#########################")
print("Unzipping d_world_txt.zip")
print("#########################")
print("#########################")
print("#########################")
os.system('unzip d_world_txt.zip')
print("###################################")
print("###################################")
print("###################################")
print("Unzipping d_world_txt.zip finished!")
print("###################################")
print("###################################")
print("###################################")
# display contents of the current directory for reference...
print("##########################################")
print("List of contents of the current directory:")
print("##########################################")
list_of_contents = os.listdir(os.getcwd())
# display the content in loop
for iter, el in enumerate(list_of_contents):
    print(str(iter), ": ", el)

#####################################################
# 2. Move the important data to a normal directory...
#####################################################
os.system('cp -R /home/wegar/github_repos/financial_data/data/daily/world '
          '/home/wegar/github_repos/financial_data/')
# remove the useless data set...
os.system('rm -rf /home/wegar/github_repos/financial_data/data')

############################################################
# 3. Rename the directories in the new directory with data..
############################################################
os.chdir('/home/wegar/github_repos/financial_data/world')

##################################
# 4. Download data dictionaries...
##################################