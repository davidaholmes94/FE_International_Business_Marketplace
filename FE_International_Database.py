# -*- coding: utf-8 -*-
"""
MySQL database test script
Created on Fri Nov 15 17:54:54 2019
@author: David
"""
#
#REMEMBER TO PRINT OUT THE VERSIONS OF EVERYTHING YOU'RE USING
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from datetime import date
import logging
import os
import glob
import json
#this says it goes unused but it's used in line 44
import mysql.connector
#%% Set up Logger
file_dir = 'C:/Users/David/Documents/Python/Github/FE_International_Scraper/Data/'
logging.basicConfig(filename=file_dir+'database.log',level=logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#%% Get Data
#Read In CSV file
#Typically, you want to select the most recent file.

list_of_files = glob.glob(file_dir + '*.csv')
latest_file = max(list_of_files, key=os.path.getctime)

#slightly modify database types
fe_listings = pd.read_csv(latest_file, index_col=0)
fe_listings.listing_date = fe_listings.listing_date.astype('str')
fe_listings.listing_id = fe_listings.listing_id.astype('int')

#%%Database Information
#Grab Credentials
with open('credentials.json') as json_data:
    credentials = json.load(json_data)

user = credentials['user']
password = credentials['password']
host = credentials['host']
port = credentials['port']
database = credentials['database']

#Connect to SQL Database
engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.format(user,password,host,port,database), echo = False)
db = pymysql.connect(host,user,password,database)
cursor = db.cursor()
 
#%% Create Table
sql_creation = """CREATE TABLE IF NOT EXISTS fe_creation_python (
    ID int AUTO_INCREMENT,
    listing_id int NOT NULL,
    title text,
    revenue text,
    profit text,
    asking_price text,
    status text,
    listing_date date,
    url text,
    UNIQUE(listing_id),
    PRIMARY KEY(ID)); """

cursor.execute(sql_creation)
#%% ADD NEW LISTINGS
#1. Add entire database into temporary dataframe
#2. Use INSERT IGNORE to only add new rows into the production database
#2a. This uses the unique listing_id to check for new rows
#described further here: https://stackoverflow.com/questions/30631848/insert-ignore-pandas-dataframe-into-mysql
fe_listings.to_sql('fe_creation_temp', con = engine, index=False, if_exists = 'replace')

sql_add_rows = """INSERT IGNORE INTO fe_creation_python (listing_id,title,revenue,profit,asking_price,status,listing_date,url) 
                  SELECT * FROM fe_creation_temp"""
rows_added = cursor.execute(sql_add_rows)

db.commit()

#print(str(rows_added) + " new entries were added on " + str(date.today()))
logging.info("\n***" + str(rows_added) + " new entries were added. ***\n")

#%% UPDATE STATUS OF ALL LISTINGS
#1. Fetch SQL production database
#2. Compare to new database using concatenation and drop duplicates
#2a. Right now this only checks for change in listing status (active, offer pending, sold)
#3. Go row by row and update the SQL production database

#read in database
sql_get_data = """SELECT * FROM fe_creation_python"""
cursor.execute(sql_get_data)
data = cursor.fetchall()

fe_database = pd.DataFrame(data)
#drop the index - it's duplicated
fe_database.drop(0,axis=1,inplace=True)
#column names don't come through in the SQL so duplicate from scraped dataframe
fe_database.columns = list(fe_listings)


#keep only the rows that differ
#have to clean the data beause some NA's pop up for status - investigate this further
df1 = pd.concat([fe_listings,fe_database]).drop_duplicates(subset=['listing_id','status'],keep=False)
changes = df1.groupby('listing_id').status.apply(list).apply(pd.Series).add_prefix('Status_')
#have to clean data - some NAs pop up for status - this is because the listing can't be found anymore
changes.fillna('Status Not Found', inplace = True)



logging.info("\n" + str(len(changes)) + " listing status was changed on " + str(date.today()) + "***\n")


#iterate through dataframe and make changes
if len(changes) > 0:
    for i, row in changes.iterrows():
        sql_change_data = "UPDATE fe_creation_python SET status = \'{0}\' WHERE listing_id = {1}".format(row['Status_1'],int(i))
        cursor.execute(sql_change_data)
        logging.info("Listing id: " + str(i) + "status was changed from " + row['Status_0'] + " to " + row['Status_1'] + "***\n")

db.commit()
logging.info("\n***Database changes were made successfully***\n")  
db.close()
logging.info("\n***Database connection closed***\n")  


                      



