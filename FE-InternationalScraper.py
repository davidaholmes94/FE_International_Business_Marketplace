# -*- coding: utf-8 -*-
"""
FE International Website Valuation Scraping
Created on Fri Nov  1 11:50:32 2019
@author: David
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
import re
import logging
#%% Set up Logger
file_dir = 'C:/Users/David/Documents/Python/FE_International_Scraper/FE_Data/'
logging.basicConfig(filename=file_dir + 'log.log',level=logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#%% Scrape Website
#FE International
webpage_response = requests.get("https://feinternational.com/buy-a-website/")
if webpage_response.status_code != 200:
    print("FE International website is unavailable. Returning status code " + str(webpage_response.status_code))
    logging.ERROR("\nFE International website is unavailable. Returning status code " + str(webpage_response.status_code) + "\n")
else:
    logging.info("\nFE International Website available. Starting scraping now. \n")
main_page = webpage_response.content
FEsoup = BeautifulSoup(main_page,"html.parser")
#%% Comprehend Data
listing_description = FEsoup.find_all("div", class_="listing-description")
details = FEsoup.find_all("article", {"data-date": True})

#Grab Listing Dates
listing_dates = []
for j in range(len(details)):
    listing_dates.append(details[j]['data-date'])
    

#Grab Rest of Information
titles=[];
revenues=[];
profits=[];
asking_prices=[];
listing_status=[];
urls=[];
listing_ids=[];
#Navigate through HTML and pull out interesting info
for i in range(len(listing_description)):
    #Attribute Error gets thrown if the object can't be found. In this case
    #replace with NaN value.
    try:
        title = listing_description[i].find("h2",class_="listing-title").string
        url = listing_description[i].find("a",href=True)['href']
    except AttributeError:
        title = float('Nan')
    try:
        revenue = listing_description[i].find("dd", class_="listing-overview-item listing-overview-item--yearly-revenue").string
    except AttributeError:
        revenue = float('Nan')
    try:
        profit = listing_description[i].find("dd", class_="listing-overview-item listing-overview-item--yearly-profit").string
    except AttributeError:
        profit = float('Nan')
    try:
        #asking price can have two parts - the price and a span class that says Under Offer or Sold
        # to get just the asking price use contents[0]. To get whether it's solr or not use contents[1].string
        asking_price = listing_description[i].find("dd", class_="listing-overview-item listing-overview-item--asking-price").contents[0]
    except AttributeError:
        asking_price = float('Nan')
    #Mark the ones that are sold
    #<span class="asking-price-sold">SOLD</span> </dd>
    try:
        status = listing_description[i].find("span", class_="asking-price-sold").string
    except AttributeError:
        status = 'Active'
    listing_description[i].parent.find("article")
    #build lists - they should all be the same size now
    titles.append(title)
    revenues.append(revenue)
    profits.append(profit)
    asking_prices.append(asking_price)
    listing_status.append(status)
    urls.append(url)


for i in range(len(urls)):
    #regex magic the \d+ means it looks for digits in the range of 0-9 and stops
    #when it comes to something that isn't a digit.
    #https://stackoverflow.com/questions/7548692/how-to-grab-number-after-word-in-python
    match = re.search('https://feinternational.com/buy-a-website/(\d+)', urls[i])
    listing_id = match.group(1)
    listing_ids.append(listing_id)
    

FE = {'listing_id':listing_ids,'title':titles,'revenue':revenues,
      'profit':profits,'asking_price':asking_prices,
      'status':listing_status, 'listing_date':listing_dates,'url':urls}

FE_listings = pd.DataFrame(FE)
today = date.today()
logging.info("\nDataframe successfully created. \n")

FE_listings.to_csv(file_dir + 'FE_International_Listings-' + str(today) + '.csv')
logging.info("\nNew file saved in folder: FE_International_Data\n")

