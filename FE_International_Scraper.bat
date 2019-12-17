echo Starting Python Program Now.
call activate WebScraper
python "C:\Users\David\Documents\Python\FE_International_Scraper\Code\FE-InternationalScraper.py"
echo Scrape Completed Successfully
echo Importing Data to Database now
python "C:\Users\David\Documents\Python\FE_International_Scraper\Code\FE_International_Database.py"
echo Database Imported Successfully
pause