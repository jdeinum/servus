'''
The purpose of this script is to update the data in our database

This script should be run every day at midnight to ensure you continue
updating the data in the database.

Data will still be updated even if the entry in the mapping file is deleted.
If you want to stop updating data, update the parse frequency to 'never' in 
the database for that entry


'''

from datetime import date
from system import update

DB_FILE = "../MASTER.sqlite"



def main():
    todays_date = date.today().strftime("%d/%m/%Y")
    day_of_the_week = date.today().weekday()
    values = todays_date.split('/')
    
    if day_of_the_week == 0:
        update("weekly")

    if int(values[0]) == 1:
        update("monthly")

    if int(values[0]) == 1 and int(values[1]) % 4 == 0:
        update("quarterly")

    if int(values[0]) == 1 and int(values[1]) == 1:
        update("yearly")

    update("daily")

main()
