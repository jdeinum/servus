'''
The purpose of this script is to update the data in our database

This script will be run every day at midnight.

Each entry in MASTER.sqlite has an associated update frequency,
daily = 1
weekly = 2
monthly = 3
quarterly = 4
twice a year = 5
yearly = 6 

Currently, for simplicity, I just choose times to update these values. i.e daily at midnight, weekly on monday at
midnight, etc..  - This can be adjusted for data that is required to be up to date immediately
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
