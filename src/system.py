''' 
this file contains most if not all of the program logic


python is not my primary language, and i am not too familiar with its 
tooling, i've left formal documentation in hope that my inline documentation 
is sufficient


'''
##############################################################################
# IMPORTS
from abc import abstractmethod
import numpy as np
import requests
import sqlite3
from urllib.request import urlopen
from io import StringIO, BytesIO
import csv
import pandas as pd
import math
from website import *
from datetime import date
import os
import zipfile

##############################################################################
# CONSTANTS / GLOBALS
ROW_ORIENTED = 0
DATE_LAST_PARSED= 1
CRAWL_FREQUENCY = 2
UPDATE_TYPE= 3
INDUSTRY_CLASS = 4
ZIPPED = 5
FILE_TYPE = 6
SHEET_NUMBER = 7
TABLE_NAME = 8
TABLE_DESCRIPTION = 9
LABELS = 10
CONTENT = 11
URL = 12
RELATIVE = 13
CUT = 14


max_row_id = None # used for caching


##############################################################################
# CLASSES


'''
represents any way to get a location in a file

we use 2 locations (start and end) for each field (title, desc, content, etc)
to mark the start and end of that field in the data
'''
class Location():
    
    data = None

    def __init__(self, data):
        self.data = data
    
    def get_data(self):
        return self.data

    '''
    describe how to get the index within the data file 
    '''
    @abstractmethod
    def get_index(self):
        pass

'''
represents a location within a file identified by a row number
'''
class RowNumber(Location):
    number = None
    sheet = 0

    def __init__(self, number, data):
        super().__init__(data)
        self.number = number

    def get_index(self):
        return self.number


    

'''
represents a location within a file identified by a tag and offset

i.e finding a row by matching a string in the first column, and then you
can seek relative to that location

ex) "NAME"+-1 would find the row where the first cell contains only NAME and 
go up one row
'''
class TagAndOffset(Location):
    tag = None
    offset = None
    sheet = 0

    def __init__(self, tag, offset, data):
        super().__init__(data)
        self.tag = tag
        self.offset = offset


    def get_index(self):
        for sheet in self.get_data():
            for index, row in enumerate(sheet):
                if len(row) > 0 and row[0]== self.tag:
                    return index + self.offset + 1



        return -1
    
##############################################################################
# FUNCTIONS

'''
Returns a list of URLs to parse information from

Unless a wildcard is used, there will only be one url to scan per entry

If a wildcard is used, it finds each valid URL to scrape from
'''
def get_urls_list(base_url, row):
    base_url = row[URL]
    url_list = []

    # there is wildcard in the address, we must scrape the website for files
    if "/*" in base_url:
        
        # ensure the user specified a web address type
        try:
            relative = row[RELATIVE]
            if relative != "R" and relative != "A":
                raise Exception


        except:
            print("RELATIVE must be 'A' or 'R',please read the description "
                    "of this field")
            print(base_url)
            exit(1)
    
        # ensure the user provided a file type to scrape for
        try:
            search = row[FILE_TYPE]
            if search != "csv" and search != "xlsx":
                raise Exception
       
        except:
                print("only xlsx and csv files are supported for the FILE-TYPE")
                exit(1)
        



        # replace the asterix with nothing
        base_url = base_url.replace("/*", "")

        # create our website parser
        website = Website("", base_url, relative)
        crawler = Crawler(website, search, 'Return')

        # tell the crawler we want to scrape all sites in the base url
        # appended with '' (empty)
        crawler.get('')
        url_list = crawler.get_urls()
        
        # adjust the urls if the website uses relative addresses
        if relative == "R":
            url_list = [base_url + x for x in url_list]
    
    # does not use wildcards, therefore there is a single url in this field
    else:
        url_list.append(base_url)

    return url_list



'''
data will be a 3D list, a list of sheets, where each sheet contains a 
list of rows

csv files dont have sheets, but have been made compatible with this function

currently it can be a 2D list, but I left it this way incase servus wants to 
be able to grab fields from multiple sheets i.e title from sheet 1, description
from sheet 2, etc ... 
'''
def get_data(start, end, data, number=0):

    # index is -1 in the case where some location in a file cannot be found
    # returning None will signal the program to insert Null
    if start < 0 or end < 0:
        return None
    return data[number][start:end]


'''
find the required data that was specified in the mapping file
'''
def find_data(data, string, sheet):
    
    # ranges are seperated by ":"
    split = string.split(":")

    # each entry should atleast have one location on the left, i.e start
    start = get_location_object(split[0], data, sheet)

    # if start is a string, the user entered a string in this field
    # we must return it as a 2D list to ensure it remains compatible 
    # with the rest of the program.
    if type(start) == str:
        first = []
        second = []
        second.append(start)
        first.append(second)
        return first


    # not a string
    # and no range was specified, which means the user provided a single row
    # just return the row of data in the file
    if len(split) == 1:
        return get_data(start.get_index() - 1, start.get_index(), data, sheet)
    

    # a range was specified, so we grab the end location
    end = get_location_object(split[1], data, sheet)

    # return the data between the start and end locations
    try:
        return get_data(start.get_index() - 1, end.get_index(), data, sheet)

    except Exception:
        print("A row number must be specified using a number or 'END'")
        exit(1)
    


'''
finds and returns the type of location object by parsing 'string'

the program will take a look at the string passed and return either:

a) a row number object
b) a tag and offset object
'''
def get_location_object(string, data, sheet):


    location = None

    # if there a + in the string, it is a range
    if "+" in string:
        split= string.split("+")
        tag = split[0]
        offset = int(split[1])

        location = TagAndOffset(tag, offset, data)

    # "END" means to the end of the sheet
    elif string == "END":
        location = RowNumber(len(data[sheet]), data)

    
    else:
        
        # is it a number or a string?
        # if it throws an exception it is a string, just return it
        # otherwise make a location object with the row number and return it
        try:
            number = int(string)
        except:
            return string
        else:
            location = RowNumber(number, data)


    if location:
        return location

    else:
        print("Error parsing for location object!")
        exit(1)



'''
Processes a single line of our mapping file
'''
def process_entry(row_passed, curs, entry_type):

    # we need the URL to get the data from the web
    try:
        base_url = row_passed[URL]
    except:
        print("No URL was specified, or the mapping file was not formatted" 
                " properly")
        exit(1)

    # get the urls to iterate over
    url_list = get_urls_list(base_url, row_passed)


    for url in url_list:
        

        # data is a list of rows (which are also lists)
        data = None

        # unzip the data if needed, and change the filename
        zipped = row_passed[ZIPPED].split(":")
        if zipped[0] == "yes":
            filename = zipped[1]
            byte_data = unzip_and_extract(url, filename)

            if "csv" in filename:
                data = parse_csv(url, curs, byte_data)

            elif "xlsx" in filename:
                data = parse_xlsx(url, curs, byte_data)
        
        # file is a csv
        elif "csv" in url:
           data = parse_csv(url, curs)
        
        # file is an xlsx
        elif "xlsx" in url:
            data = parse_xlsx(url, curs)

        else:
           print("Unsupported data type")
           print("url: {}".format(url))
           exit(1)
    
        
        # get the orientation of the file
        # column oriented, need to transpose each of the sheets
        row_type = row_passed[ROW_ORIENTED]
        if row_type == "C":
            for index, lst in enumerate(data):
                data[index] = columm_to_row(lst)


        # get the sheet number from the mapping file
        # we subtract 1 because excel and similar applications number sheets
        # from 1, but python begins at index 0
        try:
            sheet_number = int(row_passed[SHEET_NUMBER]) - 1
        except:
            sheet_number = 0
        

        # get all of the data we want our DB entry to have
        title = construct_string(find_data(data, row_passed[TABLE_NAME], 
            sheet_number))
        description = construct_string(find_data(data,
            row_passed[TABLE_DESCRIPTION], sheet_number))
        frequency = row_passed[CRAWL_FREQUENCY]
        keywords = row_passed[INDUSTRY_CLASS].split(":")
        labels = label_concat(find_data(data, row_passed[LABELS], sheet_number))
        content = find_data(data, row_passed[CONTENT], sheet_number)
        crawl_frequency = row_passed[CRAWL_FREQUENCY]

        # enter data into our DB
        enter_data(title, description, frequency, keywords, labels, content,
                 curs, url,crawl_frequency,",".join(row_passed), 
                 row_passed[UPDATE_TYPE])




'''
constructs a string from a 2D list

we use this for our title and description so each of these fields can 
span multiple rows
'''
def construct_string(data):

    string = ""
    for first in data:
        for second in first:
            string += second + " "

    return string



'''
write the table to file to allow a user to look at the table without 
needing to find it again

res_bytes is the byte representation of the file

we require the url to know what to name the table
'''
def write_to_file(res_bytes, url, curs):
    
    # we use the rowid in the tables table as the name for our file
    name = get_rowid(url, curs)

    # get the file extension
    file_type = None
    if "xlsx" in url:
        file_type = "xlsx"
    
    elif "csv" in url:
        file_type = "csv"

    if not file_type:
        raise Exception("Unsupported file type!")

    # write to file
    string = "tables/" + str(name) + "." + file_type
    f = open(string, "wb") 
    f.write(res_bytes)
    f.close()



'''
parses an xlsx file and returns a list of lists of rows (also lists)
'''
def parse_xlsx(url, curs, bytes_data=None):
    
    # bytes data gets passed if the file was zipped
    # in that case we can do not need to request it again
    if not bytes_data:

        # request and get the data
        res = requests.get(url)
        res_byte = BytesIO(res.content).read()

    # we passed the bytes in because the file was zipped
    # use these instead
    else:
        
        res_byte = bytes_data



    # write the data to file
    write_to_file(res_byte, url, curs)
    
    # read the excel file
    values = pd.read_excel(res_byte, usecols=None, header=None,
        sheet_name=None)
    clean_combined = []

    for key in values.keys():
        clean = []
        val = values[key].values[:]

        # replace all of the NaN in this file with ''
        # it is important that we replace them and not remove them
        # if we remove them, we will likely get errors when entering data into
        # the database since it may remove an empty field (order is important)
        for x in val:
            new_row = []
            for y in x:
                if type(y) == float and math.isnan(y) is True:
                    y = ''
                new_row.append(y)
            clean.append(new_row)
        
        clean_combined.append(clean)

    # as with CSVs , we return a list containing a list of rows
    return clean_combined


'''
Parses a csv file and returns a list of lists of rows (also lists)
'''
def parse_csv(url, curs, bytes_data=None):
    
    # bytes data is passed if the file was zipped
    if not bytes_data: 

        # read in the entire csv file
        data = urlopen(url).read()
        res_bytes = BytesIO(data).read()

    else:

        res_bytes = bytes_data
        data = res_bytes

    # write the bytes to file
    write_to_file(res_bytes, url, curs,)

    # decode the bytes so csv can interpret them
    data_file = StringIO(data.decode('ascii', 'ignore'))
    csvReader = csv.reader(data_file)
    rows = []
    
    # we return a list containing a list of rows, for future support
    # of choosing specific sheets for each field in xlsx files
    for row in csvReader:
        rows.append(row)

    second = []
    second.append(rows)
    
    return second





'''
update data in our tables metadata table

this function is used for both 'ADD' and 'REPLACE' update types
'''
def update_tables(title, description, update_frequency,
        curs, url, crawl_freq, row_passed):

    row_passed_split = row_passed.split(",")
    row_passed_split[DATE_LAST_PARSED] = date.today().strftime("%d/%m/%Y")
    row_passed = ",".join(row_passed_split)


    # first enter the metadata details into our metadata tables
    curs.execute("INSERT INTO tables VALUES (NULL,\"{}\", \"{}\", \"{}\", \"{}\", \"{}\",\"{}\") ON CONFLICT DO UPDATE SET \
            title=excluded.title,description=excluded.description,update_frequency=excluded.update_frequency, \
            crawl_frequency=excluded.crawl_frequency,row=excluded.row".
    format(title,
                description,
                update_frequency,
                crawl_freq,
                url,
                row_passed))
    

    # get the table id of the table if it already exists
    curs.execute("SELECT rowid FROM tables WHERE URL = \"{}\";".
            format(url))
    curs.execute("SELECT rowid from tables WHERE URL = \"{}\";"
            .format(url))
    table_id = curs.fetchone()[0]
    return table_id



'''
updates the data in the cells table that use the replace update type

we use REPLACE when rows are appended, and the old data remains in the file. In
a sense we erase all of the existing data in the database, and then add all of 
the new data. The reason we replace all of the data instead of just inserting
new rows is incase they make changes to the existing data. If the data will not
changed (i.e rows that arent appended) then use 'ADD' instead, it will be much 
faster
'''
def update_cells_replace(table_id, content, labels, curs):


    # first delete all of the existing cells
    curs.execute("DELETE FROM cells WHERE table_id = {}".format(table_id))


    # enter the cell data into the cells table
    # start by iterating over each row of the data
    row_id = 0

    for row in content:


        # skip rows if they are missing values
        # we are not skipping any rows which contain data, since these are
        # guaranteed to have the same number of elements as the labels
        if len(row) < len(labels):
            continue

        # iterate over each value in the row
        for col_id,cell in enumerate(row):
            
            # don't enter any empty elements
            if cell == '':
                continue
            
            curs.execute("INSERT INTO cells VALUES ({}, {}, {}, ?) ON CONFLICT DO UPDATE SET \
                    row_id=excluded.row_id, col_id=excluded.col_id,cell=excluded.cell;"
                    .format(table_id, row_id, col_id), (cell,))
        row_id += 1


'''
Updates the cells for tables which replace old data with new data

We do not want to delete all of the previous data, and instead want to add 
all of the new rows to the existing data

'''
def update_cells_add(table_id, content, curs):

    # get the rowid
    curs.execute("SELECT max(row_id) FROM cells WHERE table_id = {}".format(
        table_id))
    row_id = curs.fetchone()[0]

    # rowid will be None if the table is empty
    # we dont want the row to match at a
    if not row_id:
        max_index = -1
        row_id = 0

    else:

        # get all of the rows associated with this table
        curs.execute("SELECT * FROM cells WHERE table_id = {} and row_id = {}".format(
            table_id,row_id))


        # reconstruct the last row for matching purposes
        last_row = curs.fetchall()
        con_row = [x[3] for x in last_row]



        # the index of which specifies the start of the new rows
        # since we add 1, in theory if we dont get a match, we add the whole
        # table
        max_index = -1

        
        # find the index of the last matched row in the data
        for i in range(len(content) - 1 , 0, -1):
            if content[i] == con_row:
                print("The matching row was row number {}".format(i))
                max_index = i

    
    # add any new rows to the data
    for row in content[max_index + 1:]:
        for index, val in enumerate(row):
            curs.execute("INSERT INTO cells VALUES ( {}, {}, {}, ?)".format(
                table_id,row_id + 1,index),(val,))


        row_id += 1
            
    




'''
updates the columns table
'''
def update_columns(table_id, labels, curs):

    # first delete any entries from the column table for the current table
    curs.execute("DELETE FROM columns WHERE table_id = {}".
        format(table_id))

    # find the last empty non empty label, and delete everything after it
    max_index = 0
    for index, x in enumerate(labels):
        if x != '':
            max_index = index
    labels = labels[:max_index + 1]

    
    # handle empty labels, cannot have duplicate columns
    unknown = 0
    for index, x in enumerate(labels):
        if x == '':
            labels[index] = "unknown_column" + str(unknown)
            unknown += 1


    # get rid of newlines our labels
    labels = [x.replace("\n", "") for x in labels]


    # enter the column data into the columns table
    column_id = 0
    for label in labels:

        curs.execute("INSERT INTO columns VALUES ({},{},?) ON CONFLICT DO UPDATE SET \
               column_id=excluded.column_id,header=excluded.header".
               format(table_id, column_id),(label,))
        column_id += 1

    return labels
    

'''
update the keywords table
'''
def update_keywords(table_id, keywords, curs):

    for keyword in keywords:
        curs.execute("INSERT INTO keywords VALUES ({}, \"{}\") ON CONFLICT DO NOTHING;".
            format(table_id, keyword))





'''
enters the acquired data into our SQLite DB
'''
def enter_data(title, description, update_frequency, keywords, labels, 
        content, curs, url, crawl_freq,row_passed, update_type):

    # insert / update our data
    table_id = update_tables(title, description,update_frequency,
            curs, url, crawl_freq, row_passed)
    labels = update_columns(table_id, labels, curs)
    update_keywords(table_id, keywords, curs)


    # ensure we use the correct update type
    if update_type == "REPLACE":
        update_cells_replace(table_id, content, labels, curs)

    elif update_type == "ADD":
        update_cells_add(table_id, content, curs)

    

'''
updates the last-date-crawled field, currently this doesn't do anything other
than let the people looking at the mapping file know when the last time they 
crawled it
'''
def update_parse(filename):

    # read in the current file
    fin = open(filename)
    data = fin.readlines()



    new_data = []
    
    for line in data:

        # comments can just be added
        if line[0] == "#":
            new_data.append(line)
            continue
    
        
        split = line.split(",")

        # lines that don't contain entries can just be added to the new data
        if len(split) < 11:
            new_data.append(line)
            continue
        
        # replace the date with todays date for any entry
        else:       
            split[1] = date.today().strftime("%d/%m/%Y")
        line =  ",".join(split)
        new_data.append(line)
            

    fin.close()
    
    # reopen the file in write mode
    # and write the new data
    fout = open(filename, "w")
    for line in new_data:
        fout.write(line)
    fout.close()



'''
gets the rowid of the row containing the specified URL

this is used when writing the file to storage, since we use the rowid as 
the file name when saving it


NOTE: This function will fail if the number of entries exceeds 2^32 - 1 , since
sqlite will randomly try different numbers until it finds one it can use.
'''
def get_rowid(url, curs):
    global max_row_id


    # if this query succeeds, then the url already has an entry
    # we use that for the filename
    curs.execute("SELECT rowid FROM tables WHERE url = \"{}\"".
            format(url))
    number = curs.fetchone()
   
    # this url is already in the database, get the rowid
    if number and len(number) > 0:
        return int(number[0])

    # this url is not in our database, get the next table number and use it
    else:

        # avoid queries if we can
        # max_row_id is the locally stored value of the next rowid to be used
        if max_row_id:
            max_row_id += 1
            return max_row_id

        curs.execute("SELECT max(rowid) FROM tables")
        max_id = curs.fetchone()[0]

        # max_id will be None if it is a new table
        # sqlite defaults rowid to 1 in this case
        if not max_id:
            max_row_id = 1
            return 1


        # use the next row number as the id
        # and save the rowid to use for later
        else:
            max_id = int(max_id)
            max_row_id = max_id + 1
            return max_id + 1

'''
updates all data in our database with a specific crawl frequency

this function is called from update.py , we could probably move this function
to that file and just import system.py
'''
def update(frequency):

    # open a new connection to the database
    conn = sqlite3.connect("MASTER.sqlite")
    curs = conn.cursor()

    # fails if the user tries to update the data before the tables are even
    # created, in that case, they need to run create.py before running update
    try:
        curs.execute("SELECT row FROM tables WHERE crawl_frequency = '{}';".format(frequency))
    except:
        print("Please run: python3 create.py before trying to update!")
        exit(1)
    result = curs.fetchall()

    # unpack the tuples
    result = [x[0] for x in result]

    # remove duplicates, this is more of a failsafe 
    result = list(set(result))
    
    # for each entry in the database that matches the update frequency, 
    # update it
    for row in result:
        values = row.split(",")
        process_entry(values, curs, "update")


    conn.commit()
    curs.close()
    conn.close()



'''
deletes all entries asscoiated with a URL


WARNING: This will delete a file with the same name as the table with any
file extension
'''
def delete_entry(url, curs):

    # get the row numbers of the url we are removing
    # so we can delete the files from the tables/ directory
    curs.execute("SELECT rowid FROM tables WHERE url LIKE \"{}%\"".format(url))
    tables = curs.fetchall()

    # delete the files off their local system
    # ONLY WORKS ON UNIX
    for table in tables:
        table = table[0]
        os.remove("tables/" + str(table) + ".*")




    # wildcard in delete statement, delete any entries matching the wildcard 
    if "/*" in url:
        url = url.replace("/*", "").strip()

        curs.execute("DELETE FROM tables WHERE URL LIKE \"{}%\"".format(url))
        return


    curs.execute("DELETE FROM tables WHERE URL = \"{}\"".format(url))
    return



'''
given n rows with m columns, it will return a single row with m columns,
where each column is a - seperated list of the joined column values


i.e 

1 2 3 BLANK 5 6 7
a b c d     e f g

will return:
1a 2b 3c 3d 5e 6f 7g
'''
def label_concat(rows):

    cleaned_rows = []

    # empty row, panic!
    if len(rows) == 0:
        print("We got an empty concat!")
        exit(1)
    
    max_row_length = 1


    # fill in any blanks, start by using the same row
    # if column 2 is empty, we use the value in column 1, 4 we use 3 ...
    # if the first column is empty ... then we stick our head in the sand
    for row in rows:
        
        
        # replace the blanks
        for index, val in enumerate(row):
            
            # convert any dates or numbers to strings to allow concatenation
            if type(val) != str:
                row[index] = str(val)
            
            # we also want to find the last column with a non empty value,
            # since we will truncate each row to this length to avoid having
            # the database cluttered with 
            if index > max_row_length and val != '':
                max_row_length = index + 1


            # value is emptry, replace it with the previous value
            if val == '' and index > 0:
                row[index] = row[index - 1] 

    max_row_length += 1
        
    # truncate the data 
    for row in rows:
        row = row[0:max_row_length]
        cleaned_rows.append(row)

    

    # now we join all the columns together in one list (1 list per column)
    # should't have too large of a performance penalty due to caching
    out_list = [[] for i in range(max_row_length)]
    for row in cleaned_rows:
        for index,val in enumerate(row):
            out_list[index].append(val)

    # join all of the values in each list with a dash ( - )
    for i in range(len(out_list)):
        out_list[i] = "-".join(out_list[i])
    
    return out_list


'''
converts column oriented data into row data

as long as the user specifies all of the locations using column numbers instead
of row numbers, simply taking the transpose should work.

Not tested, didnt find any pure column oriented data
'''
def columm_to_row(columns):
    rows = np.transpose(columns) 
    return rows



'''
validates some of the input from the mapping file
'''
def validate_input(row):
    
    # row vs column
    if row[0] != "R" and row[0] != "C":
        print("ROW-ORIENTED MUST BE 'R' OR 'C'")
        exit(1)

    # update frequency
    update_freqs = ["daily", "weekly", "monthly", "quarterly", "yearly",
            "never"]
    if row[2] not in update_freqs:
        print("update type must be:")
        for x in update_freqs:
            print("\"{}\" ".format(x))
        exit(1)

    # update type
    if row[3] != "ADD" and row[3] != "REPLACE":
        print("update type must be 'ADD' or 'REPLACE'")
        exit(1)

    
    return


'''
unzips and extracts a file into the given directory
'''
def unzip_and_extract(url, filename):
        
    # request and get the data
    res = requests.get(url)
    res_byte = BytesIO(res.content).read()

    # convert into a zipfile
    filebytes = BytesIO(res_byte)
    myzipfile = zipfile.ZipFile(filebytes)

    # extract the file
    extracted_file = myzipfile.open(filename)

    # read the data as bytes and return it
    data = extracted_file.read()

    return data

    




