from abc import ABC, abstractmethod
import numpy as np
import requests
import sqlite3
from ast import literal_eval as make_tuple
from urllib.request import urlopen
from io import StringIO, BytesIO
import csv
import pandas as pd
import math
from website import *
from datetime import date
import os


##############################################################################
# CONSTANTS / GLOBALS
ROW_ORIENTED = 0
DATE_LAST_PARSED= 1
CRAWL_FREQUENCY = 2
UPDATE_TYPE= 3
INDUSTRY_CLASS = 4
SHEET_NUMBER = 5
TABLE_NAME = 6
TABLE_DESCRIPTION = 7
LABELS = 8
CONTENT = 9
URL = 10
RELATIVE = 11
FILE_TYPE = 12
CUT = 13


max_row_id = None # used for caching


##############################################################################
# CLASSES


'''
represents any way to get a location in a file
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
    describe how to return a string if there exists one
    '''
    @abstractmethod
    def get_string(self):
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

    def get_string(self):
        return None
    

'''
represents a location within a file identified by a tag and offset
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
                print("only xlsx and csv files are supported for the FILE-TYPE"
                        " field, please send me an email!")
                exit(1)
        



        # scrape the website and get all of the urls
        base_url = base_url.replace("/*", "")
        website = Website("", base_url, relative)
        crawler = Crawler(website, search, 'Return')
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

csv files dont have sheets, but have been compatible with this function
'''
def get_data(start, end, data, number=0):
    if start < 0 or end < 0:
        return None
    return data[number][start:end]


'''
find the required data that was specified
'''
def find_data(data, string, sheet):
    
    # ranges are seperated by ":"
    split = string.split(":")

    # each entry should atleast have one location on the left
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
    # and no range was specified, just return the row of data in the file
    if len(split) == 1:
        return get_data(start.get_index() - 1, start.get_index(), data, sheet)
    

    # a range was specified, so we grab the end location
    end = get_location_object(split[1], data, sheet)

    # return the data between the start and end locations
    try:
        return get_data(start.get_index() - 1, end.get_index(), data, sheet)
    except Exception as e:
        print("A row number must be specified using a number or 'END'")
        exit(1)


'''
finds and returns the type of location object by parsing 'string'
'''
def get_location_object(string, data, sheet):


    location = None

    # tag and offset
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

    # get the url from the mapping file
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
        if "csv" in url:
           data = parse_csv(url, curs)

        elif "xlsx" in url:
            data = parse_xlsx(url, curs)

        else:
           print("Unsupported data type")
           print("url: {}".format(url))
           exit(1)
    
        
        # get the orientation of the file
        row_type = row_passed[ROW_ORIENTED]

        # column oriented, need to transpose each of the sheets
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
                 row_passed[UPDATE_TYPE], entry_type)




'''
constructs a string from a 2D list
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
    name = get_rowid(url, curs)

    # get the file extension
    file_type = None
    if "xlsx" in url:
        file_type = "xlsx"
    
    elif "csv" in url:
        file_type = "csv"

    if not file_type:
        raise Exception("Unsupported file type!")


    string = "tables/" + str(name) + "." + file_type
    f = open(string, "wb") 
    f.write(res_bytes)
    f.close()



'''
parses an xlsx file and returns a list of lists of rows (also lists)
'''
def parse_xlsx(url, curs):
    res = requests.get(url)
    res_byte = BytesIO(res.content).read()

    write_to_file(res_byte, url, curs)

    values = pd.read_excel(BytesIO(res.content), usecols=None, header=None,
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
    return clean_combined


'''
Parses a csv file and returns a list of lists of rows (also lists)
'''
def parse_csv(url, curs):

    # read in the entire csv file
    data = urlopen(url).read()
    res_bytes = BytesIO(data).read()
    write_to_file(res_bytes, url, curs)


    data_file = StringIO(data.decode('ascii', 'ignore'))
    csvReader = csv.reader(data_file)
    rows = []

    for row in csvReader:
        rows.append(row)

    second = []
    second.append(rows)
    
    return second





'''
update data in our tables metadata table

this function is used for both 'ADD' and 'REPLACE' updates
'''
def update_tables(title, description, update_frequency,
        curs, url, crawl_freq, row_passed):

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
updates the data in the cells table
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
def update_cells_add(table_id, content, labels, curs):

    # get the rowid
    curs.execute("SELECT max(row_id) FROM cells WHERE table_id = {}".format(
        table_id))
    row_id = curs.fetchone()[0]

    # get all of the rows associated with this table
    curs.execute("SELECT * FROM cells WHERE table_id = {} and row_id = {}".format(
        table_id,row_id))


    last_row = curs.fetchall()

    con_row = [x[3] for x in last_row]
    max_index = -1

    
    for i in range(len(content) - 1 , 0, -1):
        if content[i] == con_row:
            max_index = i

    for row in content[max_index + 1:]:
        for index, val in enumerate(row):
            curs.execute("INSERT INTO cells VALUES ( {}, {}, {}, ?)".format(
                table_id,row_id + 1,index),(val))

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
        content, curs, url, crawl_freq,row_passed, update_type, entry_type):

    # insert / update our data
    table_id = update_tables(title, description,update_frequency,
            curs, url, crawl_freq, row_passed)
    labels = update_columns(table_id, labels, curs)
    update_keywords(table_id, keywords, curs)



    if update_type == "REPLACE" or entry_type == "create":
        update_cells_replace(table_id, content, labels, curs)

    elif update_type == "ADD":
        update_cells_add(table_id, content, labels,curs)

    

'''
updates the last-date-crawled field 

I have yet to implement a parse rule dealing with the last date that
an entry was parsed.
'''
def update_parse(filename):
    fin = open(filename)
    data = fin.readlines()
    new_data = []

    for line in data:
        if line[0] == "#":
            new_data.append(line)
            continue

        split = line.split(",")
        if len(split) < 8:
            new_data.append(line)
            continue

        else:       
            split[1] = date.today().strftime("%d/%m/%Y")
        line =  ",".join(split)
        new_data.append(line)
            

    fin.close()

    fout = open(filename, "w")
    for line in new_data:
        fout.write(line)
    fout.close()



'''
gets the rowid of the row containing the specified URL
'''
def get_rowid(url, curs):
    global max_row_id
    curs.execute("SELECT rowid FROM tables WHERE url = \"{}\"".
            format(url))
    number = curs.fetchone()
   
    # this url is already in the database, get the rowid
    if number and len(number) > 0:
        return int(number[0])

    # this url is not in our database, get the next table number and use it
    else:

        # avoid queries if we can
        if max_row_id:
            max_row_id += 1
            return max_row_id

        curs.execute("SELECT max(rowid) FROM tables")
        max_id = curs.fetchone()[0]
        if not max_id:
            max_row_id = 1
            return 1
        else:
            max_id = int(max_id)
            max_row_id = max_id + 1
            return max_id + 1

'''
updates all data in our database with a specific crawl frequency
'''
def update(frequency):
    conn = sqlite3.connect("MASTER.sqlite")
    curs = conn.cursor()
    try:
        curs.execute("SELECT row FROM tables WHERE crawl_frequency = '{}';".format(frequency))
    except:
        print("Please run: python3 create.py before trying to update!")
        exit(1)
    result = curs.fetchall()

    # unpack the tuples
    result = [x[0] for x in result]

    # remove duplicates
    result = list(set(result))

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
    curs.execute("SELECT * FROM tables WHERE url LIKE \"{}%\"".format(url))
    tables = curs.fetchall()

    # delete the files off their local system
    # ONLY WORKS ON UNIX
    for table in tables:
        table = table[0]
        os.remove("tables/" + str(table) + ".*")




    
    if "/*" in url:
        url = url.replace("/*", "").strip()

        curs.execute("DELETE FROM tables WHERE URL LIKE \"{}%\"".format(url))
        return


    curs.execute("DELETE FROM tables WHERE URL = \"{}\"".format(url))
    return



'''
given n rows with m columns, it will return a single row with m columns,
where each column is a - seperated list of the joined column values
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
    # if the first column is empty ... then that is a problem
    for row in rows:
        
        
        # replace the blanks
        for index, val in enumerate(row):

            if type(val) != str:
                row[index] = str(val)

            if index > max_row_length and val != '':
                max_row_length = index + 1

            if val == '' and index > 0:
                row[index] = row[index - 1] 

    max_row_length += 1

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
of row numbers, simply taking the transpose would would
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
            print("\"{}\" ", x)
        exit(1)

    # update type
    if row[3] != "ADD" and row[3] != "REPLACE":
        print("update type must be 'ADD' or 'REPLACE'")
        exit(1)

    
    return


