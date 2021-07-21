'''
this program is designed to be run whenever a new line is added to the 
mapping file

the program will add any new entries to the existing database

DO NOT use this program to update the database, for any entry, if the update
type is 'ADD' then data will be lost.
'''




from system import *



'''
driver function
'''
def main():

    # if the tables directory does not exist, create one
    # we store all files we scrape on the users local machine incase they
    # want the full file
    try:
        os.mkdir("./tables")
    except FileExistsError:
        pass



    # try opening the mapping file
    try:
        f = open("mapping.csv", "r")


        # open a connection to the DB
        conn = sqlite3.connect("MASTER.sqlite")
        curs = conn.cursor()
        curs.execute("PRAGMA foreign_keys=on")
        curs.execute("BEGIN TRANSACTION")
    
    # Exception can only occur if the mapping file does not exist
    except Exception as e:
        print("Error opening the file: {}".format(e))
        exit(1)
    
    else:

        # create the tables
        curs.execute("CREATE TABLE IF NOT EXISTS tables (rowid INTEGER PRIMARY KEY ON CONFLICT REPLACE,title TEXT, description TEXT, update_frequency TEXT, crawl_frequency TEXT, url TEXT UNIQUE ON CONFLICT REPLACE, row TEXT);")
        curs.execute("CREATE TABLE IF NOT EXISTS columns (table_id INTEGER, column_id INTEGER, header TEXT ,FOREIGN KEY(table_id) references tables(rowid) ON DELETE CASCADE, PRIMARY KEY (table_id, column_id) ON CONFLICT REPLACE);")
        curs.execute("CREATE TABLE IF NOT EXISTS cells (table_id INTEGER, row_id INTEGER, column_id INTEGER, value TEXT, FOREIGN KEY(table_id) REFERENCES tables(rowid) ON DELETE CASCADE);")
        curs.execute("CREATE TABLE IF NOT EXISTS keywords (table_id INTEGER, keyword TEXT, FOREIGN KEY(table_id) REFERENCES tables(rowid) ON DELETE CASCADE, PRIMARY KEY (table_id,keyword) ON CONFLICT REPLACE);")


        
    
    for index,line in enumerate(f):

        # comments start with #
        if line[0] == '#':
            continue
        
        # D@URL will delete any entry associated with that URL from the database
        elif line[0] == "D" and line[1] == "@":
            delete_entry(line[2:], curs)
            continue

        # clean up the values a bit, incase the user provides dirty data
        values = line.strip().replace("\"", "").split(',')


        # we require atleast 9 fields in order for the entry to be a valid one
        # with the last field being the URL
        if len(values) > 0 and len(values) < 11:

            # skip lines that start spaces
            if values[0] == '' and len(values) == 1:
                continue
            print("Error on line {} please check formatting!".format(index + 1))
            continue
        
        
        # validate the data and process the entry
        validate_input(values)
        process_entry(values, curs, "create")

        
    # housekeeping
    conn.commit()
    curs.close()
    conn.close()
    f.close()


    # updates the 'LAST-PARSED' field in the mapping file for every entry
    # not currently used for anything in the code
    update_parse("mapping.csv")


main()

