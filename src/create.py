from system import *
'''
driver function
'''
def main():

    # if the tables directory does not exist, create one
    try:
        os.mkdir("./tables")
    except FileExistsError:
        pass



    # try opening the file
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

        elif line[0] == "D" and line[1] == "@":
            delete_entry(line[2:], curs)
            continue

        values = line.strip().replace("\"", "").split(',')
        if len(values) > 0 and len(values) < 9:
            if values[0] == '' and len(values) == 1:
                continue
            print("Error on line {} please check formatting!".format(index + 1))
            continue

        validate_input(values)
        process_entry(values, curs, "create")

        

    conn.commit()
    curs.close()
    conn.close()
    f.close()

    update_parse("mapping.csv")


main()

