import psycopg2
import psycopg2.extras 
import csv
import datetime

conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
cur = conn.cursor()


## CREATE TABLES
def create_tables():  
    cur.execute(""" CREATE TABLE IF NOT EXISTS i_type(ufeffID integer, sequence text, pname text, pstyle text, description text, iconurl text, avatar integer, id serial PRIMARY KEY)""")
    cur.execute(""" CREATE TABLE IF NOT EXISTS priority (ufeffID integer, sequence text, pname text, description text, iconurl text, statuscolor text, id serial PRIMARY KEY)""")
    cur.execute(""" CREATE TABLE IF NOT EXISTS i_status (ufeffID integer, sequence integer, pname text, description text, iconurl text, statuscategory text, id serial PRIMARY KEY)""")
    cur.execute(""" CREATE TABLE IF NOT EXISTS jira_i (ufeffID integer, pkey integer,project integer,reporter text, assignee text,issuetype integer, summary text,description text,environment text,priority integer, resolution integer, issuestatus integer, created bigint, updated bigint, duedate bigint, resolutiondate bigint,votes integer, watches integer, timeoriginalestimate integer, timeestimate integer,timespent integer,workflow_id integer,security integer,fixfor integer,component integer, issuenum integer,creator text,archived text, primary_key serial PRIMARY KEY) """)
    cur.execute(""" CREATE TABLE IF NOT EXISTS cgci (id_one integer, issueid integer, author text,created bigint, id_two integer,groupid integer,fieldtype text,field text,oldvalue text,oldstring text,newvalue text,newstring text, primary_id serial PRIMARY KEY)""")
    print('tables done')
    conn.commit()
 

## ISSUETYPE
def insert_issuestype(): 
 with open('./files/issuetype.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        row = [x if x !='NULL' else None for x in row]    
        cur.execute(
        "INSERT INTO i_type VALUES (%s, %s, %s, %s, %s, %s, %s)",
        row    
    )  
    print('it done')
    conn.commit()
    

## PRIORITY
def insert_priority(): 
 with open('./files/priority.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        row = [x if x !='NULL' else None for x in row]   
        cur.execute(
        "INSERT INTO priority VALUES (%s, %s, %s, %s, %s, %s)",
        row    
    ) 
    print('pr done') 
    conn.commit()
 
### ISSUESTATUS
def insert_issuestatus(): 
 with open('./files/issuestatus.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        row = [x if x !='NULL' else None for x in row]   
        cur.execute(
        "INSERT INTO i_type VALUES (%s, %s, %s, %s, %s, %s)",
        row    
    )  
    print('is done')
    conn.commit()

## JIRAISSUE
def insert_jiraissue(): 
 with open('./files/jiraissue.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        row = [x if x !='NULL' else None for x in row]   
        cur.execute(
        "INSERT INTO jira_i VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        row    
    )
    print('ji done') 
    conn.commit()
  

## CHANGEGROUP CHANGEITEM
def insert_changegroup_changeitem(): 
 with open('./files/changegroup_changeitem.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        row = [x if x !='NULL' else None for x in row]         
        cur.execute(
        "INSERT INTO cgci VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        row    
    )  
    print('cgci done')   
    conn.commit() 


## (TEMPORARY TABLES & UNNEEDED FUNCTIONS)

def timestamp_generator(unixcode): 
    timestamp = int(unixcode)
    dt = datetime.datetime.fromtimestamp(int(timestamp)/1000000)  
    return (dt.strftime("%Y-%m-%d %H:%M:%S"))

def tt_changegroupchangeitem():
    cur.execute("SELECT primary_id, created from changegroupchangeitemxxx;")
    data = cur.fetchall()
    results = []
    for i in data:  
        results.append((i[0], str(timestamp_generator(i[1]))))

    for d in results:
        cur.execute("INSERT into temporarytablex(foreign_key, time_stamp) VALUES (%s, %s)", d)  
    conn.commit()
    conn.close()

def update_changegroupchangeitem():
    cur.execute("alter table changegroupchangeitemxxx add created_time_x timestamp")
    cur.execute("""update changegroupchangeitemxxx set created_timestamp = temporarytablex.time_stamp 
    from temporarytablex where temporarytablex.foreign_key = changegroupchangeitemxxx.primary_id;""")
    conn.commit()
    conn.close()

def tt_jiraissue():
    cur.execute("SELECT created, updated from jiraissue;")
    data = cur.fetchall()
    res = []
    for i in data:  
        res.append((timestamp_generator(i[0]),timestamp_generator(i[1])))
    for d in res:
        cur.execute("INSERT into jiraissue_temp(created_time, updated_time ) VALUES (%s, %s)", d)     
    conn.commit()
    conn.close()

def update_jiraissue():
    cur.execute("alter table jiraissuex add created_timestamp timestamp, add updated_timestamp timestamp")
    cur.execute("""update jiraissuex 
                set (created_timestamp, updated_timestamp) = (jiraissue_temp.created_time,jiraissue_temp.updated_time) 
                from jiraissue_temp 
                where jiraissue_temp.foreign_key = jiraissuex.primary_key;""")
    conn.commit()
    conn.close()



### QUERY DB
def querydb(issuenumber,time_stamp):

    ## QUERY ISSUES
    db_query_jiraissues = """select * 
                    from jira_i 
                    where ufeffID = %s and issuetype = 1 and created <= %s;    
                    """
 
    cur.execute(db_query_jiraissues, (issuenumber, time_stamp)) 
    
    res = cur.fetchall()
    if len(res) == 0:
        print('No issues with issuetype = "bug"')
        exit
    for row in res:
        reporter = row[3]
        assignee = row[4]
        summary = row[6]
        description = row[7]
        priority = row[9]
        status = row[11]
        created = row[12]
        closed =row[14]

    ### QUERY LOGS       
    db_query_logs = """select * 
                    from cgci 
                    where issueid = %s and created <= %s
                    order by issueid, created desc
                    """
 
    cur.execute(db_query_logs, (issuenumber,time_stamp))  

    logs = cur.fetchall()
    if len(logs) == 0:
        print('No issues with issuetype = "bug"')
    
    updated = []
    for row in logs: 
        updated.append(row[3])
        
    conn.commit()
    conn.close()
   
    return (created, closed,updated, summary, description, status, priority, assignee, reporter)
    

    

## INPUTFUNCTION
   
def get_input():
    issue_number = int(input("Issue Number: "))
    time = int(input("Time: "))
    res = querydb(issue_number, time)
    created, closed, updated, summary, description, status, priority, assignee, reporter = res
    
    print(f"Created:{created} ")
    print(f"Closed:{closed} ")
    print(f"Updated: {updated}")
    print(f"Summary:{summary} ")
    print(f"{description} ")
    print(f"Status:{status}")
    print(f"Priority: {priority}")
    print(f"Assignee:{assignee}")
    print(f"Reporter:{reporter}")   
    
def source_input():
    create_tables()    
    insert_issuestype() 
    insert_priority() 
    insert_issuestatus() 
    insert_jiraissue()   
    insert_changegroup_changeitem() 
  
  

def init():
    """ source_input() """
    get_input() 

init()
  