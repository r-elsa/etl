
import csv
import sqlite3

def create_table():
    conn = sqlite3.connect('etldb.db')
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS issuetype(ufeffID INTEGER, sequence INTEGER, pname TEXT, pstyle TEXT, description TEXT, iconurl TEXT, avatar INTEGER)')
    c.execute('CREATE TABLE IF NOT EXISTS priority (ufeffID INTEGER, sequence INTEGER, pname TEXT, description TEXT, iconurl TEXT, statuscolor TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS issuestatus (ufeffID INTEGER, sequence INTEGER, pname TEXT, description TEXT, iconurl TEXT, statuscategory TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS jiraissue (ufeffID INTEGER, pkey INTEGER,project INTEGER,reporter TEXT, assignee TEXT,issuetype INTEGER, summary TEXT,description TEXT,environment TEXT,priority INTEGER, resolution INTEGER, issuestatus INTEGER, created INTEGER, updated INTEGER,duedate INTEGER, resolutiondate INTEGER,votes INTEGER, watches INTEGER, timeoriginalestimate INTEGER, timeestimate INTEGER,timespent INTEGER,workflow_id INTEGER,security INTEGER,fixfor INTEGER,component INTEGER, issuenum INTEGER,creator TEXT,archived TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS changegroupchangeitem (id_one INTEGER,issueid INTEGER, author TEXT,created INTEGER,id_two INTEGER,groupid INTEGER,fieldtype TEXT,field TEXT,oldvalue TEXT,oldstring TEXT,newvalue TEXT,newstring TEXT)')
    print('TABLEs CREATED')

               

class IssueType:
    def __init__(self,ufeffID,sequence,pname,pstyle,description,iconurl,avatar):
        self.ufeffID = ufeffID
        self.sequence = sequence
        self.pname = pname
        self.pstyle = pstyle
        self.description = description
        self.iconurl = iconurl
        self.avatar = avatar

    
    def issuetype_data_entry(self):
        conn = sqlite3.connect('etldb.db')
        c = conn.cursor()
        c.execute("INSERT INTO issuetype (ufeffID, sequence, pname, pstyle, description, iconurl, avatar) VALUES (?, ?, ?, ?, ?, ?, ?)", (self.ufeffID,self.sequence,self.pname,self.pstyle,self.description,self.iconurl,self.avatar))   
        conn.commit()
        c.close()
        conn.close()


class Priority:
    def __init__(self,ufeffID,sequence,pname,description,iconurl,statuscolor):
        self.ufeffID = ufeffID
        self.sequence = sequence
        self.pname = pname
        self.description = description
        self.iconurl = iconurl
        self.statuscolor = statuscolor

    
    def priority_data_entry(self):
        conn = sqlite3.connect('etldb.db')
        c = conn.cursor()
        c.execute("INSERT INTO priority (ufeffID, sequence, pname, description, iconurl, statuscolor) VALUES ( ?, ?, ?, ?, ?, ?)", (self.ufeffID,self.sequence,self.pname,self.description,self.iconurl,self.statuscolor))   
        conn.commit()
        c.close()
        conn.close()
    

class IssueStatus:
    def __init__(self,ufeffID,sequence,pname,description,iconurl,statuscategory):
        self.ufeffID = ufeffID
        self.sequence = sequence
        self.pname = pname
        self.description = description
        self.iconurl = iconurl
        self.statuscategory = statuscategory

    
    def issuestatus_data_entry(self):
        conn = sqlite3.connect('etldb.db')
        c = conn.cursor()
        c.execute("INSERT INTO issuestatus (ufeffID, sequence, pname, description, iconurl, statuscategory) VALUES ( ?, ?, ?, ?, ?, ?)", (self.ufeffID,self.sequence,self.pname,self.description,self.iconurl,self.statuscategory))   
        conn.commit()
        c.close()
        conn.close()


class JiraIssue:
    def __init__(self,ufeffID,pkey,project,reporter, assignee,issuetype, summary,description,environment,priority, resolution, issuestatus, created, updated,duedate, resolutiondate,votes, watches, timeoriginalestimate, timeestimate,timespent,workflow_id,security,fixfor,component, issuenum,creator,archived):       
        self.ufeffID = ufeffID
        self.pkey = pkey
        self.project = project
        self.reporter = reporter
        self.assignee = assignee
        self.issuetype = issuetype
        self.summary = summary
        self.description = description
        self.environment = environment
        self.priority = priority
        self.resolution = resolution
        self.issuestatus = issuestatus
        self.created = created
        self.updated = updated
        self.duedate = duedate
        self.resolutiondate = resolutiondate
        self.votes = votes
        self.watches = watches
        self.timeoriginalestimate = timeoriginalestimate
        self.timeestimate = timeestimate
        self.timespent = timespent
        self.workflow_id = workflow_id
        self.security = security
        self.fixfor = fixfor
        self.component = component
        self.issuenum = issuenum
        self.creator = creator
        self.archived = archived

   
    def jiraissue_data_entry(self):
        conn = sqlite3.connect('etldb.db')
        c = conn.cursor()
        c.execute("INSERT INTO jiraissue (ufeffID, pkey, project, reporter, assignee, issuetype, summary, description,environment,priority, resolution, issuestatus, created, updated,duedate, resolutiondate,votes, watches, timeoriginalestimate, timeestimate,timespent,workflow_id,security,fixfor,component, issuenum,creator,archived) VALUES ( ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (self.ufeffID, self.pkey, self.project, self.reporter, self.assignee, self.issuetype, self.summary,self.description,self.environment,self.priority, self.resolution, self.issuestatus, self.created, self.updated,self.duedate, self.resolutiondate,self.votes, self.watches, self.timeoriginalestimate, self.timeestimate,self.timespent,self.workflow_id,self.security,self.fixfor,self.component, self.issuenum,self.creator,self.archived))   
        conn.commit()
        c.close()
        conn.close()


class ChangeGroupChangeItem:
    def __init__(self,id_one,issueid, author,created,id_two,groupid,fieldtype,field,oldvalue,oldstring,newvalue,newstring ):
        self.id_one = id_one
        self.issueid = issueid
        self.author = author
        self.created = created 
        self.id_two = id_two
        self.groupid = groupid
        self.fieldtype = fieldtype
        self.field = field
        self.oldvalue = oldvalue
        self.oldstring = oldstring
        self.newvalue = newvalue
        self.newstring = newstring

    
    def changegroup_changeitem_data_entry(self):
        conn = sqlite3.connect('etldb.db')
        c = conn.cursor()
        c.execute("INSERT INTO changegroupchangeitem (id_one,issueid, author,created,id_two,groupid,fieldtype,field,oldvalue,oldstring,newvalue,newstring) VALUES ( ?, ?, ?, ?, ?, ?,?,?,?,?,?,?)", (self.id_one,self.issueid, self.author,self.created,self.id_two,self.groupid,self.fieldtype,self.field,self.oldvalue,self.oldstring,self.newvalue,self.newstring))   
        conn.commit()
        c.close()
        conn.close()




 
def get_input():

    issue_number = input("Issue Number: ")
    time = input("Time: ")
    issue_number_data = issue_number_query(issue_number)

    print(f"Created:{issue_number} ")
    print(f"Closed:{issue_number} ")
    print(f"Updated: {issue_number}")
    print(f"Summary:{issue_number} ")
    print(f"Description:{issue_number} ")
    print(f"Status:{time}")
    print(f"Priority: {time}")
    print(f"Assignee:{time}")
    print(f"Summary:{time}")
    print(f"Reporter:{time}")

def issue_number_query(conn,issue_number):
    cur = conn.cursor()
    cur.execute("SELECT * FROM changegroupchangeitem WHERE issue_number=?", (issue_number,))
    rows = cur.fetchall()

    for row in rows:
        print(row) 


if __name__ == '__main__':
    create_table()
    """  get_input() """
    with open('./files/issuetype.csv', 'r') as issuetype:
        read_issuetype = csv.reader(issuetype, delimiter=',')
        next(read_issuetype)
        for row in read_issuetype:
            row = [x if x !='NULL' else None for x in row]
            ufeffID =  int(row[0]) if row[0]!= None else None
            sequence = int(row[1]) if row[1]!= None else None
            pname = row[2]if row[2]!= None else None
            pstyle = row[3]if row[3]!= None else None
            description = row[4]if row[4]!= None else None
            iconurl = row[5]if row[5]!= None else None
            avatar=  int(row[6]) if row[6]!= None else None
            current = IssueType(ufeffID,sequence,pname,pstyle,description,iconurl,avatar) 
            current.issuetype_data_entry()
    
    with open('./files/priority.csv') as priority:
        read_priority = csv.reader(priority, delimiter=',')
        next(read_priority)
        for row in read_priority:
            row = [x if x !='NULL' else None for x in row]
            ufeffID =  int(row[0]) if row[0]!= None else None
            sequence = int(row[1]) if row[1]!= None else None
            pname = row[2]if row[2]!= None else None
            description = row[3]if row[3]!= None else None
            iconurl = row[4]if row[4]!= None else None
            statuscolor=  row[5]if row[5]!= None else None
            current = Priority(ufeffID,sequence,pname,description,iconurl,statuscolor) 
            current.priority_data_entry()
                
    with open('./files/issuestatus.csv') as issuestatus:
        read_issuestatus = csv.reader(issuestatus, delimiter=',')
        next(read_issuestatus)
        for row in read_issuestatus:
            row = [x if x !='NULL' else None for x in row]
            ufeffID =  int(row[0]) if row[0]!= None else None
            sequence = int(row[1]) if row[1]!= None else None
            pname = row[2]if row[2]!= None else None
            description = row[3]if row[3]!= None else None
            iconurl = row[4] if row[4]!= None else None
            statuscategory= row[5] if row[5]!= None else None
            current = IssueStatus(ufeffID,sequence,pname,description,iconurl,statuscategory) 
            current.issuestatus_data_entry()
        


    with open('./files/jiraissue.csv') as jiraissue:
        read_jiraissue = csv.reader(jiraissue, delimiter=',')
        next(read_jiraissue)
        for row in read_jiraissue:
            issuetype = int(row[5]) if row[5]!= None else None
            if issuetype == 1:
                print('yes')
                row = [x if x !='NULL' else None for x in row]
                ufeffID =  int(row[0]) if row[0]!= None else None
                pkey = int(row[1]) if row[1]!= None else None
                project = int(row[2]) if row[2]!= None else None
                reporter =row[3]if row[3]!= None else None
                assignee =row[4]if row[4]!= None else None
                issuetype = int(row[5]) if row[5]!= None else None
                summary =row[6] if row[6]!= None else None
                description =row[7]if row[7]!= None else None
                environment =row[8]if row[8]!= None else None
                priority = int(row[9]) if row[9]!= None else None
                resolution = int(row[10]) if row[10]!= None else None
                issuestatus = int(row[11]) if row[11]!= None else None
                created = int(row[12]) if row[12]!= None else None
                updated = int(row[13]) if row[13]!= None else None
                duedate = int(row[14]) if row[14]!= None else None
                resolutiondate = int(row[15]) if row[15]!= None else None
                votes = int(row[16]) if row[16]!= None else None
                watches = int(row[17]) if row[17]!= None else None
                timeoriginalestimate = int(row[18]) if row[18]!= None else None
                timeestimate = int(row[19]) if row[19]!= None else None
                timespent = int(row[20]) if row[20]!= None else None
                workflow_id = int(row[21]) if row[21]!= None else None
                security = int(row[22]) if row[22]!= None else None
                fixfor = int(row[23]) if row[23]!= None else None
                component = int(row[24]) if row[24]!= None else None
                issuenum = int(row[25]) if row[25]!= None else None
                creator =row[26]if row[26]!= None else None
                archived =row[27]if row[27]!= None else None
                current = JiraIssue(ufeffID,pkey,project,reporter, assignee,issuetype, summary,description,environment,priority, resolution, issuestatus, created, updated,duedate, resolutiondate,votes, watches, timeoriginalestimate, timeestimate,timespent,workflow_id,security,fixfor,component, issuenum,creator,archived) 
                current.jiraissue_data_entry()

            else:
                pass 

    with open('./files/changegroup_changeitem.csv') as changegroup_changeitem:
            read_changegroup_changeitem = csv.reader(changegroup_changeitem, delimiter=',')
            next(read_changegroup_changeitem)
            for row in read_changegroup_changeitem:
                row = [x if x !='NULL' else None for x in row]
                id_one = int(row[0]) if row[0]!= None else None
                issueid = int(row[1]) if row[1]!= None else None
                author =row[2]if row[2]!= None else None
                created = int(row[3]) if row[3]!= None else None
                id_two= int(row[4]) if row[4]!= None else None
                groupid = int(row[5]) if row[5]!= None else None
                fieldtype = row[6]if row[6]!= None else None
                field = row[7]if row[7]!= None else None
                oldvalue = row[8] if row[8]!= None else None
                oldstring = row[9]if row[9]!= None else None
                newvalue = row[10]if row[10]!= None else None
                newstring = row[11] if row[11]!= None else None
                current = ChangeGroupChangeItem(id_one,issueid, author,created,id_two,groupid,fieldtype,field,oldvalue,oldstring,newvalue,newstring) 
                current.changegroup_changeitem_data_entry() 


     