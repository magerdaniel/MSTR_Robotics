import psycopg2
from mstrio.api import objects
from mstrio.connection import Connection

####### OPEN CONNENCTION TO MSTR ##################
username = "Administrator"
password =""
server="your_id"
port="8080"
project_id="B7CA92F04B9FAE8D941C3E9B7E0CD754" #Turtorial
base_url= "http://" + server + ":" + port + "/MicroStrategyLibrary/api"

conn = Connection(base_url=base_url, project_id=project_id,username=username,password=password)
conn.headers['Content-type'] = "application/json"
conn.select_project(project_id)

##### OPEN CONNENCTION TO MD '''''
connection = psycopg2.connect(
    user="mstr",
    password="",
    host="localhost",
    port="5432",
    database="poc_metadata"
)

# Ein Cursor-Objekt erstellen
cursor = connection.cursor()

######## SQL AGAINST METADATA ############
SQL="""select 
        project_id
        ,object_id
        ,object_type
        from (
        select obj.project_id
                ,obj.object_id
                ,obj.object_type
        from public.dssmdobjinfo obj
        where obj.parent_id ='EE9BD70043F0F571B38E43BF19B9301A'
        and obj.abbreviation =''
        and obj.project_id='B7CA92F04B9FAE8D941C3E9B7E0CD754'
            except
            select
                depn.depn_prjid,
                depn.depn_objid,
                depn.depnobj_type
            from public.dssmdobjinfo obj_1 
            inner join public.dssmdobjdepn depn on
                obj_1.project_id=depn.project_id and
                obj_1.object_id=depn.object_id
            where  obj_1.project_id='B7CA92F04B9FAE8D941C3E9B7E0CD754'
         )O
"""

cursor.execute(SQL)
results = cursor.fetchall()

for row in results:
    try:
        out = objects.delete_object(connection=conn, id=row[1]
                                      , object_type=row[2])
        print("Object with id "+str(row[1]) +" of type "+str(row[2])+" has been deleted")
    except Exception as err :
        print(err)
    cursor.execute(SQL)
    results = cursor.fetchall()

cursor.close()
connection.close()
