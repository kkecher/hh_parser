import sqlite3

DATABASE = 'vacancies.db'
table = 'hh_vacancies'

con = sqlite3.connect(DATABASE)
cur = con.cursor()
create_query = 'CREATE TABLE ' + str(table) + ' (id integer)'
cur.execute(create_query)

insert_query = 'INSERT INTO hh_vacancies (id) VALUES (21714125);'
cur.execute(insert_query)
con.commit ()
cur.close()
con.close()
print ('The SQLite connection is closed')
