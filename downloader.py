import MySQLdb
import csv

db = MySQLdb.connect(host="",    # your host, usually localhost
                      user="",         # your username
                      passwd="",  # your password
                      db="")

cur = db.cursor()
cur.execute("")

c = csv.writer(open("reco_data.csv","wb"))
for row in cur.fetchall():
    c.writerow(row)


db.close()