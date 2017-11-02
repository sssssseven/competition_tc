
import pymysql as msq

conn = msq.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')

cur = conn.cursor()

cur.execute('SELECT * FROM shop_info WHERE shop_id="s_1877554"')

for r in cur.fetchall():
    print(r)

cur.close()
conn.close()