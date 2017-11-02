import pymysql as db

if __name__ == '__main__':
    conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()
    sql = 'SELECT DISTINCT mall_id FROM shop_info'
    cur.execute(sql)

    for r in cur.fetchall():
        sql = '''CREATE TABLE `{s}` 
(`user_id` varchar(255) DEFAULT NULL,  
`shop_id` varchar(255) DEFAULT NULL,  
`time_stamp` datetime DEFAULT NULL,
`longitude` varchar(255) DEFAULT NULL,
`latitude` varchar(255) DEFAULT NULL,
`wifi_ssid` varchar(255) DEFAULT NULL,
`wifi_db` int(255) DEFAULT NULL,
`wifi_conn` varchar(255) DEFAULT NULL,
ENGINE=InnoDB DEFAULT CHARSET=utf8;'''.format(s = r[0])
        cur.execute(sql)
        sql = '''INSERT INTO {s} 
SELECT * FROM behavior_final WHERE shop_id IN (SELECT shop_id FROM shop_info WHERE mall_id='{s}')'''.format(s = r[0])
        cur.execute(sql)
