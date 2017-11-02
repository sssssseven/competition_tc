import pymysql as db
import threading


def run(index,mall_id):
    print('start 1 thread')
    conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()
    sql = '''CREATE TABLE `{s}` (
  `data_id` int(11) DEFAULT NULL,
  `user_id` varchar(255) DEFAULT NULL,
  `shop_id` varchar(255) DEFAULT NULL,
  `time_stamp` datetime DEFAULT NULL,
  `longitude` varchar(255) DEFAULT NULL,
  `latitude` varchar(255) DEFAULT NULL,
  `wifi_ssid` varchar(255) DEFAULT NULL,
  `wifi_db` int(255) DEFAULT NULL,
  `wifi_conn` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''.format(s=mall_id)
    cur.execute(sql)
    print('table ', mall_id, ' created')
    sql = '''INSERT INTO {s} 
                  SELECT * FROM d_final_copy_copy
                   WHERE shop_id IN (SELECT shop_id FROM shop_info WHERE mall_id='{s}')'''.format(s=mall_id)
    cur.execute(sql)
    conn.commit()
    print('finish 1 thread')


if __name__ == '__main__':
    conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()
    sql = 'SELECT DISTINCT mall_id FROM shop_info'
    cur.execute(sql)
    threads = []
    for r in cur.fetchall():
        print(r[0])
        t = threading.Thread( target=run, args=(0,r[0]) )
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    print('all done')