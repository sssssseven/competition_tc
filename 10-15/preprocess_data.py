
# 数据处理，删除某些wifi


import pymysql as db
import threading


def run(index, ssids_):
    print('thread {t} starts...'.format(t = index))
    conn_ = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur_ = conn_.cursor()
    f = open(str(index) + '.txt', 'a')
    handled = 0;
    for r in ssids_:
        sql = "DELETE FROM d_final_copy_copy WHERE wifi_ssid='{s}'".format(s=r)
        cur_.execute(sql)
        handled += 1
        f.write('{h:8} were handled : {w:10} , and {cnt} were deleted.\n'.format(h=handled, w=r, cnt = cur_.rowcount))
        if handled % 100 == 0:
            f.flush()
            print(handled, '\twere handled!')
    conn_.commit()
    print('thread {t} finished...'.format(t = index))

if __name__ == '__main__':
    conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()
    strength = 30
    connects = 9
    sql = 'SELECT wifi_ssid FROM d_final_index GROUP BY wifi_ssid HAVING MAX(wifi_db)<-{s} AND COUNT(*)<{c}'.format(s=strength, c=connects)
    cur.execute(sql)
    ssids = [x[0] for x in cur.fetchall() ]
    size = 20000
    ssids_split = [ssids[i:i+size] for i in range(0, len(ssids), size)]
    threads = []
    for i in ssids_split:
        t = threading.Thread(target=run, args=(ssids_split.index(i), i));
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    print("ALL DONE!")
