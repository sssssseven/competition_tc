import pymysql as db

if __name__ == '__main__':
    conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()

    t = 5
    acc = 1
    while acc > 0.98:
        cur.execute("SELECT shop_id,wifi_ssid FROM behavior_final WHERE wifi_db>-{s} ORDER BY wifi_ssid".format(s=t))
        d = dict()
        err = 0
        wifi_cnt = 0
        for r in cur.fetchall():
            if d.__contains__(r[1]):
                if d[r[1]] != r[0]:
                    err += 1
            else:
                d[r[1]] = r[0]
                wifi_cnt += 1
        acc = 1 - err / (cur.rowcount - wifi_cnt)
        print('取值为\t{t}\t时，共\t{s}\t条数据，其中出错的数据有\t{e}\t条，wifi热点个数为\t{w}\t，平均每个热点有\t{a_l:.3}\t个连接，平均每个热点出错\t{a_e:.2}\t个，总准确率为\t{p:.5}\t%。'
              .format(t = t,s = cur.rowcount,e = err,p = acc * 100, w = wifi_cnt, a_l = cur.rowcount/wifi_cnt, a_e = err/wifi_cnt))
        t += 2