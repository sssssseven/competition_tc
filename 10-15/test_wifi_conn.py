import pymysql as db
import matplotlib.pyplot as plt

# 单个wifi连接数分布
if __name__ == '__main__':
    conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()
    sql = 'SELECT COUNT(*) c FROM d_final GROUP BY wifi_ssid ORDER BY c'
    cur.execute(sql)
    x,y = [],[]
    t = 0
    tmp = 1
    x.append(1)
    y.append(0)
    for r in cur.fetchall():
        if t > 100:
            break
        if r[0] == tmp:
            y[t] += 1
        else:
            tmp = r[0]
            x.append(r[0])
            y.append(0)
            t += 1

    print(x)
    print(y)
    plt.xlabel('biggest wifi strength')
    plt.ylabel('count')
    plt.title('biggest wifi strength in each data')
    plt.bar(x,y)
    # plt.axis(x)
    plt.show()