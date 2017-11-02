# -*- coding:utf-8 -*-

import pymysql as db
import matplotlib.pyplot as plt

# 每条数据最大的wifi强度分布
if __name__ == '__main__':
    # conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    # cur = conn.cursor()
    # sql = 'SELECT MAX(wifi_db) m FROM d_final GROUP BY data_id ORDER BY m DESC'
    # cur.execute(sql)
    # x,y = [],[]
    # t = 0
    # db = 1
    # x.append(db)
    # y.append(0)
    # for r in cur.fetchall():
    #     if r[0] == -db:
    #         y[t] += 1
    #     else:
    #         db = -r[0]
    #         x.append(-r[0])
    #         t += 1
    #         y.append(1)
    # print(x)
    # print(y)
    x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 110]
    y = [18, 9, 22, 23, 45, 43, 53, 85, 126, 156, 242, 300, 423, 548, 733, 933, 1175, 1499, 1903, 2327, 2885, 3368, 4121, 4831, 5798, 6893, 8251, 9468, 11405, 13017, 15802, 17735, 20809, 23277, 26672, 28809, 31228, 32786, 35921, 37300, 38150, 39076, 39488, 39075, 40050, 40243, 39610, 38349, 37503, 36066, 34429, 32920, 31169, 29262, 27635, 26035, 24256, 22444, 20640, 18325, 16717, 15045, 14011, 12530, 11336, 9754, 8412, 7451, 6589, 5675, 4713, 4094, 3365, 2815, 2151, 1804, 1361, 1086, 856, 598, 493, 389, 257, 200, 147, 122, 78, 65, 44, 29, 18, 20, 8, 6, 5, 4, 2, 1]

    plt.xlabel('biggest wifi strength')
    plt.ylabel('count')
    plt.title('biggest wifi strength in each data')
    plt.bar(x,y)
    # plt.axis(-i for i in x)
    plt.show()