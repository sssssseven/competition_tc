import pymysql as msq
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm

if __name__ == '__main__':

    # X = np.arange(-5, 5, 0.25)
    # Y = np.arange(-5, 5, 0.25)
    # X, Y = np.meshgrid(X, Y)
    # R = np.sqrt(X ** 2 + Y ** 2)
    # Z = np.sin(R)
    # print(X)
    # print(Y)
    # print(Z)
    #
    # fig = plt.figure()
    # ax = Axes3D(fig)
    # ax.plot_surface(X, Y, Z, rstride=1, cstride=1, cmap=cm.viridis)
    #
    # plt.show()
    #
    #
    # exit()

    conn = msq.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()

    fig = plt.figure(figsize=(8,6))
    ax = fig.add_subplot(111,facecolor='#cccccc')

    shops = ['s_461797','s_305973']
    style = ['b.','g.']
    style2 = ['bo','go']
    i = 0
    for shop in shops:
        sql = "SELECT longitude,latitude FROM behavior_pre WHERE shop_id='{s}'".format(s=shop)
        cur.execute(sql)
        x = []
        y = []
        for r in cur.fetchall():
            x.append(float(r[0]))
            y.append(float(r[1]))
        sql = "SELECT longitude,latitude FROM shop_info WHERE shop_id='{s}'".format(s=shop)
        cur.execute(sql)
        r = cur.fetchone()
        ax.plot(x,y,style[i])
        ax.plot(float(r[0]),float(r[1]),style2[i],markersize=14)
        i+=1
    plt.show()


from math import radians, cos, sin, asin, sqrt


def haversine(lon1, lat1, lon2, lat2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r * 1000