
import matplotlib.pyplot as plt
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']

import utils


# 各个wifi强度，以及该强度下的连接数量
# type 为 0 处理之前的数据，1 处理之后的数据
def get_wifi_strength_data(type = 0):
    conn = utils.get_db_conn()
    cur = conn.cursor()
    table = 'd_final_index' if type == 0 else 'd_final_handled'
    sql = 'SELECT wifi_db,COUNT(*) FROM %s GROUP BY wifi_db ORDER BY wifi_db DESC' % table
    print(sql)
    cur.execute(sql)
    x = []
    y = []
    for r in cur.fetchall():
        x.append(-r[0])
        y.append(r[1])
    return x, y


# 图：各个wifi强度，以及该强度下的连接数量
def g():
    fig = plt.figure(figsize=(20, 10))
    titles = ['before processing', 'after processing']
    for i in [0, 1]:
        x, y = get_wifi_strength_data(i)
        ax = fig.add_subplot(1, 2, i + 1)
        ax.set_title(titles[i])
        ax.set_ylim(-10000, 330000)
        ax.plot(x, y)
    fig.savefig('./figs/wifi_strength_counts.png', format='png')
    plt.show()


# 显示并保存图片
def plot_and_save(x, y, path, title='untitled',format='png'):
    fig = plt.figure(figsize=(8, 6))
    fig.add_subplot()
    ax = fig.add_subplot(111)
    ax.set_title(title)
    ax.plot(x, y)
    fig.savefig(path, format=format)
    plt.show()


# 显示并保存图片(柱状图)
def plot_and_save_bar(x, y, path, title='untitled',format='png'):
    fig = plt.figure(figsize=(8, 6))
    ax = fig.add_subplot(111)
    ax.set_title(title)
    ax.bar(x, y)
    fig.savefig(path, format=format)
    plt.show()



# 获取全部数据的消费时间分布，周分布和日分布
def d():
    conn = utils.get_db_conn()
    cur = conn.cursor()
    sql = 'SELECT d,t,DAYOFWEEK(t) w,HOUR(t) h FROM (SELECT DISTINCT data_id d,time_stamp  t FROM d_final_index) AS tmpr ORDER BY h,w'
    cur.execute(sql)
    dow = [0 for x in range(0, 7)]
    time = [0 for x in range(0, 24)]
    for r in cur.fetchall():
        dow[r[2] - 1] += 1
        time[r[3]] += 1
    plot_and_save([1, 2, 3, 4, 5, 6, 7], dow, './figs/消费时间周分布.png', title='消费时间周分布')
    plot_and_save([x for x in range(0, 24)], time, './figs/消费时间日分布.png', title='消费时间日分布')


# 显示并存储某商场的周消费分布和日消费分布
def t(mall_id):
    conn = utils.get_db_conn()
    cur = conn.cursor()
    sql = "SELECT DAYOFWEEK(`timestamp`) w,HOUR(`timestamp`) h FROM behavior_pre WHERE shop_id='%s'" % mall_id
    cur.execute(sql)
    dow = [0 for x in range(0, 7)]
    time = [0 for x in range(0, 24)]
    for r in cur.fetchall():
        dow[r[0] - 1] += 1
        time[r[1]] += 1
    plot_and_save([1, 2, 3, 4, 5, 6, 7], dow, './figs/消费时间周分布_%s.png' % mall_id, title='消费时间周分布')
    plot_and_save([x for x in range(0, 24)], time, './figs/消费时间日分布_%s.png' % mall_id, title='消费时间日分布')


if __name__ == '__main__':
    # 查看某商场的商店地理分布, 相同种类商店颜色一致
    mall_id = 'm_2224'
    conn = utils.get_db_conn()
    cur = conn.cursor()
    sql = '''
            SELECT b.id,category_id,s.shop_id,s.longitude,s.latitude,b.longitude,b.latitude 
            FROM shop_info s LEFT JOIN behavior_pre b ON s.shop_id=b.shop_id
            WHERE mall_id='%s' ORDER BY category_id,b.shop_id
            ''' % mall_id
    cur.execute(sql)
    category = 0
    shop_id = ''
    color = (0,0,0)#'#cccccc'
    fig = plt.figure(figsize=(8,6))
    ax = fig.add_subplot(111)
    for r in cur.fetchall():
        if category != r[0] and category != 0 :
            # change color
            category = r[1]
            ax.plot(float(r[3]), float(r[4]), 'o', color=color)
        ax.plot(float(r[5]), float(r[6]), color=color)
    plt.show()
