
# 整理数据，获取各个商场的数据矩阵
import pymysql as db
import numpy as np
import os.path as op
import threading
import time
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.decomposition import PCA

thread_mall_ids = threading.local()


def get_time():
    return time.asctime(time.localtime(time.time()))


# 归一化信号强度，1-10
def normal(val):
    ret = 0
    if val>=-10:
        ret = 0
    elif val <= -90:
        ret = 9
    else:
        ret = int(-val / 10)
    return 10 - ret


# 获取到存储文件名，固定格式
def get_file(type, mall_id):
    switcher = {
        'data' : './data/'+mall_id+'_data',
        'tar' : './data/' + mall_id + '_tar'
    }
    return switcher.get(type, 'no_data')


# 获取一个mall_id
def get_a_mall():
    try:
        return thread_mall_ids.pop()
    except :
        return ''


# 线程run方法
def run():
    print('a thread starts...')
    mall_id = get_a_mall()
    while mall_id != '':
        get_data(mall_id)
        mall_id = get_a_mall()


# 获取数据，如果数据已经存储在文件中，直接读取文件，否则进行数据库查询并构建数据
def get_data(mall_id):
    # print(mall_id)
    if op.exists(get_file('data', mall_id) + '.npy') and op.exists(get_file('tar', mall_id) + '.npy'):
        metrix = np.load(get_file('data', mall_id) + '.npy')
        tar = np.load(get_file('tar', mall_id) + '.npy')
    else:
        conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
        cur = conn.cursor()
        # 查出所有wifi，排序
        sql = 'SELECT DISTINCT wifi_ssid FROM {m} ORDER BY wifi_ssid'.format(m = mall_id)
        cur.execute(sql)
        wifi_ssids = [r[0] for r in cur.fetchall()]
        vec_mod = [0 for x in range(0,len(wifi_ssids))]
        # print(wifi_ssids)
        # 建立最终矩阵
        metrix = []
        # 创建答案数组
        tar = []
        # 查出所有数据，按照 data_id, wifi_ssid 排序
        sql = 'SELECT data_id,wifi_ssid,wifi_db,shop_id FROM {m} ORDER BY data_id,wifi_ssid'.format(m = mall_id)
        cur.execute(sql)
        r = cur.fetchone()
        data_id = r[0]
        vec = vec_mod[:]
        shop_id = r[3]
        vec[wifi_ssids.index(r[1])] = normal(r[2])
        for r in cur.fetchall():
            if r[0] != data_id:
                metrix.append(vec)
                tar.append(shop_id)
                data_id = r[0]
                vec = vec_mod[:]
                shop_id = r[3]
            vec[wifi_ssids.index(r[1])] = normal(r[2])
        metrix.append(vec)
        tar.append(shop_id)
        # print(len(metrix))
        # print(len(tar))
        # data_ids = [r[0] for r in cur.fetchall()]
        # print(data_ids)
        # # for
        # for data in data_ids:
        #     # 读取一条数据（根据data_id）
        #     sql = 'SELECT wifi_ssid,wifi_db,shop_id FROM {m} WHERE data_id={d} ORDER BY wifi_ssid'.format(m = mall_id, d = data)
        #     cur.execute(sql)
        #     # 建立向量数组
        #     vec = vec_mod[:]
        #     shop_id = ''
        #     # for
        #     for r in cur.fetchall():
        #         # 查找改条数据对应的wifi的索引
        #         # 设置向量值
        #         vec[wifi_ssids.index(r[0])] = normal(r[1])
        #         shop_id = r[2]
        #     print(get_time(), ' （为了告诉你程序还没挂）我们获取了一条数据')
        #     # append
        #     metrix.append(vec)
        #     tar.append(shop_id)
        metrix = np.array(metrix)
        tar = np.array(tar)
        np.save(get_file('data', mall_id), metrix)
        np.save(get_file('tar', mall_id), tar)
        sql = "INSERT INTO handled_mall SET mall_id='{m}'".format(m=mall_id)
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        # exit()
        print(mall_id, ' is finished')
    return metrix,tar


# 获取
def get_mall_ids():
    conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()
    sql = "SELECT mall_id FROM shop_info GROUP BY mall_id HAVING mall_id NOT IN (SELECT mall_id FROM handled_mall) ORDER BY COUNT(*)"
    cur.execute(sql)
    return [r[0] for r in cur.fetchall()]


# 文本化所有数据，20个线程
def save_all_data():
    threads = []
    for i in np.arange(0, 1):
        t = threading.Thread(target=run)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    print('done')


# 对一个商场建模（3313）
if __name__ == '__main__':
    # thread_mall_ids = get_mall_ids()
    # save_all_data()
    # print('all done')
    mall_id = 'm_2058'
    metrix, tar = get_data(mall_id)
    # print(metrix)
    # print(tar)
    # exit()
    # np.savetxt(mall_id+'_tar.txt', tar)
    # print(metrix)
    # print(tar)
    # a = np.load(mall_id + '_data.txt')
    # print(a)
    # pca = PCA(n_components=6)
    # x_new = pca.fit(a).transform(a)
    # print(x_new)

    # x_train, x_test, y_train, y_test = train_test_split(metrix, tar, test_size=0.1)
    # print('分完了')
    # knn = KNeighborsClassifier()
    # knn.fit(x_train, y_train)
    # # knn.
    # print('训练完了')
    # print(knn.predict(x_test))
    # print(knn.score(x_test, y_test))