import queue
import time
import pymysql
from multiprocessing.managers import BaseManager
from sklearn.externals import joblib  # 储存模型
from sklearn.model_selection import train_test_split
from xgboost.sklearn import XGBClassifier  # Boosting Tree方法

import utils


class QueManager(BaseManager):
    pass


def train_model(mall_id):
    # 开始训练模型
    random_state = 10
    metrix, tar = utils.get_data(mall_id)
    x_train, x_test, y_train, y_test = train_test_split(metrix, tar, test_size=0.1, random_state=random_state)
    # xgboost方法，基于boosting tree(提升树方法)
    # 设参数 训练慢
    clf_name = "xgboost"
    save_dir = "./model/" + clf_name + "_" + mall_id + "_model.m"
    n_est = 50
    clf = XGBClassifier(learning_rate=0.1,  # 学习率 典型值为0.01-0.2
                        n_estimators=n_est,
                        max_depth=5,  # 树的最大深度 一般3-10
                        min_child_weight=1,  # 决定最小叶子节点样本权重和 值较大，避免过拟合 值过高，会导致欠拟合
                        gamma=0,  # 指定了节点分裂所需的最小损失函数下降值。 这个参数的值越大，算法越保守
                        subsample=0.8,  # 对于每棵树，随机采样的比例 减小，算法保守，避免过拟合。值设置得过小，它会导致欠拟合 典型值：0.5-1
                        colsample_bytree=0.8,  # 每棵随机采样的列数的占比
                        objective='binary:logistic',  # 使用二分类
                        nthread=4,  # 线程数
                        scale_pos_weight=1,  # 在各类别样本十分不平衡时，参数设定为一个正值，可以使算法更快收敛
                        seed=0)  # 随机数的种子 设置它可以复现随机数据的结果
    print(utils.get_time(), ' ', mall_id, ' starts...')
    train_time = time.time()
    clf.fit(x_train, y_train)
    train_time = time.time() - train_time
    score = clf.score(x_test, y_test)
    joblib.dump(clf, save_dir)
    print(utils.get_time(), ' saved a model for ', mall_id, ' score: ', score, '  train time : ', train_time)
    train_time = int(train_time)
    return (score, n_est, train_time)


if __name__ == '__main__':
    # 从网络下载，只注册名字
    QueManager.register('get_task')
    QueManager.register('get_result')
    # 服务器信息
    server_addr = '127.0.0.1'
    server_port = 7788
    # 创建并连接
    m = QueManager(address=(server_addr, server_port), authkey=b'seven')
    m.connect()
    # 获取队列
    task = m.get_task()
    result = m.get_result()
    # 连接数据库
    conn = pymysql.connect(host=server_addr, port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()
    # 处理任务
    while task.qsize() != 0:
        try:
            mall_id = task.get(timeout=1)
            print('start handling mall ', mall_id)
            # 判断是否已经生成过模型
            sql = "SELECT xgb_itr_times FROM scores WHERE mall_id='{m}'".format(m=mall_id)
            cur.execute(sql)
            if cur.fetchone()[0] != 0:
                print(mall_id, ' has already been fitted.')
                continue
            else:
                r = train_model(mall_id)
                # 更新数据库
                if r == 0:
                    print(mall_id, ' already exists.')
                else:
                    sql = "UPDATE scores SET xgb='{s}',xgb_itr_times={t}, xgb_train_time={tt} WHERE mall_id='{m}'".format(
                        s=r[0], t=r[1], m=mall_id, tt=r[2])
                    cur.execute(sql)
                    conn.commit()
                result.put(mall_id)
        except queue.Empty:
            break
