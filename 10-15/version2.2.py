
import numpy as np
import pymysql as db
import matplotlib.pyplot as plt
from sklearn.externals import joblib    #储存模型
import os.path as op
import time

from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from sklearn import metrics
from xgboost.sklearn import XGBClassifier   #Boosting Tree方法
from version_1 import get_data
from version_1 import get_file



def get_time():
    return time.asctime(time.localtime(time.time()))

if __name__ == '__main__':
    # conn = db.connect(host='localhost', port=3306, user='root', passwd='199477', db='tianchi2')
    # cur = conn.cursor()
    # sql = "SELECT mall_id FROM shop_info GROUP BY mall_id"
    # pers = []
    # for r in cur.fetchall():
    #     mall_id = r[0]
    malls = []
    random_state = 10
    for mall_id in malls:
        metrix, tar = get_data(mall_id)
        # random_state是随机数的种子，如果该数不是0，每次随机数组是确定的
        # 此处应该保持random_state是固定的，否则测试训练好的的模型，它会测试训练时用过的数据
        x_train, x_test, y_train, y_test = train_test_split(metrix, tar, test_size=0.1, random_state=random_state)

        # xgboost方法，基于boosting tree(提升树方法)
        # 设参数 训练慢
        i = 1
        score = 0
        while True:
            n_est = i * 10
            clf_name = "xgboost"

            save_dir = "./model/" + clf_name + "_" +mall_id + "_model.m"
            if op.exists(save_dir):
                clf = joblib.load(save_dir)
                print("load the existing model...")
            else:
                clf = XGBClassifier(learning_rate =0.1, #学习率 典型值为0.01-0.2
                                    n_estimators=n_est,
                                    max_depth=5,   #树的最大深度 一般3-10
                                    min_child_weight=1,    #决定最小叶子节点样本权重和 值较大，避免过拟合 值过高，会导致欠拟合
                                    gamma=0,   #指定了节点分裂所需的最小损失函数下降值。 这个参数的值越大，算法越保守
                                    subsample=0.8, #对于每棵树，随机采样的比例 减小，算法保守，避免过拟合。值设置得过小，它会导致欠拟合 典型值：0.5-1
                                    colsample_bytree=0.8,  #每棵随机采样的列数的占比
                                    objective= 'binary:logistic',    #使用二分类
                                    nthread=4, #线程数
                                    scale_pos_weight=1,    #在各类别样本十分不平衡时，参数设定为一个正值，可以使算法更快收敛
                                    seed=0)   #随机数的种子 设置它可以复现随机数据的结果
                clf.fit(x_train, y_train)
                joblib.dump(clf, save_dir)
                print('save the model...')
            score_tmp = clf.score(x_test,y_test)
            print(n_est, ' : ', score_tmp)

            # print('test done... spent time = {s}'.format(s=get_time() - time))



