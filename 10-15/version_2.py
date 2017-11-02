
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from version_1 import get_data
import pymysql as db
import matplotlib.pyplot as plt

def cmp(x, y):
    return x > y

if __name__ == '__main__':
    conn = db.connect(host='localhost', port=3306, user='root', passwd='imseven', db='datamining')
    cur = conn.cursor()
    sql = "SELECT mall_id FROM shop_info GROUP BY mall_id"
    cur.execute(sql)

    malls = {}
    for r in cur.fetchall():
        mall_id = r[0]
        data, tar = get_data(mall_id)
        x_train, x_test, y_train, y_test = train_test_split(data, tar, test_size=0.1)
        # clf = MultinomialNB()     # avg : 0.86
        # clf = DecisionTreeClassifier()  # avg : 0.881
        clf = KNeighborsClassifier(n_neighbors=5)   # avg : 0.872
        clf.fit(x_train, y_train)
        malls[mall_id] = clf.score(x_test, y_test)
        print(mall_id, ' test done')
    print(malls.keys())
    print(malls.values())
    print('avg : ', sum(malls.values()) / len(malls.keys()))
    plt.bar(malls.keys(), sorted(malls.values()))
    plt.show()


    exit()
    mall_id = 'm_1377'
    metrix, tar = get_data(mall_id)
    x_train, x_test, y_train, y_test = train_test_split(metrix, tar, test_size=0.1)
    print('split done...')



    # knn方法
    # for i in np.arange(5,20):
    #     knn = KNeighborsClassifier(n_neighbors=i)
    #     knn.fit(x_train, y_train)
    #     # knn.
    #     print('训练完了')
    #     print('k = ', i, ' : 准确率', knn.score(x_test, y_test))

    # # 朴素贝叶斯
    # clf = GaussianNB()      # 不设参数，慢，0.5
    # clf = BernoulliNB()     # 不设参数，快，0.86
    #
    # # 决策树
    # clf = DecisionTreeClassifier()  # 不设参数，慢，0.935
    #
    #
    # clf = KNeighborsClassifier(n_neighbors=5)
    #
    # clf = SGDClassifier()
    clf = MultinomialNB()   # 不设参数，快，0.916

    clf.fit(x_train, y_train)
    print('train done...')
    print(clf.score(x_test,y_test))


