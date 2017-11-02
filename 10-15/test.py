import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.decomposition import PCA
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
import numpy as np

import utils


# xgb根据商场数据量加权的准确率
malls = utils.get_malls()
conn = utils.get_db_conn()
cur = conn.cursor()
cnts = []
acc_rates = []
for mall_id in malls:
    sql = 'SELECT COUNT(DISTINCT data_id) FROM %s' % mall_id
    cur.execute(sql)
    cnt = cur.fetchone()[0]
    cnts.append(cnt)
    sql = "SELECT xgb FROM scores WHERE mall_id='%s'" % mall_id
    cur.execute(sql)
    acc_rate = cur.fetchone()[0]
    acc_rates.append(acc_rate)
print('cnts : ', cnts)
print('rates : ', acc_rates)
rights = [cnts[i] * float(acc_rates[i]) for i in range(0, len(cnts))]
print(rights)
print(sum(rights)/sum(cnts))

exit()

mall_id = 'm_8379'
model = utils.get_model_xgb(mall_id)
data = np.load('./data/' +  mall_id + '_data.npy')
print(model.predict(data))

exit()

vec = [0,0,0,0]
v = vec[:]
v[0] = 1
print(v)
print(vec)
exit()

a = 4
b = [3,2,1]
print(b.__contains__(a))
print(b.index(a))
exit()

iris = datasets.load_iris()

X = iris.data
y = iris.target
target_names = iris.target_names

print(X)

pca = PCA(n_components=2)
X_r = pca.fit(X).transform(X)

print(X_r)
exit()
lda = LinearDiscriminantAnalysis(n_components=2)
X_r2 = lda.fit(X, y).transform(X)

# Percentage of variance explained for each components
print('explained variance ratio (first two components): %s'
      % str(pca.explained_variance_ratio_))

plt.figure()
colors = ['navy', 'turquoise', 'darkorange']
lw = 2

for color, i, target_name in zip(colors, [0, 1, 2], target_names):
    plt.scatter(X_r[y == i, 0], X_r[y == i, 1], color=color, alpha=.8, lw=lw,
                label=target_name)
plt.legend(loc='best', shadow=False, scatterpoints=1)
plt.title('PCA of IRIS dataset')

plt.figure()
for color, i, target_name in zip(colors, [0, 1, 2], target_names):
    plt.scatter(X_r2[y == i, 0], X_r2[y == i, 1], alpha=.8, color=color,
                label=target_name)
plt.legend(loc='best', shadow=False, scatterpoints=1)
plt.title('LDA of IRIS dataset')

plt.show()