
from matplotlib.widgets import Cursor
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']

np.random.seed(19680801)

fig = plt.figure(figsize=(8,8))
ax = fig.add_subplot(111,facecolor='#cccccc')
ax2 = fig.add_subplot(122,facecolor='#ffffff')

x, y = 4*(np.random.rand(2, 100) - .5)
# print(x)
ax.set_title('爱爱爱')
ax.plot(x, y, 'o')
ax.plot(y, x, 'o')
ax2.plot(x, y, 'o')
ax2.plot(y, x, 'o')
# ax.set_xlim(-2, 2)
# ax.set_ylim(-2, 2)

# cursor = Cursor(ax, useblit=True, color='red', linewidth=2)

plt.show()