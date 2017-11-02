
# 负责分发商场id
from multiprocessing.managers import BaseManager
import queue
import pymysql
import time

task_queue = queue.Queue()
result_queue = queue.Queue()


def t():
    return task_queue

def r():
    return result_queue

class QueManager(BaseManager):
    pass

if __name__ == '__main__':
    # 注册队列
    QueManager.register('get_task',callable=t)
    QueManager.register('get_result', callable=r)

    # 设置服务器信息
    manager = QueManager(address=('127.0.0.1',7788), authkey=b'seven')
    # 启动
    manager.start()
    # 获取que对象
    task = manager.get_task()
    result = manager.get_result()
    # 数据库查询
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='imseven', db='datamining')
    cur =  conn.cursor()
    sql = 'SELECT mall_id FROM scores WHERE xgb_itr_times=0 ORDER BY mall_id desc'
    cur.execute(sql)
    # 将待处理的商场id放入队列
    for r in cur.fetchall():
        task.put(r[0])
        print('put a mall id : ', r[0])
    # 每分钟查询一次结果队列
    while True:
        try:
            r = result.get(timeout=10)  # block is true, 该方法默认阻塞
            if r:
                print('handled : %s . left task : %d' % (r, task.qsize()) )
            else:
                print('no task handled in this minute. task left : %d' % task.qsize())
            time.sleep(10)
        except queue.Empty:
            print('no task handled in this minute. task left : %d' % task.qsize())
            time.sleep(60)


