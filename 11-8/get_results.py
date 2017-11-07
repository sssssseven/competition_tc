
# 计算测试数据

import numpy as np
import utils

if __name__ == '__main__':
    malls = utils.get_malls()
    conn = utils.get_db_conn()
    cur = conn.cursor()
    i = 1
    for mall_id in malls:
        print(utils.get_time(), ' ','start handle mall ', mall_id)
        # 获取模型
        model = utils.get_model_xgb(mall_id)
        if model == 0:
            print('no model for mall ', mall_id)
            continue
        # 查出所有wifi，排序
        sql = 'SELECT DISTINCT wifi_ssid FROM {m} ORDER BY wifi_ssid'.format(m=mall_id)
        cur.execute(sql)
        wifis = [r[0] for r in cur.fetchall()]
        # 初始化数据矩阵和初始向量
        matrix = []
        weight_conn = 1.5   # 连接为true时的权重
        matrix_day = []
        weight_day = 3  # [0, 0, 3, 0, 0, 0, 0]
        matrix_hour = []
        # 以上三个矩阵分别存储wifi信息，消费时间是周几的信息，消费时间是几点的信息，最后合并三个矩阵，作为全部数据
        weight_hour = 3 # [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        vec = [0 for wifi in range(0, len(wifis))]
        vec_mod_day = [0 for x in range(0,7)]
        vec_mod_hour = [0 for x in range(0,24)]
        rows = []
        # 查询所有数据
        sql = "SELECT data_id,wifi_ssid,wifi_db,time_stamp,wifi_conn,DAYOFWEEK(time_stamp),HOUR(time_stamp),MINUTE(time_stamp) FROM data_test_final WHERE mall_id='%s' ORDER BY data_id,wifi_ssid " % mall_id
        cur.execute(sql)
        row = cur.fetchone()
        v = vec[:]
        vec_day = vec_mod_day[:]
        vec_day[ row[5] - 1 ] = weight_day
        vec_hour = vec_mod_hour[:]
        hour = (row[6]+1) if row[7]>=30  else row[6]
        vec_hour[0 if hour > 23 else hour] = weight_hour
        row_id = row[0]
        if wifis.__contains__(row[1]):
            v[wifis.index(row[1])] = utils.normal(row[2])
        for r in cur.fetchall():
            # 根据是否与前一条row_id相同进行不同操作
            if r[0] != row_id:
                matrix.append(v)
                matrix_day.append(vec_day)
                matrix_hour.append(vec_hour)
                rows.append(row_id)
                v = vec[:]
                vec_day = vec_mod_day[:]
                vec_day[r[5] - 1] = weight_day
                vec_hour = vec_mod_hour[:]
                hour = (r[6] + 1) if r[7] >= 30  else r[6]
                vec_hour[0 if hour > 23 else hour] = weight_hour
                row_id = r[0]
            if wifis.__contains__(r[1]):
                v[wifis.index(r[1])] = utils.normal(r[2])
        matrix.append(v)
        matrix_day.append(vec_day)
        matrix_hour.append(vec_hour)
        rows.append(row_id)
        matrix = np.hstack([matrix_day,matrix_hour,matrix])
        result = model.predict(matrix)
        # print(result)
        # print(cur.rowcount)
        # print(len(result))
        # print(len(rows))
        for r in range(0, len(rows)):
            sql = "INSERT INTO data_test_result VALUES ('{r}','{s}')".format(r = rows[r], s=result[r])
            cur.execute(sql)
        sql = "INSERT INTO data_test_handled SET mall_id='%s',handled=1" % mall_id
        cur.execute(sql)
        conn.commit()
        print(utils.get_time(), ' ',mall_id, ' handled done')
        print(i, ' handled.')
        i += 1
