
import src.utils as u
import numpy as np
from sklearn.model_selection import train_test_split
import gc

if __name__ == '__main__':
    random_state = 3
    test_size = 0.05
    # malls = u.get_malls()
    malls = ['m_7800']
    algs = ['knn','RF']
    for mall in malls:
        print('start ',mall)
        data, tar = u.get_data(mall)
        # x_train,x_test, y_train, y_test = train_test_split(data, tar, test_size=test_size, random_state=random_state)
        x_train, x_test, y_train, y_test = data,data,tar,tar
        labels = sorted(set(y_train))
        print('start ', 'xgb')
        model = u.get_model_xgb(mall)
        score = model.score(x_test, y_test)
        print(mall, ' score : ', score, '  ', 'xgb')
        print(mall, ' predicting  ', 'xgb')
        result = []
        result_proba = []
        result.append(model.predict(x_test))
        result_proba.append(model.predict_proba(x_test))
        for al in algs:
            gc.collect()
            print('start ', al)
            model = u.get_model(al)
            print(mall,' training model ', al)
            model.fit(x_train,y_train)
            score = model.score(x_test,y_test)
            print(mall, ' score : ', score, '  ', al)
            print(mall, ' predicting  ', al)
            result.append(model.predict(x_test))
            result_proba.append(model.predict_proba(x_test))
        wrong = 0
        # print(result)
        result = [[result[0][i],result[1][i],result[2][i]] for i in range(0, len(result[0]))]
        for i in range(0, len(y_test)):
            if y_test[i] not in result[i] or len(set(result[i])) > 1:
                print(y_test[i],'-------',result[i])
                if y_test[i] not in result[i]:
                    for proba in result_proba:
                        print(sorted(dict(zip(labels,proba[i])).items(), key=lambda item:item[1], reverse=True)[:5])
                    wrong += 1
        print('wrong : ', wrong)
        print(1-wrong/len(y_test))
        print(mall, 'done')