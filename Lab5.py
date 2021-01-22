

import pandas as pd
import csv
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.gaussian_process.kernels import RBF
from sklearn.ensemble import AdaBoostClassifier

df = pd.read_csv("../4lab/4lab.csv", delimiter = ",", index_col = [0], na_values = ['NA'], low_memory = False)

def set_class(vacancyName):
    if "analyst" in vacancyName and ("system" in vacancyName or "системный" in vacancyName):
        return "Системный аналитик "
    if "analyst" in vacancyName and ("data" in vacancyName or "данны" in vacancyName):
        return "Аналитик данных"
    if "developer" in vacancyName or "programmer" in vacancyName:
        return "Разработчик"
    if "devops" in vacancyName:
        return "Devops"
    if "manager" in vacancyName:
        return "Менеджер"
    if "администратор" in vacancyName or "administrator" in vacancyName:
        return "Администратор"
    if "тестировщик" in vacancyName or "tester" in vacancyName:
        return "Тестировщик"
    if "artist" in vacancyName or "designer" in vacancyName:
        return "Дизайнер"
    if "маркетолог" in vacancyName:
        return "Маркетолог"
    if "seo" in vacancyName:
        return "seo"
    if "консультант" in vacancyName:
        return "Консультант"
    if "teamlead" in vacancyName:
        return "Teamlead"
    if "руководитель" in vacancyName:
        return "Руководитель"
    if "инженер" in vacancyName or "engineer" in vacancyName:
        return "Инженер"
    return "Остальные"

df['Class'] = df['Name'].apply(lambda x: set_class(x))

df2 = df.drop(labels=['Name', 'City', 'Employer name', 'Description', 'Responsibility','Requirement'], axis=1)

df2 = df2.dropna(subset=['Salary from', 'Salary to'])
df2.reset_index(drop=True, inplace=True)

leClass = LabelEncoder()
leClass.fit(list(df2['Class'].astype(str).values))
f = open("Classes.txt", "w")
for i in range(len(leClass.classes_)):
    f.write(str(i)+" "+leClass.classes_[i]+"\n")
f.close()
df2['Class'] = leClass.transform(list(df2['Class'].astype(str).values))

oheExpierence  = OneHotEncoder(sparse=False)
oheExpierence.fit(df2['Expierence'].to_numpy().reshape(-1, 1))
transformed = oheExpierence.transform(df2['Expierence'].to_numpy().reshape(-1, 1))
ohe_df = pd.DataFrame(transformed, columns=oheExpierence.get_feature_names())
df2 = pd.concat([df2, ohe_df], axis=1).drop(['Expierence'], axis=1)

oheEmployment  = OneHotEncoder(sparse=False)
oheEmployment.fit(df2['Employment'].to_numpy().reshape(-1, 1))
transformed = oheEmployment.transform(df2['Employment'].to_numpy().reshape(-1, 1))
ohe_df = pd.DataFrame(transformed, columns=oheEmployment.get_feature_names())
df2 = pd.concat([df2, ohe_df], axis=1).drop(['Employment'], axis=1)

oheSchedule  = OneHotEncoder(sparse=False)
oheSchedule.fit(df2['Schedule'].to_numpy().reshape(-1, 1))
transformed = oheSchedule.transform(df2['Schedule'].to_numpy().reshape(-1, 1))
ohe_df = pd.DataFrame(transformed, columns=oheSchedule.get_feature_names())
df2 = pd.concat([df2, ohe_df], axis=1).drop(['Schedule'], axis=1)

text_transformer = CountVectorizer()
text = text_transformer.fit_transform(df2['Key skills'])
words = pd.DataFrame(text.toarray(), columns=text_transformer.get_feature_names())
df2 = pd.concat([df2, words], axis=1).drop(['Key skills'], axis=1)

data = df2.drop(labels=['Class'], axis=1)
target = df2['Class']

train_data, test_data, train_target, test_target = train_test_split(data,target, test_size=0.3, random_state=0)


def test_model(model):
    model.fit(train_data, train_target)
    print("cv: "+str(model.score(test_data, test_target)))

print("MultinomialNB")
model = MultinomialNB() #0.19148936170212766
test_model(model)

print("AdaBoostClassifier")
model = AdaBoostClassifier(n_estimators=500) #0.4263056092843327
test_model(model)

print("KNeighborsClassifier")
model = KNeighborsClassifier(n_neighbors=20) #0.5822050290135397
test_model(model)

print("RandomForestClassifier")
model = RandomForestClassifier(n_estimators=190, max_depth = 80) #0.6874274661508704
test_model(model)

print("SVC RBF")
model = SVC(kernel='rbf',gamma='auto') #0.6481624758220503
test_model(model)

df_new = pd.read_csv("lab4-Тюмень.csv", delimiter = ",", index_col = [0], na_values = ['NA'], low_memory = False)

df_new['Class'] = df_new['Name'].apply(lambda x: set_class(x))
df_new = df_new.dropna(subset=['Salary from', 'Salary to'])
copy = df_new.copy()


df_new = df_new.drop(labels=['Name', 'City', 'Employer name', 'Description','Responsibility','Requirement'], axis=1)
df_new.reset_index(drop=True, inplace=True)

df_new['Class'] = leClass.transform(list(df_new['Class'].astype(str).values))

transformed = oheExpierence.transform(df_new['Expierence'].to_numpy().reshape(-1, 1))
ohe_df = pd.DataFrame(transformed, columns=oheExpierence.get_feature_names())
df_new = pd.concat([df_new, ohe_df], axis=1).drop(['Expierence'], axis=1)

transformed = oheEmployment.transform(df_new['Employment'].to_numpy().reshape(-1, 1))
ohe_df = pd.DataFrame(transformed, columns=oheEmployment.get_feature_names())
df_new = pd.concat([df_new, ohe_df], axis=1).drop(['Employment'], axis=1)

transformed = oheSchedule.transform(df_new['Schedule'].to_numpy().reshape(-1, 1))
ohe_df = pd.DataFrame(transformed, columns=oheSchedule.get_feature_names())
df_new = pd.concat([df_new, ohe_df], axis=1).drop(['Schedule'], axis=1)

text = text_transformer.transform(df_new['Key skills'])
words = pd.DataFrame(text.toarray(), columns=text_transformer.get_feature_names())
df_new = pd.concat([df_new, words], axis=1).drop(['Key skills'], axis=1)

data_new = df_new.drop(labels=['Class'], axis=1)
target_new = df_new['Class']
model = RandomForestClassifier(n_estimators=190, max_depth = 80)

model.fit(data, target)
print(str(model.score(data_new, target_new)))
copy["Predicted class"] = model.predict(data_new)

copy["Predicted class"] = leClass.inverse_transform(list(copy["Predicted class"].values))
copy.to_csv("lab5.csv",  na_rep = 'NA', index = True, index_label = "", quotechar = '"', quoting = csv.QUOTE_NONNUMERIC, encoding = "utf-8-sig")
