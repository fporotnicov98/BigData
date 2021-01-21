import pandas
import re
import csv
import dateutil.parser
import datetime

df = pandas.read_csv("../3lab/3lab.csv", delimiter = ",", index_col = [0], na_values = ['NA'], low_memory = False)

limiter = ['/','+',' ']

variablesSearch = [
  'javascript',
  'node.js',
  'html5',
  'reactjs'
  'backend',
  'frontend',
  'linux',
  'junior',
  'middle',
  'senior',
  'c++',
  'c#',
  'php',
  'asp.net',
  'ui',
  'ux',
  'sql',
  'vuejs',
  'angular',
  'unix',
  'java',
  'kotlin',
  'golang',
  'python'
]

variants = [
  ['big data','bigdata'],
  ['mssql','ms sql'],
  ['frontend', 'фронтенд'],
  ['backend', 'бэкенд'],
  ['developer', 'разработчик'],
  ['analyst','аналитик'],
  ['fullstack', 'фулл-стек'],
  ['programmer', 'программист'],
  ['designer', 'дизайнер'],
  ['manager', 'менеджер'],
  ['junior', 'джуниор'],
  ['middle','миддл'],
  ['senior', 'сеньор'],
  ['teamlead','тимлид'],
  ['js', 'javascript'],
  ['web','веб'],
  ['devops', 'девопс']
]

delete = ['удаленно','remote','фултайм','fulltime','(\(.*\))','(\[.*\])','\s+в\s+.*','г\.\s+.*']

print("Заменяем знаки в столбце наименования вакансий")

df["Name"] = df["Name"].apply(lambda x: x.lower()) # в нижний регистр
df["Name"] = df["Name"].apply(lambda x: x.replace('\\','/')) # замена символов
df["Name"] = df["Name"].apply(lambda x: x.replace('|','/'))
df["Name"] = df["Name"].apply(lambda x: x.replace(',','/'))
df["Name"] = df["Name"].apply(lambda x: x.replace('-',' '))
df["Name"] = df["Name"].apply(lambda x: re.sub('\s*\/\s*', '/', x)) # поиск и замена
df["Name"] = df["Name"].apply(lambda x: re.sub('\s+', ' ', x).strip()) # поиск и замена с удалением пробелов

print("Удаляем найденные слова")

for d in delete:
    regex = re.compile(d)
    df["Name"] = df["Name"].apply(lambda x: regex.sub('', x))

print("Замена слов")

for v in variants:
    for i in range(1, len(v)):
        df["Name"] = df["Name"].apply(lambda x: x.replace(v[i],v[0]))

print("Перестановка различных вариантов в названии")

for i in range(len(variablesSearch)):
    for j in range(i+1, len(variablesSearch)):
        for d in limiter:
            regex = re.compile(re.escape(variablesSearch[j])+'\s*'+re.escape(d)+'\s*'+re.escape(variablesSearch[i]))
            df["Name"] = df["Name"].apply(lambda x: regex.sub(variablesSearch[i]+limiter[0]+variablesSearch[j], x))

df["Name"] = df["Name"].apply(lambda x: re.sub('\s+', ' ', x).strip())

print("Замена пустых значений")

df["companyName"] = df["companyName"].fillna("Не указано")
df["city"] = df["city"].fillna("Не указан")
df["expierence"] = df["expierence"].fillna("Не требуется")
df["employment"] = df["employment"].fillna("Любой тип")
df["schedule"] = df["schedule"].fillna("Любой график")
df["responsibility"] = df["responsibility"].fillna("Не требуются")
df["requirement"] = df["requirement"].fillna("Не требуются")
df["keySkill"] = df["keySkill"].fillna("Не требуются")

df["minSalary"] = df.groupby(["Name", "city"]).transform(lambda x: x.fillna(x.mean()))["minSalary"]
df["maxSalary"] = df.groupby(["Name", "city"]).transform(lambda x: x.fillna(x.mean()))["maxSalary"]

print("Добавим новый признак: 'Количество дней с момента размещения' ")

df["Published at"] = df["Published at"].apply(lambda x: (datetime.datetime.now() - dateutil.parser.parse(x).replace(tzinfo=None)).days)

print("Сохранение")

df.to_csv("lab4.csv",  na_rep = 'NA', index = True, index_label = "", quotechar = '"', quoting = csv.QUOTE_NONNUMERIC, encoding = "utf-8-sig")

print("Готово")