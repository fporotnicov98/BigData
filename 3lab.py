import pandas as pd
import numpy as np
import urllib.request, json 
import csv
import dateutil.parser
from datetime import datetime, time
import os
import shutil
import threading

mapColumns = [
        {
            'column' : 'Name',
            'json_name' : 'name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'city',
            'json_name' : 'address.city',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'minSalary',
            'json_name' : 'salary.from',
            'dtype' : 'float',
            'multiple' : False
        },
        {
            'column' : 'maxSalary',
            'json_name' : 'salary.to',
            'dtype' : 'float',
            'multiple' : False
        },
        {
            'column' : 'companyName',
            'json_name' : 'employer.name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'publishedAt',
            'json_name' : 'published_at',
            'dtype' : 'str',
            'multiple' : False
        },
        {
            'column' : 'expierence',
            'json_name' : 'experience.name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'employment',
            'json_name' : 'employment.name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'schedule',
            'json_name' : 'schedule.name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'description',
            'json_name' : 'description',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'responsibility',
            'json_name' : 'snippet.responsibility',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'requirement',
            'json_name' : 'snippet.requirement',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'keySkill',
            'json_name' : 'keySkill.name',
            'dtype' : 'object',
            'multiple' : True
        }
    ]
print("********Часть 1********")
def getUrlJson(url): #получение адреса
    while(True):
        try:
            response = urllib.request.urlopen(url)
            return json.loads(response.read().decode("utf-8"))
        except:
            time.sleep(0.5)
            
def getPages(specialization):
    curentPage = 0
    totalPages = 1
    while(curentPage<totalPages):
        page = getUrlJson("https://api.hh.ru/vacancies?specialization="+specialization+"&per_page=100&page="+str(curentPage))
        totalPages = page["pages"]
        curentPage += 1
        yield page
        
def getVacancyThread(lst, vacancies):
    for vacancy in vacancies:
        lst.append([vacancy,getUrlJson("https://api.hh.ru/vacancies/"+str(vacancy['id']))])

def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def getVacanciesDetails(listOfVacancies):
    parts = list(split(listOfVacancies, 20))
    results = []
    threads = []
    for i in range(len(parts)):
        results.append([])
        thread = threading.Thread(target=getVacancyThread, args=(results[i],parts[i],))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    return [item for sublist in results for item in sublist]

def getValue(column, value, multiple=""):
    if column['multiple']:
        result = ""
        for item in value:
            result += str(item[multiple]) + ';'
        result = result[:-1]
        if result == "":
            result = np.nan
    else:
        if value == None:
            result = np.nan
        else:
            result = str(value)
    return result

def addVacancyToRow(vacancy, details): #добавление вакансий в массив
    row = []
    for column in mapColumns:
        names = column['json_name'].split('.')
        if len(names) == 1:
            if names[0] in vacancy and vacancy[names[0]] != None:
                row.append(getValue(column, vacancy[names[0]]))
            elif names[0] in details and details[names[0]] != None:
                row.append(getValue(column, details[names[0]]))
            else:
                row.append(np.nan)
        else:
            if column['multiple']:
                if names[0] in vacancy and vacancy[names[0]] != None:
                    row.append(getValue(column, vacancy[names[0]], names[1]))
                elif names[0] in details and details[names[0]] != None:
                    row.append(getValue(column, details[names[0]], names[1]))
                else:
                    row.append(np.nan)
            else:
                if names[0] in vacancy and vacancy[names[0]] != None and names[1] in vacancy[names[0]]:
                    row.append(getValue(column, vacancy[names[0]][names[1]]))
                elif names[0] in details and details[names[0]] != None and names[1] in details[names[0]]:
                    row.append(getValue(column, details[names[0]][names[1]]))
                else:
                    row.append(np.nan)
    return row

parsedIds = []
resultRows = []

def getVacancies(): #получаем вакансии с апишки  hh.ru
    specializations = getUrlJson("https://api.hh.ru/specializations")
    for specialization in specializations[0]['specializations']:
        for page in getPages(specialization['id']):
            vacancies = []
            for vacancy in page["items"]:
                if vacancy['id'] not in parsedIds: 
                    vacancies.append(vacancy)
                    parsedIds.append(vacancy['id'])
            data = getVacanciesDetails(vacancies)
            for vacancyData in data:
                row = addVacancyToRow(vacancyData[0], vacancyData[1])
                resultRows.append(row)
            
csvColumns = []
types = {}

for col in mapColumns:
    csvColumns.append(col['column'])
    types[col['column']] = col['dtype']

getVacancies()

dt = pd.DataFrame(resultRows, columns=csvColumns)

dt = dt.astype(types)

dt.to_csv("Vacancies.csv",  na_rep = 'NA', index=True, index_label="", quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig") #запишем вакансии в csv
    
def saveCount(df, col, file):
    row = []
    for item in df[col].unique():
        if not pd.isna(item):
            count = (df[col] == item).sum()
            row.append([item, count])
    newDf = pd.DataFrame(row, columns=[col, "Count"])
    newDf.to_csv(file,  na_rep = 'NA', index=True, index_label="", quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

def saveCount2(df, col1, col2, file):
    row = []
    for item1 in df[col1].unique():
        eq = df.loc[(df[col1] == item1)]
        for item in eq[col2].unique():
            if not pd.isna(item):
                count = (eq[col2] == item).sum()
                row.append([item1, item, count])
    newDf = pd.DataFrame(row, columns=[col1, col2, "Count"])
    newDf.to_csv(file,  na_rep = 'NA', index=True, index_label="", quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

dt = dt.sort_values(["maxSalary", "minSalary"]) #сортируем

dt.to_csv("SortedSalaryVacancies.csv",  na_rep = 'NA', index=True, index_label="", quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig") #записываем

for column in dt.columns:
    if str(dt.dtypes[column]) == 'object':
        dt[column] = dt[column].apply(lambda x: x.lower() if not pd.isna(x) else np.nan)
dt["publishedAt"] = dt["publishedAt"].apply(lambda x: dateutil.parser.parse(x, ignoretz=True))

notNullSalary = dt.loc[(dt['maxSalary'].notnull()) & (dt['minSalary'].notnull())]
salariesGroups = np.array_split(notNullSalary['minSalary'].unique(),10)
lastSalary = 0
daysRows = []
FolderCount = 0

for salaryGroup in salariesGroups:
    FolderCount += 1
    group = notNullSalary.loc[(notNullSalary['minSalary'] > lastSalary) & (notNullSalary['minSalary'] <= max(salaryGroup))]
    FolderName = 'Группа ' + str(FolderCount)
    lastSalary = max(salaryGroup)
    os.mkdir(FolderName)
    saveCount(group, "Name", FolderName + "/Names.csv")
    publishedAt = group["publishedAt"]
    if len(publishedAt)>0:
        publishedAt = (datetime.now() - publishedAt).dt.days
        daysRows.append([FolderName, publishedAt.mean(), publishedAt.min(), publishedAt.max()])
    else:
        daysRows.append([FolderName, np.nan, np.nan, np.nan])
    saveCount(group, "expierence", FolderName + "/expierence.csv")
    saveCount(group, "employment", FolderName + "/employment.csv")
    saveCount(group, "schedule", FolderName + "/schedule.csv")    
    skills = group.loc[group["keySkill"].notnull()]
    if len(skills) != 0 :
        skills = pd.DataFrame(skills)
        skills["keySkill"] = skills["keySkill"].apply(lambda x: x.split(';'))
        skills = skills.explode("keySkill")
        saveCount(skills, "keySkill", FolderName + "/KeySkills.csv")

sDaysresult = pd.DataFrame(daysRows, columns=['Range', "Avg", "Min", "Max"])
sDaysresult.to_csv("Part2.csv",  na_rep = 'NA', index=True, index_label="", quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")
    
saveCount2(dt, "Name", "minSalary", "Part 3 - minSalary.csv")
saveCount2(dt, "Name", "maxSalary", "Part 3 - maxSalary.csv")
saveCount2(dt, "Name", "expierence", "Part 3 - expierence.csv")
saveCount2(dt, "Name", "employment", "Part 3 - employment.csv")
saveCount2(dt, "Name", "schedule", "Part 3 - schedule.csv")

skills = dt.loc[dt["keySkill"].notnull()]
skills = pd.DataFrame(skills)
skills["keySkill"] = skills["keySkill"].apply(lambda x: x.split(';'))
skills = skills.explode("keySkill")
saveCount2(skills, "Name", "keySkill", "KeySkills.csv")