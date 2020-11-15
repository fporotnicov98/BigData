import pandas as pd
import numpy as np
import re
from lxml import etree

root = etree.parse("OBV_full.xml")

columns = [] #заголовки
data = [] #запись
row = []

vacancies = root.find('vacancies').getiterator('vacancy')
for vacancy in vacancies:
  tags = vacancy.getiterator()
  for i in tags:
    if i.tag not in columns:
      columns.append(i.tag)
      
columns.remove('address')
columns.remove('addresses')
columns.remove('salary')
columns.append('salaryMin')
columns.append('salaryMax')


indexColumn = {}
for i in range(len(columns)):
  indexColumn[columns[i]] = i


def addRow(row, column_name, value):
  index = indexColumn[col_name]
  if not pd.isna(row[index]):
    row[index] = row[index] + ':'+ value
  else:
    row[index] = value

salaryMin = re.compile("min (\d+)", re.IGNORECASE) # минммальная зп
salaryMax = re.compile("max (\d+)", re.IGNORECASE) # максимальная зп

vacancies = root.find('vacancies').getiterator('vacancy')

for vacancy in vacancies:
  if len(row)!=0:
    data.append(row)
  row = [np.nan] * len(columns)
  tags = vacancy.getiterator()
  for i in tags:
    if i.text and not i.text.isspace() and i.tag!='vacancy' and i.tag!='address' and i.tag!='addresses':
      if i.tag == 'job-name':
        addRow(row, i.tag, i.text.replace(',',';'))
      elif i.tag == 'salary':
        sMin=salaryMin.search(i.text)
        sMax=salaryMax.search(i.text)
        sMin = sMin.group(1) if sMin else np.nan
        sMax = sMax.group(1) if sMax else np.nan
        addRow(row, 'salaryMin', sMin)
        addRow(row, 'salaryMax', sMax)
      else:
        addRow(row, i.tag, i.text)
if row:
  data.append(row)

result = pd.DataFrame(data, columns=columns)
result.to_csv("Result.csv", na_rep = '*')

