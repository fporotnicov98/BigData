import pandas as pd

#1
data = pd.read_csv('brooklyn_sales_map.csv', sep=',')
searchCategory = data['building_class_category'].unique()

#2
for columnsName in searchCategory:
    SearchResult = data.loc[data['building_class_category'] == columnsName]
    SearchResult.to_csv(str(columnsName).replace("/", " ") + '.csv')

#3
columnsName = data.select_dtypes(include='number').columns
for column in columnsName:
    print('missing: ' + str(data[column].isna().sum()))
    print('mean: ' + str(data[column].mean()))
    print('median: ' + str(data[column].median()))
    print('max: ' + str(data[column].max()))
    print('min: ' + str(data[column].min()))
    print('unique: ' + str(len(data[column].unique())))

#4
for column in searchCategory:
    try:
        part = data['building_class_category'].value_counts()[column] / len(data['building_class_category'].values)
        print(column + ': доля от выборки (building_class_category) = ' + str(part))
    except:
        continue

#5
normalize = (data._get_numeric_data() - data._get_numeric_data().min()) / (data._get_numeric_data().max() - data._get_numeric_data().min())
print(normalize)