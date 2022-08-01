import pandas as pd

FilePath = "latency88.csv"


df = pd.read_csv(FilePath)

df = df.drop(['Unnamed: 0'], axis=1)

uniqueQueryTime = df['Query Time'].unique()

df = df.drop_duplicates(subset=['Query Time'], keep='first')


dateTimeCol = ['Query Time', 'Frame Start Time', 'Frame End Time',
       'L1 Rad Created', 'L1 Rad Available', 'L2 ABI-L2-MCMIPC Created',
       'L2 ABI-L2-MCMIPC Available', 'L2 ABI-L2-FDCC Created',
       'L2 ABI-L2-FDCC Available', 'L2 ABI-L2-ACMC Created',
       'L2 ABI-L2-ACMC Available', 'L1 Rad Downloaded',
       'L2 ABI-L2-FDCC Downloaded']


for dt_col_name in df.columns:
    if dt_col_name in dateTimeCol:
        df[dt_col_name]= pd.to_datetime(df[dt_col_name])
        df[dt_col_name]= df[dt_col_name].dt.tz_convert('UTC')


start_time, end_time = df['Frame Start Time'].min(), df['Frame Start Time'].max()
print(start_time, end_time)




df.insert(loc=3, column='Image Acquisition Time', value = None)
df['Image Acquisition Time'] = df['Frame End Time'] - df['Frame Start Time']
df['Image Acquisition Time'] = df['Image Acquisition Time'].dt.seconds

df.insert(loc=4, column='Blank1', value = '')


df.insert(loc=7, column='L1 Availablity Time', value = None)
df['L1 Availablity Time'] = df['L1 Rad Available'] - df['Frame End Time']
df['L1 Availablity Time'] = df['L1 Availablity Time'].dt.seconds

df.insert(loc=8, column='Blank2', value = '')


df.to_csv('trackLatency3.csv', index=False)


