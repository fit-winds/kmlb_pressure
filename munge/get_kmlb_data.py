# get_kmlb_data.py
# bryan holman // v0.2 // 20171005

import pandas as pd
import numpy as np
import datetime as dt
import shutil
import pytz

# input integers of current year and month, return strings of next month to grab data
def get_next_month(yr, mo):
    mo_next = dt.datetime(yr, mo, 1, 0, 0, 0) + dt.timedelta(weeks = 5)
    year = str(mo_next.year)
    if mo_next.month < 10:
        month = '0' + str(mo_next.month)
    else:
        month = str(mo_next.month)
    return [year, month]

# grab data for the next month on the list
month_data = pd.read_csv('data/processed_months.csv')
mo = month_data['month'][-1:].values[0]
yr = month_data['year'][-1:].values[0]
year, month = get_next_month(yr, mo)
print('Grabbing data for ' + month + '/' + year + ' ...')
data_url = ('ftp://ftp.ncdc.noaa.gov/pub/data/asos-onemin/6406-' + year +
    '/64060KMLB' + year + month + '.dat')
column_names = ['Station', 'DateTime', 'Note1', 'Note2', 'Note3', 'Pres1',
                'Pres2', 'Pres3', 'Tmp', 'Dwpt']

# only continue if the data for this month are available
try:
    kmlb_data = pd.read_fwf(data_url, header=None, names=column_names)
except:
    error_msg = ('KMLB data for ' + month + '/' + year +
                 ' are not available yet. Please try again later.')
    print(error_msg)
    exit()

print('Processing data ...')
# drop columns we don't need
kmlb_data = kmlb_data.drop(['Station', 'Note1', 'Note2', 'Note3', 'Tmp', 'Dwpt'], axis=1)

# convert to time series in UTC
try:
    kmlb_data['DateTime'] = pd.to_datetime(kmlb_data['DateTime'].str[3:15])
# some of the files' fixed width nature isn't quite right ... this fixes that
except ValueError:
    print('Inferring columns failed. Attempting to do so manually ...')
    kmlb_data = pd.read_fwf(data_url, header=None, dtype=object,
                            colspecs=[[13, 25], [70, 76], [78, 84], [86, 92]],
                            names=['DateTime', 'Pres1', 'Pres2', 'Pres3'])
    kmlb_data['DateTime'] = pd.to_datetime(kmlb_data['DateTime'])
kmlb_data = kmlb_data.set_index('DateTime')
kmlb_data = kmlb_data.tz_localize('EST').tz_convert('UTC')

# make sure each column is a float
for col in ['Pres1', 'Pres2', 'Pres3']:
    if not kmlb_data[col].dtype == np.float64:
        kmlb_data[col] = pd.to_numeric(kmlb_data[col], errors='coerce')

# remove duplicate values and sample every five minutes
kmlb_data5 = kmlb_data[~kmlb_data.index.duplicated(keep='first')].asfreq('5Min')
del(kmlb_data, data_url, column_names, col)

# convert from inHg to Pa and add 200 Pa to get to sea level
kmlb_data5['Pressure (Pa)'] = kmlb_data5.mean(axis=1, skipna=False) * 3386.38816
kmlb_data5['MSLP (Pa)'] = kmlb_data5['Pressure (Pa)'] + 200

# drop original pressure column_names
kmlb_data5 = kmlb_data5.drop(['Pres1', 'Pres2', 'Pres3'], axis=1)

# write csv file to disk with proper date format (per Peyman's liking) and
# fill missing values with 999999
print('Writing to disk ...')
filename = 'data/KMLB' + year + month + '.csv'
kmlb_data5.to_csv(filename, date_format='%d-%b-%Y %H:%M:%S',
                  float_format='%.6f', na_rep='999999')

# now let's append this new data to all KMLB data
print('Write successful. Appending to data all data ...')
print('Backing up KMLB_all.csv ...')
shutil.copy('data/KMLB_all.csv', 'data/KMLB_all_old.csv')

# load and preprocess all KMLB data
all_data = pd.read_csv('data/KMLB_all.csv', na_values='999999')
all_data['DateTime'] = pd.to_datetime(all_data['DateTime'], format='%d-%b-%Y %H:%M:%S')
all_data = all_data.set_index('DateTime')
all_data = all_data.tz_localize('UTC')

# append new data to all data and remove duplicates. This is important in case
# we accidentally just processed a month that is already contained in the data
concat_data = pd.concat([all_data, kmlb_data5])
concat_data = concat_data[~concat_data.index.duplicated(keep='first')]

# now write this to disk
concat_data.to_csv('data/KMLB_all.csv', date_format='%d-%b-%Y %H:%M:%S',
                   float_format='%.6f', na_rep='999999')

# update processed_data.csv
month_data.loc[len(month_data)] = [int(year), int(month)]
month_data.to_csv('data/processed_months.csv', index=False)

# commit to github if applicable
from git import Repo
import os
repo = Repo(os.getcwd())
file_list = [filename, 'data/KMLB_all_old.csv', 'data/KMLB_all_old.csv']
commit_message = 'Add data for ' + month + '/' + year
repo.index.add(file_list)
repo.index.commit(commit_message)
origin = repo.remote('origin')
origin.push()
