# Author: Cesar Krischer
# 02/03/2022 – initial commit
# ETL for robot data aquisition. Start with RB-HA-01.


import pandas as pd
import os
import matplotlib.pyplot as plt
import datetime as dt
import time
import fnmatch
import datetime


def find(pattern, path):
    '''
    looks for all files in a folder that contain a certain patter;
    use a blank list to append all files to be read and worked with
    input: pattern as RE and path to scan files
    '''
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


# it needs to open: 
#   RejectDataLogs 
#       \\rb-ha-01\LocalShare\RuntimeData\RobotFailureLogs\RBHA01_RobotFailureLog_2022.csv
#   RobotFailureLogs
#       \\rb-ha-01\LocalShare\RuntimeData\RobotStoppageLogs\2022\RBHA01_RobotStoppageLog_February.csv


def get_input_file_name(robot_folder_name='rb-ha-01', file_name_prefix='RBHA01', metric_folder='RobotFailureLogs',
                    day_folder=str(datetime.datetime.now().strftime('%B')),
                    month_folder=str(datetime.datetime.now().strftime('%B')),
                    year_folder=datetime.datetime.now().strftime('%Y')):
    '''
    returns the path for the file to be oppened.
    Arguments: robot_folder_name, file_name_prefix, metric_folder, day_folder, month_folder, year_folder.
    Returns: path.
    '''
    windows_separator_for_network_access = '\\' # needs \\ to open files on Windows. To be updated to be more generic
    # metrics have different folders structures, stored in the _directories_ dictionary. Retrieve them for path retrieval
    if directories[metric_folder] == 'YMF_D': 
        file_path = os.sep.join([windows_separator_for_network_access, 
                        robot_folder_name, 'LocalShare', 'RuntimeData',
                        metric_folder + 'Logs', #folder ends with 'Logs' and files end with 'Log'
                        year_folder, month_folder, file_name_prefix + 
                        '_' + metric_folder + 'Log_' + datetime.datetime.now().strftime('%d') + '.csv'])

#   RejectDataLogs 
#       \\rb-ha-01\LocalShare\RuntimeData\RobotFailureLogs\RBHA01_RobotFailureLog_2022.csv

    elif directories[metric_folder] == 'F_Y': #RobotFailureLogs
        file_path = os.sep.join([windows_separator_for_network_access, 
                        robot_folder_name, 'LocalShare', 'RuntimeData',
                        metric_folder + 'Logs', #folder ends with 'Logs' and files end with 'Log'
                        #year_folder, 
                        #month_folder,
                        file_name_prefix + '_' + metric_folder + 'Log_' +
                        year_folder + '.csv'])

    elif directories[metric_folder] == 'YF_M': #RobotStoppageLogs
        file_path = os.sep.join([windows_separator_for_network_access, 
                        robot_folder_name, 'LocalShare', 'RuntimeData',
                        metric_folder + 'Logs', #folder ends with 'Logs' and files end with 'Log'
                        year_folder,
                        file_name_prefix + '_' + metric_folder + 'Log_' + month_folder + '.csv'])
    return file_path


directories = {
# Y = year, M = month; 
# F_D = file per day, F_M = file per month, F_Y = file per year 
'AllToolLives':'YF_M',
'CableCutTime': 'YMF_D',
'ExtraEvents': 'YMF_D',
'FittingLength': 'YMF_D',
'FtgLocation': 'YMF_D',
'OverallLength': 'YMF_D',
'PressInsertHeight': 'YMF_D',
'RejectData': 'F_Y',
'RobotFailure': 'F_Y',
'RobotStoppage': 'YF_M',
'StartPress': 'YF_M',
'UncrimpedZone': 'YMF_D'}


# = = = = = = = = = = = = = = = =#
# DEFINING FILE(S) TO BE OPPENED #
# = = = = = = = = = = = = = = = =#
file_path_RobotFailure = get_input_file_name('rb-ha-01', 'RBHA01', 'RobotFailure')
file_path_RobotStoppage = get_input_file_name('rb-ha-01', 'RBHA01', 'RobotStoppage')

# = = = == = = = = = = = = = = = = = = = = = #
# READING FILE(S) AND CONVERTING TO DATETIME #
# = = = == = = = = = = = = = = = = = = = = = #
RobotFailure_raw = pd.read_csv(file_path_RobotFailure)
RobotStoppage_raw = pd.read_csv(file_path_RobotStoppage)

RobotFailure_raw['Rst DateTime'] = pd.to_datetime(RobotFailure_raw['Rst DateTime'])
RobotStoppage_raw['DateTime'] = pd.to_datetime(RobotStoppage_raw['DateTime'])

# = = = = = = = = = = #
# PANDAS MANIPULATION #
# = = = = = = = = = = #
'''
Metrics:
    tendency of production per shift/day (beg shift to time) -> TABLE 1, as is
    total production per shift/day                           -> TABLE 2, groupby shift
    average production per shift                             -> TABLE 2, groupby shift
Shifts (from Excel):
    1st = if >9 AND <18     09:01 to 17:59      9h      10-6
    2nd = if >17 OR <2      18:00 to 01:59      8h      6-2
    3rd = else              02:00 to 06:00      4h      2-6
    1A  = if >6 AND <10     06:01 to 09:00      3h      6-10

Folders:                    frequency of files, saving interval
        AllToolLivesLogs        per month, real time
    DB  CableCutTimeLogs        per day, real time
        ExtraEventsLogs         per day, real time
    ML  FittingLengthLogs       per day, real time
    ML  FtgLocationLogs         per day, real time
    ML/DB OverallLengthLogs     per day, real time
    ML  PressInsertHeightLogs   per day, real time
        RejectDataLogs          (per year, once per hour) 
    DB  RobotFailureLogs        per year, real time
    DB  RobotStoppageLogs       per month, real time
        StartPressLogs          per month, real time
    ML  UncrimpedZoneLogs       per day, real time

Steps:
    Import files
    Transform 
    tarnsform import step into while true loop 
'''

# CREATE SHIFT COLUMN
RB20_RejectDataLog_2022['shift'] = 0  
# POPULATE SHIFTS (MIMICS EXCEL)
RB20_RejectDataLog_2022.loc[(RB20_RejectDataLog_2022.DateTime.dt.hour >= 0), 'shift'] = int(2)
RB20_RejectDataLog_2022.loc[(RB20_RejectDataLog_2022.DateTime.dt.hour >= 2), 'shift'] = int(3)
RB20_RejectDataLog_2022.loc[(RB20_RejectDataLog_2022.DateTime.dt.hour >= 6), 'shift'] = '1A'
RB20_RejectDataLog_2022.loc[(RB20_RejectDataLog_2022.DateTime.dt.hour >= 10), 'shift'] = int(1)
RB20_RejectDataLog_2022.loc[(RB20_RejectDataLog_2022.DateTime.dt.hour >= 18), 'shift'] = int(2)

#RB20_RejectDataLog_2022[RB20_RejectDataLog_2022['Parts Made'] > 0].groupby(['shift']).mean()
#RB20_RejectDataLog_2022.groupby(['shift']).sum()
#RB20_RejectDataLog_2022.groupby(by=RB20_RejectDataLog_2022.DateTime.dt.date).mean()
# this one:
pivot_per_shift = RB20_RejectDataLog_2022.groupby([RB20_RejectDataLog_2022.DateTime.dt.date, 'shift']).count()
pivot_per_shift = pivot_per_shift.drop('Lot Count', 1)

pivot_per_hour = RB20_RejectDataLog_2022.groupby([RB20_RejectDataLog_2022.DateTime.dt.hour]).count()
pivot_per_hour_summary = pd.DataFrame(pivot_per_hour['Lot Count'])

'''
target_per_hour = pass
pivot_per_hour_summary.DateTime.dt
'''




plt.scatter(RB20_RejectDataLog_2022['DateTime'], RB20_RejectDataLog_2022['Lot Count'])
plt.scatter(pivot_per_shift['DateTime'], RB20_RejectDataLog_2022['Insert Height q'])
plt.bar(pivot_per_shift['Insert Height'])
plt.plot(pivot_per_shift['Insert Height'], pivot_per_shift['DateTime'])
pivot_per_shift.columns
#df = df.replace(np.nan, 0)
#dfg = df.groupby(['home_team'])['arrests'].mean()

pivot_per_shift.plot(kind='bar', title='', ylabel='count',
         xlabel='', figsize=(10, 8))