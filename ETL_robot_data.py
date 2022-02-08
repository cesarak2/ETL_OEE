# Author: Cesar Krischer
# 02/03/2022 – initial commit
# ETL for robot data aquisition. Start with RB-HA-01.


import pandas as pd
import numpy as np
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
# READING ALL FILE(S) TO BE USED #
# = = = = = = = = = = = = = = = =#
# DEFINING FILE(S) TO BE OPPENED #
file_path_RejectData = get_input_file_name('rb-ha-01', 'RBHA01', 'RejectData')
file_path_RobotFailure = get_input_file_name('rb-ha-01', 'RBHA01', 'RobotFailure')
file_path_RobotStoppage = get_input_file_name('rb-ha-01', 'RBHA01', 'RobotStoppage')

# some columns currently don't have titles, hence are here called reject17-22
reject_data_log_columns = ['DateTime','Part #','Lot #','Lot Count','Parts Made','Cable Rejects',
'Swager Misses','FitCut Misses','Lead Rejects','Tail Rejects','HypoRejects','Stuck Rejects',
'OL Rejects #','UZ Rejects','FL Rejects','Knots','ENFORCER!','Bad Hypo Insert', 'FL OL Rejects',
'Cam Faults','Ejected Ftgs', 'StakePulls', 'StakePullUnder', 'TailSlideJog', 'TailUnstick',
'TailSlideJogRejects', 'TailUnstickRejects']
RejectData_raw = pd.read_csv(file_path_RejectData, names=reject_data_log_columns, skiprows=1)
RobotFailure_raw = pd.read_csv(file_path_RobotFailure)
RobotStoppage_raw = pd.read_csv(file_path_RobotStoppage)
planned_downtime = pd.read_csv('planned_downtime.csv')

# CONVERT TO DATETIME FORMAT
RejectData_raw['DateTime'] = pd.to_datetime(RejectData_raw['DateTime'])
RobotFailure_raw['Rst DateTime'] = pd.to_datetime(RobotFailure_raw['Rst DateTime'])
RobotFailure_raw['LPM DateTime'] = pd.to_datetime(RobotFailure_raw['LPM DateTime'])
RobotStoppage_raw['DateTime'] = pd.to_datetime(RobotStoppage_raw['DateTime'])

# = = = = = = = = = = #
# PANDAS MANIPULATION #
# = = = = = = = = = = #
# slices the rejects columns, sums them rowwise and assigns the resuls to RejectDat_raw 
rejects_df = RejectData_raw.iloc[:, lambda columns: np.arange(5,27)]
RejectData_raw['Total Rejects'] = rejects_df.sum(axis=1)


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# LOOKUP TABLE FOR STOPPAGES CLASSIFICATIONS (SCHEDULED DOWNTIME) #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# creating a dictionary of tuples in the format "('Major', 'Minor'): ['planned or non-planned']"
# columns major and minor will be compared against tuples to retrieve if planned or non-planned downtime
stoppages_raw_tuples = [((getattr(row, 'majorStoppageReason'), getattr(row, 'minorStoppageReason')),
      getattr(row, 'classificationStoppageReason') ) for row in planned_downtime.itertuples(index=False)]
stoppages=dict()
for student,score in stoppages_raw_tuples:
    stoppages.setdefault(student, []).append(score)
#stoppages[('Nonerror','Accidentallightcurtain')]

# CREATING COLUMNS FOR DOWNTIME ## CHANGE THIS SECTION TO USE A LAMBDA FUNCTION INSTEAD OF LOOP
RobotFailure_raw['Downtime Type'] = 0
RobotFailure_raw['Minutes_down_at_hour'] = 0
RobotFailure_raw['remainder'] = 0
for i in range(0,len(RobotFailure_raw)):
    try: #stoppages[('Engineering','Code changes')] returns 'planned'
        RobotFailure_raw['Downtime Type'][i] = stoppages[(RobotFailure_raw['Major'][i],RobotFailure_raw['Minor0'][i])]
    except:
        RobotFailure_raw['Downtime Type'][i] = 'no valid code'

# POPULATE COLUMNS FOR DOWNTIME MEASUREMENT
RobotFailure_raw['time_per_stop'] = RobotFailure_raw['Rst DateTime'] - RobotFailure_raw['LPM DateTime']




RobotFailure_raw['Minutes down at the hour'] = np.where(
                                                        RobotFailure_raw['LPM DateTime'].dt.minute + 
                                                        np.floor(RobotFailure_raw['time_per_stop']/np.timedelta64(1, 'm')) +
                                                        np.floor(RobotFailure_raw['time_per_stop']/np.timedelta64(1, 'h'))*60 >= 60,
                                                            'overflow',
                                                        RobotFailure_raw['Rst DateTime'].dt.minute - 
                                                        RobotFailure_raw['LPM DateTime'].dt.minute)

RobotFailure_raw['max minutes to be absorbed'] = np.where(
                                                        RobotFailure_raw['LPM DateTime'].dt.minute + 
                                                        np.floor(RobotFailure_raw['time_per_stop']/np.timedelta64(1, 's'))/60 >= 60,
                                                        60 - RobotFailure_raw['LPM DateTime'].dt.minute,
                                                        RobotFailure_raw['Rst DateTime'].dt.minute -
                                                        RobotFailure_raw['LPM DateTime'].dt.minute)


RobotFailure_raw['remainder left for future hours'] = np.where(
                                                        RobotFailure_raw['Minutes down at the hour'] != 'overflow',
                                                        '0',
                                                        np.floor(RobotFailure_raw['time_per_stop']/np.timedelta64(1, 's')/60) + -
                                                        (60-RobotFailure_raw['LPM DateTime'].dt.minute))

#RobotFailure_raw['Minutes down at the hour'].head(30)
#RobotFailure_raw['remainder left for future hours'].head(30)
#RobotFailure_raw.to_csv('erase_view_only_RobotFailure_raw.csv')

per_hour = pd.DataFrame({'carried': [], 'generated': [], 'used' : [], 'left':[], 'time running':[]})
per_hour = per_hour.reindex(pd.date_range(start=RobotFailure_raw['LPM DateTime'].round('H').min(),
                                                  end=RobotFailure_raw['LPM DateTime'].round('H').max(),
                                                  freq='1H'))
RejectData_raw
drange=pd.date_range(per_hour.index.min(),per_hour.index.max())
[RobotFailure_raw[(RobotFailure_raw['LPM DateTime'] <= ud) & (RobotFailure_raw['LPM DateTime'] >= ud)]['Minutes down at the hour'].sum() for ud in drange]


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





# https://stackoverflow.com/questions/19913659/pandas-conditional-creation-of-a-series-dataframe-column