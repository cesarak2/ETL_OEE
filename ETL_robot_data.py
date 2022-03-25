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
#from datetime import datetime, timedelta



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


#https://stackoverflow.com/questions/48937900/round-time-to-nearest-hour-python
def round_to_next_hour(t):
    '''
    Rounds DateTime to next hour (ceiling function)
    input: timedelta 
    output: timedelta 
    '''
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +datetime.timedelta(hours=1))




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

robots = {
'RBHA01': ['RBHA01', 'rb-ha-01'],
'RBHA02': ['RBHA02', 'rb-ha-02'],
'RBHA03': ['RBHA03', 'rb-ha-03']
}



# = = = = = = = = = = = = = = =#
# READS ALL FILE(S) TO BE USED #
# = = = = = = = = = = = = = = =#
# DEFINING FILE(S) TO BE OPPENED #



selected_robot = 'RBHA02'
robot_name = robots[selected_robot][0]
robot_folder = robots[selected_robot][1]

file_path_RejectData = get_input_file_name(robot_folder, robot_name, 'RejectData')
file_path_RobotFailure = get_input_file_name(robot_folder, robot_name, 'RobotFailure')
file_path_RobotStoppage = get_input_file_name(robot_folder, robot_name, 'RobotStoppage')

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


# = = = = = = = = = = = = = = = = = #
# CALCULATES TOTAL REJECTS PER HOUR #
# = = = = = = = = = = = = = = = = = #
''' OBSOLETE WHEN STOPPED SUMMING ALL COLUMNS FOR REJECTS
# slices the rejects columns, sums them rowwise and assigns the resuls to RejectDat_raw 
#columns_rejects = np.arange(5,len(RejectData_raw.columns))

#rejects_df = RejectData_raw.iloc[:, lambda columns: columns_rejects] #0:date,1:part, 2:lot#, 3:lotcount, 4:partsmade
#RejectData_raw.iloc[:, lambda columns: np.arange(5,-1)]
#RejectData_raw['Total Rejects'] = rejects_df.sum(axis=1) # if all columns were equally counted
'''
RejectData_raw['Total Rejects'] = RejectData_raw['Cable Rejects'] + RejectData_raw['Swager Misses'] + \
                                  RejectData_raw['FitCut Misses'] + RejectData_raw['Lead Rejects']*.75 + \
                                  RejectData_raw['Tail Rejects']*.75 + RejectData_raw['HypoRejects']*.25 + \
                                  RejectData_raw['Stuck Rejects'] + RejectData_raw['OL Rejects #'] + \
                                  RejectData_raw['UZ Rejects'] + RejectData_raw['FL Rejects'] + \
                                  RejectData_raw['Bad Hypo Insert']*.75 + RejectData_raw['FL OL Rejects'] + \
                                  RejectData_raw['Cam Faults'] + RejectData_raw['Ejected Ftgs']*.25


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# LOOKUP TABLE FOR STOPPAGES CLASSIFICATIONS (SCHEDULED DOWNTIME) #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# creating a dictionary of tuples in the format "('Major', 'Minor'): ['planned or non-planned']".
# In the target dataset of stoppages, the columns major and minor will be compared against those tuples
# to retrieve if that stoppage was planned or non-planned downtime
stoppages_raw_tuples = [((getattr(row, 'majorStoppageReason'), getattr(row, 'minorStoppageReason')),
      getattr(row, 'classificationStoppageReason') ) for row in planned_downtime.itertuples(index=False)]
stoppages=dict()
for major,minor in stoppages_raw_tuples:
    stoppages.setdefault(major, []).append(minor)

# CREATING COLUMNS FOR DOWNTIME ## (Open action: CHANGE THIS SECTION TO USE A LAMBDA FUNCTION INSTEAD OF LOOP)
RobotFailure_raw['Downtime Type'] = 0
for i in range(0,len(RobotFailure_raw)):
    try: #stoppages[('Engineering','Code changes')] returns 'planned'
        [RobotFailure_raw['Downtime Type'][i]] = stoppages[(RobotFailure_raw['Major'][i],RobotFailure_raw['Minor0'][i])]
    except (KeyError):
        RobotFailure_raw['Downtime Type'][i] = 'no valid code'

# POPULATE COLUMNS FOR DOWNTIME MEASUREMENT
RobotFailure_raw['time_per_stop'] = RobotFailure_raw['Rst DateTime'] - RobotFailure_raw['LPM DateTime']

# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# CLEANS ENTRIES FOR PROCESSING – DELETES REPEATED ROWS & ADJUSTS TIMES FOR SMOOTH TRANSITION #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# drops all but the last repeated consecutive rows, when the stoppage starts and 
# the type of stoppage (planned vs. unplanned) are the same. The sttopage on the last row always ends later
# hence no time is lost by dropping the previous rows.

for i in range(1,len(RobotFailure_raw)-1):
    if RobotFailure_raw['LPM DateTime'][i] == RobotFailure_raw['LPM DateTime'][i+1] and RobotFailure_raw['Downtime Type'][i] == RobotFailure_raw['Downtime Type'][i+1]:
        RobotFailure_raw.drop(i, axis=0, inplace=True)
#        print('drops row ', i)
RobotFailure_no_duplicates = RobotFailure_raw.reset_index(drop=True)

# when they start at the same time, replace the new start with the previous stop, so the whole period the robot
# didn't work will be a continuous interval of stoppages
# In some cases, there is more than 2 changes in planned/unplanned starting at the same time,
# which means the 2nd time will copy the 1st one, but by the time the 3rd is checked with the 2nd,
# the 2nd was already changed they will be different. This would lead to a smaller LPM date than the previous.
# a check for <= accounts for both cases.
for i in range(1,len(RobotFailure_no_duplicates)):
    if RobotFailure_no_duplicates['LPM DateTime'][i] <= RobotFailure_no_duplicates['LPM DateTime'][i-1]:
#        print('rows: ', RobotFailure_no_duplicates['LPM DateTime'][i])
        RobotFailure_no_duplicates['LPM DateTime'][i] = RobotFailure_no_duplicates['Rst DateTime'][i-1]
'''
RobotFailure_no_duplicates[390:400]
'''
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =#
# SPLITS LONG ENTRIES (WHEN OVERFLOWS TO THE NEXT HOUR) INTO TWO #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =#
# When any interval goes after the hour (e.g. 7:50 to 9:10), split it in two:
#   * one from the first start to the first whole hour;
#   * one from the first whole hour to the end.
# One split is enough, as only one event will cross the hour. Later hours are going to be either 60 minutes or summed with other entries.

RobotFailure_no_duplicates_split = RobotFailure_no_duplicates.copy()
'''
11:50 | 13:50 (original row)
      V
11:50 | 12:00 (transforms row)
12:00 | 13:50 (adds row)
'''
for i in range(1,len(RobotFailure_no_duplicates_split)): #range from 1 to n-1 because it uses the previous value to compare
    #if the stoppage goes until next hour. If starts and ends on the same hour, do nothing.
    if RobotFailure_no_duplicates_split['LPM DateTime'][i].hour != RobotFailure_no_duplicates_split['Rst DateTime'][i].hour:
#        print('converting rows: ', RobotFailure_no_duplicates_split['LPM DateTime'][i])
        # adds new line: starts on first hour after overflow, ends on original value
        RobotFailure_no_duplicates_split.loc[i +0.5] = RobotFailure_no_duplicates_split['Rst DateTime'][i], \
            '','','','','','','','','','','','','', \
            round_to_next_hour(RobotFailure_no_duplicates_split['LPM DateTime'][i]), \
            '', \
            RobotFailure_no_duplicates_split['Downtime Type'][i],''
        # replaces the real end time to the next round hour, and the next line starts on that hour and goes to the real end
        RobotFailure_no_duplicates_split['Rst DateTime'].iloc[i] = round_to_next_hour(RobotFailure_no_duplicates_split['LPM DateTime'][i])
RobotFailure_no_duplicates_split = RobotFailure_no_duplicates_split.sort_index().reset_index(drop=True)

RobotFailure_no_duplicates_split.to_csv(robot_name + '_view_only_insert_within_hours.csv')


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# CREATES A FAKE EVENT OF 0 SEC AT THE LAST ROW FOR MAKING THE RANGE FOR RESAMPLE #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = # 
# When resampling, the last value in the df is going to be the final range of the new array.
# By adding a fake value of 0 seconds, it forces the new array to finish at that hour;
# as it necessaryly happens after the last real value, it forces the function to count
# time to be in range.

# duplicates the last row
RobotFailure_no_duplicates_split = RobotFailure_no_duplicates_split.append(RobotFailure_no_duplicates_split.iloc[[-1]], ignore_index=True)

# copies the value from Rst to LPM, to create a zero seconds event
RobotFailure_no_duplicates_split.iloc[-1, RobotFailure_no_duplicates_split.columns.get_loc('LPM DateTime')] = \
    RobotFailure_no_duplicates_split.iloc[-1, RobotFailure_no_duplicates_split.columns.get_loc('Rst DateTime')]


# adds columns to RobotFailure_raw:
#    'Minutes down at the hour' is the maximum number of minutes that stoppage could fit inside that hour.
#       If the the stoppage overflows to the next hour, it is set to '0', hence its maximum value is 59.
#    'max minutes to be absorbed' is similar to 'Minutes down at the hour', but it is set for all minutes
#       left, without a maximum.
#    'remainder left for future hours' is the difference between the total amount of time for that stoppage
#       and how much it can still be used on that very same hour
#del(RobotFailure_raw)
RobotFailure_raw=RobotFailure_no_duplicates_split.copy()

# POPULATE COLUMNS FOR DOWNTIME MEASUREMENT
RobotFailure_raw['time_per_stop'] = RobotFailure_raw['Rst DateTime'] - RobotFailure_raw['LPM DateTime']

RobotFailure_raw['Minutes down at the hour'] = np.where(
                                                        RobotFailure_raw['LPM DateTime'].dt.minute + RobotFailure_raw['LPM DateTime'].dt.second/60 +
                                                        RobotFailure_raw['time_per_stop']/np.timedelta64(1, 'm') >= 60,
                                                            'overflow',
#                                                        0, # taking only the minutes doesn't return enought granularity. Taking the decimal from sec
                                                        (RobotFailure_raw['Rst DateTime'].dt.minute + RobotFailure_raw['Rst DateTime'].dt.second / 60)- 
                                                        (RobotFailure_raw['LPM DateTime'].dt.minute + RobotFailure_raw['LPM DateTime'].dt.second / 60))

RobotFailure_raw['max minutes to be absorbed'] = np.where(
                                                        RobotFailure_raw['LPM DateTime'].dt.minute + RobotFailure_raw['LPM DateTime'].dt.second/60 +
                                                        RobotFailure_raw['time_per_stop']/np.timedelta64(1, 'm') >= 60, #
                                                        60 - (RobotFailure_raw['LPM DateTime'].dt.minute + RobotFailure_raw['LPM DateTime'].dt.second/60) ,
                                                        (RobotFailure_raw['Rst DateTime'].dt.minute + RobotFailure_raw['Rst DateTime'].dt.second/60) -
                                                        (RobotFailure_raw['LPM DateTime'].dt.minute + RobotFailure_raw['LPM DateTime'].dt.second/60))


RobotFailure_raw['remainder left for future hours'] = np.where(
                                                        RobotFailure_raw['Minutes down at the hour'] != 'overflow',
#                                                        RobotFailure_raw['Minutes down at the hour'] > 0.0,
                                                        '0',
                                                        RobotFailure_raw['time_per_stop']/np.timedelta64(1, 's')/60 -
                                                        (60-(RobotFailure_raw['LPM DateTime'].dt.minute + RobotFailure_raw['LPM DateTime'].dt.second/60)))

RobotFailure_raw[145:150]
RobotFailure_reordered = RobotFailure_raw.columns.tolist()
RobotFailure_reordered = RobotFailure_raw[['LPM DateTime', 'Rst DateTime',  'Downtime Type', 'time_per_stop',
                                            'Minutes down at the hour', 'max minutes to be absorbed',
                                            'remainder left for future hours', 'Detail', 'Major', 'Minor0', 'Part#', 'Lot#']]

#RobotFailure_no_duplicates.to_csv(robot_name + '_view_only_RobotFailure_no_duplicates.csv')
#RobotFailure_raw.head(15)


RobotFailure_reordered.to_csv(robot_name + '_view_only_RobotFailure_reordered(2).csv')
RejectData_raw.to_csv(robot_name + '_view_only_RejectData_raw(1).csv')
RobotFailure_raw.to_csv(robot_name + '_view_only_RobotFailure_raw.csv')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
'''
=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/
=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/
=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/
'''
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



def expand_per_hours(orignal_series):
    #fit_in_hour = [0] * len(orignal_series)
    fit_in_hour = [0] * orignal_series.size
    #for i in range(len(orignal_series)):
    for i in range(orignal_series.size):
        a = divmod(int(orignal_series[i]), 60)
        if a[0] == 0: # if doesn't overflow to the next hour, set the value for the hour
            try:
                fit_in_hour[i] = fit_in_hour[i] + orignal_series[i]
            except KeyError:
                fit_in_hour[i] = orignal_series[i]
        else:
            for ite in range(i, i+a[0]):
                try:
                    fit_in_hour[ite] = fit_in_hour[ite] + 60
                except KeyError:
                    fit_in_hour[ite] = 60
            try:
                fit_in_hour[i+a[0]] = fit_in_hour[i+a[0]] + a[1]
            except KeyError:
                fit_in_hour[i+a[0]] = a[1]
    return pd.Series(fit_in_hour)


#df = pd.read_csv('\\\\ALAN1\ckrischer\\71_ETL_robos_data\\rb-ha-01\\RBHA02_view_only_RobotFailure_reordered(2).csv')
df = RobotFailure_reordered.copy()

df['LPM DateTime'] = pd.to_datetime(df['LPM DateTime'])
df['Rst DateTime'] = pd.to_datetime(df['Rst DateTime'])

df['hour'] = df['LPM DateTime'].apply(lambda x: x.hour)
#df['diff'] = df['Rst DateTime'] - df['LPM DateTime']
#df['diff_min'] = df['diff'].apply(lambda x: divmod(x.seconds, 60)[0]+x.days*24*60) #seconds up to a day, days after that
df['diff_min'] = round(df['time_per_stop'].apply(lambda x: x.seconds/60 + x.days*24*60)) #seconds up to a day, days after that

df.tail()


df['DateTime'] = df['LPM DateTime'].apply(lambda x: x.floor('H'))


#adict = df[['hour', 'diff_min']].groupby(['hour']).sum().to_dict()
#adict = df[['DateTime', 'diff_min']].groupby(['DateTime']).sum().to_dict()
alist = df[['DateTime', 'diff_min']].groupby(['DateTime']).sum()
alist = df[['DateTime', 'diff_min']].groupby(['DateTime']).sum()
alistnoround = df[['DateTime', 'diff_min']].groupby(['DateTime']).sum()
blist = alist['diff_min'].resample('H').sum()
list_shift_corrected = blist #clist shifts index by one to match other files
list_shift_corrected.index = list_shift_corrected.index+datetime.timedelta(hours=1)
#clist.to_csv(robot_name + '00_clist.csv')

minutes_per_hour = expand_per_hours(list_shift_corrected)
#minutes_per_hour.set_axis = blist.axes

df_minutes = pd.DataFrame(minutes_per_hour, columns=['total_down_minutes'])
df_minutes.index = list_shift_corrected.axes
df_minutes.head(20)
a = df_minutes.reset_index()
type(a.DateTime[0])
type(RejectData_raw.DateTime[0])

RejectData_sum_hour = RejectData_raw.merge(df_minutes.reset_index(), on=['DateTime', 'DateTime'], how='left')
#RejectData_sum_hour.to_csv(robot_name + '_view_only_RejectData_sum_hour(3).csv')


'''
DO THE SAME BUT NOW ONLY FOR PLANNED DOWNTIME
'''

df = RobotFailure_reordered.copy()
df = df.loc[df['Downtime Type'] == 'planned']


df['LPM DateTime'] = pd.to_datetime(df['LPM DateTime'])
df['Rst DateTime'] = pd.to_datetime(df['Rst DateTime'])
df['hour'] = df['LPM DateTime'].apply(lambda x: x.hour)
#df['diff'] = df['Rst DateTime'] - df['LPM DateTime']
df['diff_min'] = round(df['time_per_stop'].apply(lambda x: x.seconds/60 + x.days*24*60)) #seconds up to a day, days after that
df['DateTime'] = df['LPM DateTime'].apply(lambda x: x.floor('H'))
alist = df[['DateTime', 'diff_min']].groupby(['DateTime']).sum()
alist = df[['DateTime', 'diff_min']].groupby(['DateTime']).sum()
alistnoround = df[['DateTime', 'diff_min']].groupby(['DateTime']).sum()
blist = alist['diff_min'].resample('H').sum()
list_shift_corrected = blist #clist shifts index by one to match other files
list_shift_corrected.index = list_shift_corrected.index+datetime.timedelta(hours=1)
minutes_per_hour = expand_per_hours(list_shift_corrected)
df_minutes = pd.DataFrame(minutes_per_hour, columns=['total_planned_minutes'])
df_minutes.index = list_shift_corrected.axes
a = df_minutes.reset_index()

RejectData_sum_hour2 = RejectData_sum_hour.merge(df_minutes.reset_index(), on=['DateTime', 'DateTime'], how='left')
RejectData_sum_hour2['total_planned_minutes']=RejectData_sum_hour2['total_planned_minutes'].fillna(0)
RejectData_sum_hour2['total_unplanned_minutes'] = RejectData_sum_hour2['total_down_minutes'] - RejectData_sum_hour2['total_planned_minutes']
RejectData_sum_hour2.to_csv(robot_name + '_view_only_RejectData_sum_hour(3).csv')

# https://stackoverflow.com/questions/19913659/pandas-conditional-creation-of-a-series-dataframe-column