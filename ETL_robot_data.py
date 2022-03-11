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

robots = {
'RBHA01': ['RBHA01', 'rb-ha-01'],
'RBHA02': ['RBHA02', 'rb-ha-02']}



# = = = = = = = = = = = = = = = =#
# READING ALL FILE(S) TO BE USED #
# = = = = = = = = = = = = = = = =#
# DEFINING FILE(S) TO BE OPPENED #



selected_robot = 'RBHA01'
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



# = = = = = = = = = = #
# PANDAS MANIPULATION #
# = = = = = = = = = = #
# slices the rejects columns, sums them rowwise and assigns the resuls to RejectDat_raw 
columns_rejects = np.arange(5,len(RejectData_raw.columns))

#rejects_df = RejectData_raw.iloc[:, lambda columns: np.arange(5,27)] #0:date,1:part, 2:lot#, 3:lotcount, 4:partsmade
rejects_df = RejectData_raw.iloc[:, lambda columns: columns_rejects] #0:date,1:part, 2:lot#, 3:lotcount, 4:partsmade
RejectData_raw.iloc[:, lambda columns: np.arange(5,-1)]
#RejectData_raw['Total Rejects'] = rejects_df.sum(axis=1) # if all columns were counted
RejectData_raw['Total Rejects'] = rejects_df['Cable Rejects'] + rejects_df['Swager Misses'] + \
                                  rejects_df['FitCut Misses'] + rejects_df['Lead Rejects']*.75 + \
                                  rejects_df['Tail Rejects']*.75 + rejects_df['HypoRejects']*.25 + \
                                  rejects_df['Stuck Rejects'] + rejects_df['OL Rejects #'] + \
                                  rejects_df['UZ Rejects'] + rejects_df['FL Rejects'] + \
                                  rejects_df['Bad Hypo Insert']*.75 + rejects_df['FL OL Rejects'] + \
                                  rejects_df['Cam Faults'] + rejects_df['Ejected Ftgs']*.25

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
        RobotFailure_raw['Downtime Type'][i] = stoppages[(RobotFailure_raw['Major'][i],RobotFailure_raw['Minor0'][i])]
    except (KeyError):
        RobotFailure_raw['Downtime Type'][i] = 'no valid code'

# POPULATE COLUMNS FOR DOWNTIME MEASUREMENT
RobotFailure_raw['time_per_stop'] = RobotFailure_raw['Rst DateTime'] - RobotFailure_raw['LPM DateTime']

# adds columns to RobotFailure_raw:
#    'Minutes down at the hour' is the maximum number of minutes that stoppage could fit inside that hour.
#       If the the stoppage overflows to the next hour, it is set to '0', hence its maximum value is 59.
#    'max minutes to be absorbed' is similar to 'Minutes down at the hour', but it is set for all minutes
#       left, without a maximum.
#    'remainder left for future hours' is the difference between the total amount of time for that stoppage
#       and how much it can still be used on that very same hour



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
RobotFailure_reordered.to_csv(robot_name + '_view_only_RobotFailure_reordered.csv')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# drops all but the last repeated consecutive rows, when the stoppage starts and 
# the type of stoppage (planned vs. unplanned) are the same. The sttopage on the last row always ends later
# hence no time is lost by dropping the previous rows.

for i in range(1,len(RobotFailure_reordered)-1):
    if RobotFailure_reordered['LPM DateTime'][i] == RobotFailure_reordered['LPM DateTime'][i+1] and RobotFailure_reordered['Downtime Type'][i] == RobotFailure_raw['Downtime Type'][i+1]:
        RobotFailure_reordered.drop(i, axis=0, inplace=True)
        print('drops row ', i)
RobotFailure_no_duplicates = RobotFailure_reordered.reset_index(drop=True)

# when they start at the same time, replace the new start with the previous stop, so the whole period the robot
# didn't work will be a continuous interval of stoppages 
for i in range(1,len(RobotFailure_no_duplicates)-1):
    if RobotFailure_no_duplicates['LPM DateTime'][i] == RobotFailure_no_duplicates['LPM DateTime'][i-1]:
        print('rows: ', RobotFailure_no_duplicates['LPM DateTime'][i])
        RobotFailure_no_duplicates['LPM DateTime'][i] = RobotFailure_no_duplicates['Rst DateTime'][i-1]

RobotFailure_no_duplicates.to_csv(robot_name + '_view_only_RobotFailure_no_duplicates.csv')








# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #




RobotFailure_raw.head(15)
RejectData_raw.to_csv(robot_name + '_view_only_RejectData_raw.csv')
RobotFailure_raw.to_csv(robot_name + '_view_only_RobotFailure_raw.csv')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #






# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

#trying resample to get original RobotFailure per hour
a = RobotFailure_raw
a.set_index('Rst DateTime', drop=False, inplace=True)
a.set_index('LPM DateTime', drop=False, inplace=True)
a['remainder left for future hours'] = a['remainder left for future hours'].astype(float)
a['time_per_stop'] = a['time_per_stop']/np.timedelta64(1, 's')/60

b = a.resample('H').sum()
a.dtypes
a['remainder left for future hours']
b = a
b = RobotFailure_raw.drop_duplicates(subset=['LPM DateTime'], keep='last').resample('H').sum()
#c = RobotFailure_raw.drop_duplicates(subset=['LPM DateTime'], keep='last')

#a.resample('D').sum().columns
#a.columns
#a.to_csv('erase_a.csv')
b.to_csv('erase_b.csv')
#c.to_csv('erase_c.csv')

#minutes down at hour = 0
#remainder = 0
#max minutes to be absorbed OK


#per_hour.loc['2022-02-01 10:00:00'] #row
#per_hour.loc['2022-02-01 10:00:00','carried'] #row and column
#per_hour.loc[:,'carried'] #column
RejectData_raw
per_hour_range['carried']
per_hour_range['generated']
per_hour_range['used']
per_hour_range['time running']


per_hour_range['start time']
RobotFailure_raw[(RobotFailure_raw['LPM DateTime'] >= per_hour_range['start time']) &
                (RobotFailure_raw['LPM DateTime'] <= per_hour_range['end time'])]['time_per_stop'].sum()







# https://stackoverflow.com/questions/19913659/pandas-conditional-creation-of-a-series-dataframe-column