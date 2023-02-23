# Author: Cesar Krischer 
# 02/03/2022 – initial commit
# ETL for robot data aquisition. Start with Rb-17.
'''
Imports RobotStoppage table that contains every time the robot stopped,
associate each stopagge with a planned/unplanned downtime label, spreads
the downtime (per type) per hour and finally merge it with the RejectData
table containing production per hour. The resulting table contains,
per hour:
    part number, lot, pieces made, n pieces rejected per defect, total downtime,
    planned downtime and unplanned downtime.
Calculating OEE per hour is then straightforward:
    availability = (available time) / (planned time) = 
        (time - unplanned - planned) / (time - planned)
    performance = (target production) / (available time)
    quality = (n good parts produced) / (n total parts produced)
'''

import numpy as np #main libraries
import pandas as pd #main libraries
import datetime
import os
import fnmatch #search for files using RE


def find(pattern, path):
    '''
    looks for all files in a folder that contain a certain pattern;
    use a blank list to append all files to be read and worked with.
    arguments: pattern as RE and path to scan files.
    returns: a list with files containing that pattern.
    '''
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


def get_input_file_name(robot_folder_name='Rb-17', file_name_prefix='RB17', metric_folder='RobotFailureLogs',
                    day_folder=str(datetime.datetime.now().strftime('%B')),
                    month_folder=str(datetime.datetime.now().strftime('%B')),
                    year_folder=2022):#datetime.datetime.now().strftime('%Y')):
    '''
    Returns the path for the file to be oppened.
    This function uses the robots folders hierarchies to translate the inputs into a file path.
    If no specific DateTime is provided, it uses the system current DateTime. 
    Arguments: robot_folder_name, file_name_prefix, metric_folder, day_folder, month_folder, year_folder.
    Returns: file path.
    '''
    windows_separator_for_network_access = '\\' # needs \\ to open files on Windows. TODO be updated to be more generic
    # metrics have different folders structures, stored in the _directories_ dictionary. Retrieve them for path retrieval
    if directories[metric_folder] == 'YMF_D': 
        file_path = os.sep.join([str(windows_separator_for_network_access), 
                        str(robot_folder_name), 'LocalShare', 'RuntimeData',
                        str(metric_folder) + 'Logs', #folder ends with 'Logs' and files end with 'Log'
                        str(year_folder), str(month_folder), str(file_name_prefix) + 
                        '_' + str(metric_folder) + 'Log_' + datetime.datetime.now().strftime('%d') + '.csv'])

    elif directories[metric_folder] == 'F_Y': #RobotFailureLogs
        file_path = os.sep.join([str(windows_separator_for_network_access), 
                        str(robot_folder_name), 'LocalShare', 'RuntimeData',
                        str(metric_folder) + 'Logs', #folder ends with 'Logs' and files end with 'Log'
                        #year_folder, 
                        #month_folder,
                        str(file_name_prefix) + '_' + str(metric_folder) + 'Log_' +
                        str(year_folder) + '.csv'])

    elif directories[metric_folder] == 'YF_M': #RobotStoppageLogs
        file_path = os.sep.join([str(windows_separator_for_network_access), 
                        str(robot_folder_name), 'LocalShare', 'RuntimeData',
                        str(metric_folder) + 'Logs', #folder ends with 'Logs' and files end with 'Log'
                        str(year_folder),
                        str(file_name_prefix) + '_' + str(metric_folder) + 'Log_' + month_folder + '.csv'])
    return file_path


def round_to_next_hour(t, delta=1):
    '''
    Rounds DateTime to next hour (equivalent to a ceiling function).
    input: DateTime.
    output: DateTime rounded to the next hour.
    '''
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +datetime.timedelta(hours=delta))


def expand_per_hours(orignal_series):
    '''
    Spreads the sum of downtimes per hour to a maximum of 60 minutes per hour.
    arguments: a series of downtimes per hour (the total downtime of a stoppage that started on that hour).
    returns: series of downtimes per hour, maximum 60 minutes per hour.
    '''
    fit_in_hour = [0] * orignal_series.size
    for current_hour in range(orignal_series.size):
        a = divmod(int(orignal_series[current_hour]), 60)
        extra_whole_hours = a[0]
        extra_minutes = a[1]
        if extra_whole_hours == 0: # if doesn't overflow to the next hour, set the value for the hour
            try:
                fit_in_hour[current_hour] = fit_in_hour[current_hour] + orignal_series[current_hour]
            except KeyError:
                fit_in_hour[current_hour] = orignal_series[current_hour]
        else: # if it overflows to the next hour, see how many hours worth of overtime and split by the next hous
            for next_hour in range(current_hour, current_hour+extra_whole_hours):
                try:
                    fit_in_hour[next_hour] = fit_in_hour[next_hour] + 60
                except KeyError:
                    fit_in_hour[next_hour] = 60
            try:
                fit_in_hour[current_hour+extra_whole_hours] = fit_in_hour[current_hour+extra_whole_hours] + extra_minutes
            except KeyError:
                fit_in_hour[current_hour+extra_whole_hours] = extra_minutes
    return pd.Series(fit_in_hour)


# = = = = = = = = = = = = = = = = = = = = = = #
# DEFINES FOLDERS STRUCTURES AND ROBOTS NAMES #
# = = = = = = = = = = = = = = = = = = = = = = #

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
'RBHA03': ['RBHA03', 'rb-ha-03'],
'RB17': ['RB17', 'Rb-17']
}


# = = = = = = = = = = = = = = =#
# READS ALL FILE(S) TO BE USED #
# = = = = = = = = = = = = = = =#
# DEFINING FILE(S) TO BE OPPENED #

#selected_robot = 'RBHA02' moved line to position 1
selected_robot = 'RB17'
robot_name = robots[selected_robot][0]   # from dictionary of names 
robot_folder = robots[selected_robot][1] # from dictionary of names

file_path_RejectData = get_input_file_name(robot_folder, robot_name, 'RejectData')
file_path_RobotFailure = get_input_file_name(robot_folder, robot_name, 'RobotFailure')
file_path_RobotStoppage = get_input_file_name(robot_folder, robot_name, 'RobotStoppage')

# names provided by robots team
reject_data_log_columns = ['DateTime','Part #','Lot #','Lot Count','Parts Made','Cable Rejects',
'Swager Misses','FitCut Misses','Lead Rejects','Tail Rejects','HypoRejects','Stuck Rejects',
'OL Rejects #','UZ Rejects','FL Rejects','Knots','ENFORCER!','Bad Hypo Insert', 'FL OL Rejects',
'Cam Faults','Ejected Ftgs', 'StakePulls', 'StakePullUnder', 'TailSlideJog', 'TailUnstick',
'TailSlideJogRejects', 'TailUnstickRejects']
RejectData_raw = pd.read_csv(file_path_RejectData, names=reject_data_log_columns, skiprows=1)
RobotFailure_raw = pd.read_csv(file_path_RobotFailure)
planned_downtime = pd.read_csv('planned_downtime.csv')
#RobotStoppage_raw = pd.read_csv(file_path_RobotStoppage)

# convert to DateTime format
RejectData_raw['DateTime'] = pd.to_datetime(RejectData_raw['DateTime'])
RobotFailure_raw['Rst DateTime'] = pd.to_datetime(RobotFailure_raw['Rst DateTime'])
RobotFailure_raw['LPM DateTime'] = pd.to_datetime(RobotFailure_raw['LPM DateTime'])
#RobotStoppage_raw['DateTime'] = pd.to_datetime(RobotStoppage_raw['DateTime'])


# = = = = = = = = = = = = = = = = = #
# CALCULATES TOTAL REJECTS PER HOUR #
# = = = = = = = = = = = = = = = = = #
# weights defined between engineering and design team to capture true bad parts only once. 
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

# CREATING COLUMNS FOR DOWNTIME ## (TODO: change loop to apply function)
RobotFailure_raw['Downtime Type'] = 0
for i in range(0,len(RobotFailure_raw)):
    try: #e.g.: stoppages[('Engineering','Code changes')] returns 'planned'
        [RobotFailure_raw['Downtime Type'].iloc[i]] = stoppages[(RobotFailure_raw['Major'][i],RobotFailure_raw['Minor0'][i])]
    except (KeyError):
        RobotFailure_raw['Downtime Type'].iloc[i] = 'no valid code' # is going to be counted as unplanned

# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# CLEANS ENTRIES FOR PROCESSING – DELETES REPEATED ROWS & ADJUSTS TIMES FOR SMOOTH TRANSITION #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# drops all but the last repeated consecutive rows, when the stoppage starts and 
# the type of stoppage (planned vs. unplanned) are the same. The sttopage on the last row always ends later
# hence no data is lost by dropping the previous repeated rows.

for i in range(1,len(RobotFailure_raw)-1):
    if RobotFailure_raw['LPM DateTime'][i] == RobotFailure_raw['LPM DateTime'][i+1] and \
    RobotFailure_raw['Downtime Type'][i] == RobotFailure_raw['Downtime Type'][i+1]:
        RobotFailure_raw.drop(i, axis=0, inplace=True) #drops repeated rows
RobotFailure_no_duplicates = RobotFailure_raw.reset_index(drop=True) #to avoid jump in index values

# when they start at the same time, replace the new start with the previous stop, so the whole period the robot
# didn't work will be a continuous interval of stoppages
# In some cases, there is more than 2 changes in planned/unplanned starting at the same time,
# which means the 2nd time will copy the 1st one, but by the time the 3rd is checked with the 2nd,
# the 2nd was already changed and they will be different. This would lead to a smaller LPM date than the previous.
# a check for <= checks for both cases.
for i in range(1,len(RobotFailure_no_duplicates)):
    if RobotFailure_no_duplicates['LPM DateTime'][i] <= RobotFailure_no_duplicates['LPM DateTime'][i-1]:
        RobotFailure_no_duplicates['LPM DateTime'].iloc[i] = RobotFailure_no_duplicates['Rst DateTime'][i-1]


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =#
# SPLITS LONG ENTRIES (WHEN OVERFLOWS TO THE NEXT HOUR) INTO TWO #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =#
# When any interval goes after the hour (e.g. 7:50 to 9:10), split it in two:
#   * one from the first start to the first whole hour;
#   * one from the first whole hour to the end.
# One split is enough, as only one event will cross the hour. Later hours are going to be either 60 minutes
# or summed with other entries.
'''
# 11:50 | 13:50 (original row)
#       V
# 11:50 | 12:00 (transforms row)
# 12:00 | 13:50 (adds row)
''' 
RobotFailure_no_duplicates_split = RobotFailure_no_duplicates.copy()
for i in range(1,len(RobotFailure_no_duplicates_split)): #If starts and ends on the same hour, do nothing.
    if RobotFailure_no_duplicates_split['LPM DateTime'][i].hour != RobotFailure_no_duplicates_split['Rst DateTime'][i].hour:
        RobotFailure_no_duplicates_split.loc[i +0.5] = RobotFailure_no_duplicates_split.loc[i] # makes new inter line = previous
        # starts on first hour after overflow (round_next_hour last LPM), ends on original value (line is already duplicated)
        RobotFailure_no_duplicates_split.loc[i +0.5, 'LPM DateTime'] = round_to_next_hour(RobotFailure_no_duplicates_split.loc[i, 'LPM DateTime'])
        # changes row[i] to end on the next rounded hour, so the line [i + 0.5] can start there and go until original end 
        RobotFailure_no_duplicates_split['Rst DateTime'].iloc[i] = round_to_next_hour(RobotFailure_no_duplicates_split['LPM DateTime'][i])
RobotFailure_no_duplicates_split = RobotFailure_no_duplicates_split.sort_index().reset_index(drop=True)


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #
# CREATES 2 FAKE EVENTS OF 0 SEC AT THE LAST ROWS FOR MAKING THE RANGE FOR RESAMPLE #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = # 
#     When resampling, the last value in the df is going to be the final range of the
# new array. By adding a fake value of 0 seconds, it forces the new array to finish at
# that hour; as it necessaryly happens after the last real value, it forces the function
# to count time to be in range.
#     It needs to be one of each (planned and unplanned downtime) as they'll be split.


# duplicates the last row
RobotFailure_no_duplicates_split = RobotFailure_no_duplicates_split.append(RobotFailure_no_duplicates_split.tail(1), ignore_index=True)
# changes descriptions to make clear when exporting table that those were created
RobotFailure_no_duplicates_split.loc[RobotFailure_no_duplicates_split.index[-1], ('Major')] = 'Non Error'
RobotFailure_no_duplicates_split.loc[RobotFailure_no_duplicates_split.index[-1], ('Minor0')] = 'Placeholder for OEE calculation'
RobotFailure_no_duplicates_split.loc[RobotFailure_no_duplicates_split.index[-1], ('Minor1')] = 'Placeholder for OEE calculation'
RobotFailure_no_duplicates_split.loc[RobotFailure_no_duplicates_split.index[-1], ('Comment')] = 'Placeholder for OEE calculation'
RobotFailure_no_duplicates_split.loc[RobotFailure_no_duplicates_split.index[-1], ('Downtime Type')] = 'planned'
# copies the value from Rst to LPM, to create a zero seconds event
RobotFailure_no_duplicates_split.iloc[-1, RobotFailure_no_duplicates_split.columns.get_loc('LPM DateTime')] = \
    RobotFailure_no_duplicates_split.iloc[-1, RobotFailure_no_duplicates_split.columns.get_loc('Rst DateTime')]
# repeat the same steps but for non-planned downtime, as the tables will be split to spread the downtime through the hours;
RobotFailure_no_duplicates_split = RobotFailure_no_duplicates_split.append(RobotFailure_no_duplicates_split.tail(1), ignore_index=True)
# as the times and descriptions were already changed, only the Downtime Type must be updated
RobotFailure_no_duplicates_split.loc[RobotFailure_no_duplicates_split.index[-1], ('Downtime Type')] = 'non-planned'


# = = = = = = = = = = = = = = = = = = = = = #
# ADDS COLUMNS FOR DEBBUGING / MANUAL CHECK #
# = = = = = = = = = = = = = = = = = = = = = #

# adds columns to RobotFailure_raw:
#    'Minutes down at the hour' is the maximum number of minutes that stoppage could fit inside that hour.
#       If the the stoppage overflows to the next hour, it is set to '0', hence its maximum value is 59.
#    'max minutes to be absorbed' is similar to 'Minutes down at the hour', but it is set for all minutes
#       left, without a maximum.
#    'remainder left for future hours' is the difference between the total amount of time for that stoppage
#       and how much it can still be used on that very same hour
# note that 'max minutes to be absorbed' + 'remainder left for future hours' = total downtime for a given period.

RobotFailure_extra_columns=RobotFailure_no_duplicates_split.copy()

# POPULATE COLUMNS FOR DOWNTIME MEASUREMENT
RobotFailure_extra_columns['time_per_stop'] = RobotFailure_extra_columns['Rst DateTime'] - RobotFailure_extra_columns['LPM DateTime']

RobotFailure_extra_columns['Minutes down at the hour'] = np.where(
                                                        RobotFailure_extra_columns['LPM DateTime'].dt.minute + RobotFailure_extra_columns['LPM DateTime'].dt.second/60 +
                                                        RobotFailure_extra_columns['time_per_stop']/np.timedelta64(1, 'm') >= 60,
                                                            'overflow',
#                                                        0, # taking only the minutes doesn't return enought granularity. Taking the decimal from sec
                                                        (RobotFailure_extra_columns['Rst DateTime'].dt.minute + RobotFailure_extra_columns['Rst DateTime'].dt.second / 60)- 
                                                        (RobotFailure_extra_columns['LPM DateTime'].dt.minute + RobotFailure_extra_columns['LPM DateTime'].dt.second / 60))

RobotFailure_extra_columns['max minutes to be absorbed'] = np.where(
                                                        RobotFailure_extra_columns['LPM DateTime'].dt.minute + RobotFailure_extra_columns['LPM DateTime'].dt.second/60 +
                                                        RobotFailure_extra_columns['time_per_stop']/np.timedelta64(1, 'm') >= 60, #
                                                        60 - (RobotFailure_extra_columns['LPM DateTime'].dt.minute + RobotFailure_extra_columns['LPM DateTime'].dt.second/60) ,
                                                        (RobotFailure_extra_columns['Rst DateTime'].dt.minute + RobotFailure_extra_columns['Rst DateTime'].dt.second/60) -
                                                        (RobotFailure_extra_columns['LPM DateTime'].dt.minute + RobotFailure_extra_columns['LPM DateTime'].dt.second/60))

RobotFailure_extra_columns['remainder left for future hours'] = np.where(
                                                        RobotFailure_extra_columns['Minutes down at the hour'] != 'overflow',
#                                                        RobotFailure_extra_columns['Minutes down at the hour'] > 0.0,
                                                        '0',
                                                        RobotFailure_extra_columns['time_per_stop']/np.timedelta64(1, 's')/60 -
                                                        (60-(RobotFailure_extra_columns['LPM DateTime'].dt.minute + RobotFailure_extra_columns['LPM DateTime'].dt.second/60)))

RobotFailure_reordered = RobotFailure_extra_columns.columns.tolist()
RobotFailure_reordered = RobotFailure_extra_columns[['LPM DateTime', 'Rst DateTime',  'Downtime Type', 'time_per_stop',
                                            'Minutes down at the hour', 'max minutes to be absorbed',
                                            'remainder left for future hours', 'Detail', 'Major', 'Minor0', 'Part#', 'Lot#']]



#RejectData_raw.to_csv(robot_name + '_view_only_RejectData_raw(1).csv')
RobotFailure_reordered.to_csv(robot_name + '_RobotFailure_manual_check(0)_2022.csv') #exports to the file folder
#RobotFailure_raw.to_csv(robot_name + '_view_only_RobotFailure_raw.csv')


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =#
# CALCULATES COLUMNS FOR FUTURE SPREADING THE MINUTES PER HOUR #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =#
RobotFailure_spread = RobotFailure_no_duplicates_split.copy()
# adds hour columns to the df so it can be grouped by them. NOTE 'diff_min' = round('max minutes to be absorbed')
RobotFailure_spread['time_per_stop'] = RobotFailure_spread['Rst DateTime'] - RobotFailure_spread['LPM DateTime']
RobotFailure_spread['diff_min'] = round(RobotFailure_spread['time_per_stop'].apply(lambda x: x.seconds/60 + x.days*24*60)) #seconds up to a day, days after that
RobotFailure_spread['DateTime'] = RobotFailure_spread['LPM DateTime'].apply(lambda x: x.floor('H'))

RobotFailure_spread_planned = RobotFailure_spread.loc[RobotFailure_spread['Downtime Type'] == 'planned']


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
#                ROUNDS HOURS IF IN BETWEEN TWO HOURS             #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
RejectData_sum_hour_rounded = RejectData_raw.copy()
for i in range(1, len(RejectData_sum_hour_rounded.DateTime)-1): # so it can compare with previous and next values
    # to be changed, that hour must be different than thar rounded hour AND
    # if i rounded value is bigger than rounded previous but smaller than rounded next
    # as to avoid 4:05 and 4:06 receiving the same data when merging this df with data
    if RejectData_sum_hour_rounded['DateTime'].iloc[i] != round_to_next_hour(RejectData_sum_hour_rounded['DateTime'].iloc[i], 0) and \
                                                          round_to_next_hour(RejectData_sum_hour_rounded['DateTime'].iloc[i-1], 0) < \
                                                          round_to_next_hour(RejectData_sum_hour_rounded['DateTime'].iloc[i], 0) < \
                                                          round_to_next_hour(RejectData_sum_hour_rounded['DateTime'].iloc[i+1], 0):
#        print('rounded hour:' + str(RejectData_sum_hour_rounded['DateTime'].iloc[i]))
        RejectData_sum_hour_rounded.DateTime.iloc[i] = round_to_next_hour(RejectData_sum_hour_rounded.DateTime[i], 0)
'''
RejectData_sum_hour_rounded.to_csv('erase_RejectData_sum_hour_rounded.csv')
'''

# TODO change next 2 sections in a function and re-use it, as they're equal.
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# SPREAD THE HOURS FOR ALL EVENTS AND RETRIEVE TOTAL DOWNTIME #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
try:
    # transforms the df in series and groups it by hour
    sum_minutes_per_hour = RobotFailure_spread[['DateTime', 'diff_min']].groupby(['DateTime']).sum()
    list_shift_corrected = sum_minutes_per_hour['diff_min'].resample('H').sum() #transforms df in series
    list_shift_corrected.index = list_shift_corrected.index+datetime.timedelta(hours=1) # shifts index by one to match other files
    # uses custom function to spread the minutes across the next hours if they overflow
    spread_minutes_per_hour = expand_per_hours(list_shift_corrected) 
    # converts back to DataFrame so it can be merged with the RejectData_raw table 
    minutes_per_hour = pd.DataFrame(spread_minutes_per_hour, columns=['total_down_minutes'])
    minutes_per_hour.index = list_shift_corrected.axes
    RejectData_sum_hour_rounded = RejectData_sum_hour_rounded.merge(minutes_per_hour.reset_index(), on=['DateTime', 'DateTime'], how='left')
except IndexError:
    print('expand_per_hours failed when calculating total_down_minutes')
    
    with open("log.txt", 'a') as file1: # appends to a log
        file1.write("expand_per_hours failed when calculating total_down_minutes at " + datetime.datetime.now().strftime('%x %X'))

# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# SPREAD THE HOURS FOR PLANNED EVENTS AND RETRIEVE PLANNED DOWNTIME #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

# transforms the df in series and groups it by hour
try:
    sum_minutes_per_hour = RobotFailure_spread_planned[['DateTime', 'diff_min']].groupby(['DateTime']).sum()
    list_shift_corrected = sum_minutes_per_hour['diff_min'].resample('H').sum() #transforms df in series
    list_shift_corrected.index = list_shift_corrected.index+datetime.timedelta(hours=1) # shifts index by one to match other files
    # uses custom function to spread the minutes across the next hours if they overflow
    spread_minutes_per_hour = expand_per_hours(list_shift_corrected) 
    # converts back to DataFrame so it can be merged with the RejectData_raw table 
    minutes_per_hour = pd.DataFrame(spread_minutes_per_hour, columns=['total_planned_minutes'])
    minutes_per_hour.index = list_shift_corrected.axes
except IndexError:
    print('expand_per_hours failed when calculating total_down_minutes')
    with open("log.txt", 'a') as file1: # appends to a log
        file1.write("expand_per_hours failed when calculating total_down_minutes at " + datetime.datetime.now().strftime('%x %X'))


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# MERGES THE PLANNED AND UNPLANNED DOWNTIMES TABLES AND EXPORT IT #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# merges RejectData, total downtime, and planned downtime. Unplanned downtime is the difference between total and planned.
# making the difference instead of a sum ensures there will be no instance of more than 60 minutes stopped per hour.
# fills NaN with 0s to avoid the blanks in the end of the columns.

# TODO extracting the time of RejectData gives the most updated time; use it on expand_per_hours to return already the
# # right size vector and avoid having blanks at the end of the columns. 
RejectData_sum_hour = RejectData_sum_hour_rounded.merge(minutes_per_hour.reset_index(), on=['DateTime', 'DateTime'], how='left')
RejectData_sum_hour['total_planned_minutes']=RejectData_sum_hour['total_planned_minutes'].fillna(0)
RejectData_sum_hour['total_down_minutes']=RejectData_sum_hour['total_down_minutes'].fillna(0)
RejectData_sum_hour['total_unplanned_minutes'] = RejectData_sum_hour['total_down_minutes'] - RejectData_sum_hour['total_planned_minutes']
RejectData_sum_hour['total_unplanned_minutes']=RejectData_sum_hour['total_unplanned_minutes'].fillna(0)
#RejectData_sum_hour.to_csv(robot_name + '_RejectData_sum_per_hour(1).csv') #exports to the file folder

# https://stackoverflow.com/questions/19913659/pandas-conditional-creation-of-a-series-dataframe-column

# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
#                   FILLS THE MISSING HOURS WITH NAs              #
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# creates a temporary df with the range of the original one to later merge it with original
df = pd.DataFrame(pd.date_range(round_to_next_hour(RejectData_sum_hour['DateTime'].min()),\
        round_to_next_hour(RejectData_sum_hour['DateTime'].max(),0),freq='H'),columns= ['DateTime'])\
            .merge(RejectData_sum_hour,on=['DateTime'],how='outer').fillna('N/A')
df.drop(df.tail(1).index,inplace=True) # as last row doesn't contain relevant data
df.to_csv(robot_name + '_complete_final_trial(2)_2022.csv') #exports to the file folder

#df.hour = df.date.dt.strftime('%H:%M:%S')
#df.date = df.date.dt.strftime('%d-%m-%Y')


with open("log.txt", 'a') as file1: # appends to a log
    file1.write("Scrip finished running at " + datetime.datetime.now().strftime('%x %X') + '\n')