"""
Author: Saa, Martin
(C):    Saa, Martin (sole)
Description:    This code will add the latest coverage data and compare with the previous
"""


################################
######## Set Up ################
################################

## import the necessary packages
import pandas as pd
import numpy as np
#
print("Import is done...")


################################
######## Functions #############
################################

"""..."""


################################
######## Main ##################
################################

'''
read in the data files that we need
-----------------------------------
'''
## read the "fact" data
fact_table_file = ""
fact_table = pd.read_csv(fact_table_file, dtype=str)
#
existing_periods = fact_table.period.drop_duplicates().astype(int)
prev_period = existing_periods.max()
fact_table_prev = fact_table.loc[fact_table.period == str(prev_period)].copy()
#
print('fact table read from: ' + fact_table_file)

## read in the latest file
new_data_file = ""
new_data = pd.read_csv(new_data_file, dtype=str)
#
print('new data read from: ' + new_data_file)


'''
process the data files
--------------------------------
<server id> is the <server ip> + <server name>; whitespace stripped, lowercase
checks existing coverage in the last time period; time period is numberical integer
compares new coverage against the existing coverage
IMPORTANT: stap files report ALL coverage, not just change
'''
## create an id for the new data
new_data.loc[:, 'server_id'] = new_data.ip_address.str.lower().str.strip() + new_data.server_name.str.lower().str.strip()

## compare data ;; left is new stap file, right is the existing file
##.new data will not have a match on the right side
##.all data on the right side should have a left match -> if not, it means we dropped coverage
data_comp = pd.merge(left=new_data, right=fact_table_prev, left_on='server_id', right_on='server_id', how='outer', suffixes=(“_x”, “_y”))
#..extract the data categories
new_coverage = data_comp.loc[data_comp.server_id_y.isna(), ['server_name', 'ip_address', 'server_id_x']]        # select orig columns
existing_coverage = data_comp.loc[(data_comp.server_id_y.notna()) & (data_comp.server_id_x.notna()), ['server_name', 'ip_address', 'server_id_x']]            # select all columns
dropped_coverage = data_comp.loc[data_comp.server_id_x.isna(), ['server_name', 'ip_address', 'server_id_y']]                                                  # select all columns
#..format the new coverage to add
new_coverage.columns = ['server_name', 'ip_address', 'server_id']
new_coverage.loc[:, 'period'] = (prev_period+1)
new_coverage.loc[:, 'status'] = "new monitored"
new_coverage.loc[:, 'record_id'] = new_coverage.server_id + new_coverage.period
#..format the new coverage to add
existing_coverage.columns = ['server_name', 'ip_address', 'server_id']
existing_coverage.loc[:, 'period'] = (prev_period+1)
existing_coverage.loc[:, 'status'] = "existing monitored"
existing_coverage.loc[:, 'record_id'] = existing_coverage.server_id + existing_coverage.period
#..format the new coverage to add
dropped_coverage.columns = ['server_name', 'ip_address', 'server_id']
dropped_coverage.loc[:, 'period'] = (prev_period+1)
dropped_coverage.loc[:, 'status'] = "dropped monitored"
dropped_coverage.loc[:, 'record_id'] = dropped_coverage.server_id + dropped_coverage.period


'''
export the processed data
--------------------------------
'''
## re-make the fact table
fact_table_new = pd.concat([fact_table, new_coverage, existing_coverage, dropped_coverage])
fact_table_new.to_csv(fact_table_file, index=False)
