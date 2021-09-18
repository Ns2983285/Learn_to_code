''' Importing all the libraries we would require for running this code'''

import pandas as pd
import os
import sys
from pathlib import Path
import datetime as dt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import numpy as np
from pandas.api.types import CategoricalDtype
from math import sin, cos, sqrt, atan2, radians
import warnings
warnings.filterwarnings("ignore")
import csv

'''Lets assign all the direcortory location for input files and required initial variables'''

#Please change below location as per the location where your input data is located
work_folder = r"C:\\Users\\RatedNik\\Desktop\\Upwork\\"

#Input file details. These are the files that we will run as a part of below steps
#Please change file name if there is any change in future
client_intel_file = r'client_intellibase_aax.csv'
cust_analytic_file = r'cust_analytic_aca.csv'
eclub_file = r'eclub_aac.csv'
geo_file = r'geo_aaa.csv'
id_mgmt_file = r'identity_management_aaa.csv'
item_hier_1_file = r'item_hier_1_aaa.csv'
loyalty_file = r'loyalty_aab.csv'
room_file = r'room_aaa.csv'
trans_detail_file = r'trans_detail_aeg.csv'
trans_header_file = r'trans_header_aba.csv'
trans_offprem_file = r'trans_offprem_abc.csv'
trans_pay_file = r'trans_pay_acs.csv'
geo_concept_file = r'geo_concept.csv'
dim_value_file = r'dim_value_aaa.csv'
daypart_file = r'daypart_aaa.csv'

#Assigning constand variables
score_dims = 'Y'
gcontrol = 'Y'
climit = 'N'
clist = [4377]
item_end_dt = 'N'
inc_concept0 = 'N'
gaps = [30, 60, 90, 180, 365]
)

#if required please change the date in below step. Right now i have hardcoded it to today's date
#score_dt would have next days date
#Note: tdt macro variable is not assigned in code shared and would need to understand that value. Python code share has hardcoded value
date_val=str(dt.datetime.today()).split()[0]
tdt = dt.datetime.strptime(date_val, '%Y-%m-%d')
score_dt = tdt + dt.timedelta(days = 1)
print(score_dt)

#Setup the oprm variable: This is being setup on basis of number of records trans_offpremfile
#Check if file exist and setup variable on the basis of that 
oprm=0
row_count=0
if os.path.isfile(os.path.join(work_folder, trans_offprem_file)):
    with open(os.path.join(work_folder, trans_offprem_file),"r") as f:
        reader = csv.reader(f,delimiter = ",")
        data = list(reader)
        row_count = len(data)
print(row_count)

if row_count>1:
    oprm=1

#item_hier file is not shared right now. Right now i am defaulting it to 11. Will update once Venu share file next week
if os.path.isfile(os.path.join(work_folder, item_hier_1_file)):
    item_hier_1_df = pd.read_csv(os.path.join(work_folder, item_hier_1_file))
    numcats = max(item_hier_1_df.shape[0], 11)
else:
    numcats=11

print("Numcats value is:",numcats)

#Total number of inputs available in gaps variable
numgaps = len(gaps)
print(numgaps)

#rooms file is not shared right now. Right now i am defaulting it to 11. Will update once Venu share file next week
if os.path.isfile(os.path.join(work_folder,room_file)):
    room_df = pd.read_csv(os.path.join(work_folder, room_file))
    on_prem_rooms = room_df[room_df['on_prem'] == 1]
    off_prem_rooms = room_df[room_df['on_prem'] != 1]
    on_cnt = on_prem_rooms.shape[0]
    off_cnt = off_prem_rooms.shape[0]
    roommx = max(6, len(set(room_df['room_id'].to_list())))
else:
    on_prem_rooms=0
    off_prem_rooms=0
    on_cnt =0
    off_cnt =0
    roommx=0

# Geo Concepts file. File is not provided will let Venu know
if os.path.isfile(os.path.join(work_folder,geo_concept_file)):
    geo_concept_df = pd.read_csv(os.path.join(work_folder, geo_concept_file))
    concmx = geo_concept_df['geo_concept'].max()
    print(concmx)
else:
    print("Please check if Geo Concept file is available in the location")
    concmx=10

# Day part file is also needed. will check with Venu on this step as well 
if os.path.isfile(os.path.join(work_folder,daypart_file)):
    daypart_df = pd.read_csv(os.path.join(work_folder, daypart_file))
    dayparts = daypart_df.shape[0]
    dayparts = len(set(dayparts['daypart_id'].to_list()))
    for i in range(dayparts):
        print(i+1)
        daypart_temp = daypart_df[daypart_df['daypart_start_hour'] == i+1]
        globals()['dp_st_%s' % i]=min(daypart_temp1['daypart_start_hour'])
        globals()['dp_end_%s' % i]=max(daypart_temp1['daypart_start_hour'])

if os.path.isfile(os.path.join(work_folder,dim_value_file)):
    dim_value_df = pd.read_csv(os.path.join(work_folder, dim_value_file))
    for c1 in range(concmx):
        dim_value_df_temp=dim_value_df[dim_value_df['geo_concept'] == c1+1]
        globals()['Cdval%s' % i] = off_prem_rooms.shape[0]
        for j in range(globals()['Cdval%s' % i]):
            globals()['C_dv_low_%s' % j]=min(dim_value_df['dim_value_low'])
            globals()['C_dv_high_%s' % j]=max(dim_value_df['dim_value_low'])
else:
    print("Please check if file dim_value is available in the location")

# Create the date variables
months_ltd, months_active, months_analytic = 36,6,12
end_dtt = score_dt + dt.timedelta(days = 0)
start_dtt = end_dtt + dt.timedelta(days = - (months_ltd * 30) - 1 )
act_dtt = end_dtt + dt.timedelta(days = - (months_active * 30) - 1 )
analy_dtt = end_dtt + dt.timedelta(days = - (months_analytic * 30) - 1 )

#Starts step 1 : Master file Custid list
# Cust ID master list
cust_analytic_df = pd.read_csv(os.path.join(work_folder, cust_analytic_file)) 
cust_ids = pd.DataFrame(list(set(cust_analytic_df['cust_id'].to_list())), columns = ['cust_id'])


# Transaction master list
date_columns = ['trans_dt']
trans_cols = ['geo_id' , 'trans_dt', 'trans_id', 'cust_id', 'pay_gross_amt', 'pay_net_amt', 'pay_disc_amt', 'loyalty_id', 'cogi']
trans_df = pd.read_csv(os.path.join(work_folder, trans_pay_file), parse_dates = date_columns, usecols = trans_cols)

# Subset with relevant dates
#Note climit and clist are defined at the top. Please update them if required beofre running this step
trans_df = trans_df[(trans_df['trans_dt'] < end_dtt) & (trans_df['trans_dt'] > start_dtt)]
if climit=='y' or climit=='Y':
    trans_df=trans_df[trans_df['cust_id'].isin(clist)]

trans_pay_ids = trans_df[['geo_id', 'trans_dt', 'trans_id']].drop_duplicates()



#Trans Header File reading
trans_head_cols = ['trans_id', 'geo_id',  'trans_dt',  'trans_time',  'room_id',  'geo_concept']
date_columns = ['trans_dt']
trans_header_df = pd.read_csv(os.path.join(work_folder, trans_header_file), parse_dates = date_columns, usecols = trans_head_cols) 
trans_header_df1=trans_header_df[(trans_header_df['trans_dt'] < end_dtt) & (trans_header_df['trans_dt'] > start_dtt)]
# Merge header data to the  pay ids dataframe
trans_header = trans_pay_ids.merge(trans_header_df1, on = ['geo_id', 'trans_dt', 'trans_id'], how = 'inner')

# Create loyalty_flag
trans_df['_loyalty_flag'] = trans_df['loyalty_id'].apply(lambda x : 1 if np.isnan(x) == False else 0)

# Aggregation of the data
trans_pay_agg = pd.pivot_table(trans_df, index = ['geo_id', 'trans_dt', 'trans_id', 'cust_id', 'cogi'], 
                               values = ['pay_gross_amt', 'pay_net_amt', 'pay_disc_amt', '_loyalty_flag'], 
                               aggfunc = 'mean', fill_value = 0).reset_index()

trans_pay_agg['_loyalty_flag'] = trans_pay_agg['_loyalty_flag'].apply(lambda x : 1 if x > 0 else 0)
trans_pay_agg.rename(columns = {'pay_gross_amt': 'sales_gross',
                               'pay_net_amt': 'sales_net',
                               'pay_disc_amt': 'sales_disc'}, inplace = True)

#Read tje transaction detail file    
trans_detail_df = pd.read_csv(os.path.join(work_folder, trans_detail_file), parse_dates = ['trans_dt'], usecols = ['geo_id', 'trans_dt', 'trans_id', 'geo_concept', 'item_hier_1', 'item_hier_2', 'detail_item_amt'])

item_cat = [i for i in range(1, numcats+1)]
item_cat_list = CategoricalDtype(categories= item_cat, ordered = False)

trans_detail_df['item_hier_1'] = trans_detail_df['item_hier_1'].fillna(1).astype(item_cat_list)
trans_detail_df['item_hier_2'] = trans_detail_df['item_hier_2'].fillna(-1)
# Subset with relevant dates
# Skipping the date subset part since it gives zero relevant transactions
trans_detail_df = trans_detail_df[(trans_detail_df['trans_dt'] < end_dt) & (trans_detail_df['trans_dt'] > start_dt)]