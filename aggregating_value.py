# -*- coding: utf-8 -*-
"""
Created on Sun Sep  1 00:18:38 2019

@author: Irina
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 09:09:34 2019

@author: Irina
"""

import numpy as np
import pandas as pd
from datetime import datetime

mydf = pd.read_csv("Data/Raw/mydf.csv")

# Second method. 
def transformation_dict(df, time_unit, measure):
    dictionary_time={'year': lambda dt: dt.year, 
                'month' : lambda dt: dt.month,
                'week' : lambda dt: dt.week,
                'weekday' : lambda dt: dt.weekday(),
                'day' : lambda dt: dt.day}
    
    dictionary_measure={'mean': lambda dt: dt.mean(), 
                'max' : lambda dt: dt.max(),
                'sum' : lambda dt: dt.sum(),
                'min' : lambda dt: dt.min(),
                'count' : lambda dt: dt.count(),
                'median' : lambda dt: dt.median()}
    
    time_unit_extraction_func = dictionary_time[time_unit]
    measure_extraction_func = dictionary_measure[measure]
    inner_df = df.copy()
    inner_df['timestamp'] = pd.to_datetime(inner_df['timestamp'])
    inner_df[time_unit]= inner_df['timestamp'].map(time_unit_extraction_func) # for ech element there f(datetime)
    inner_df = inner_df.groupby(time_unit, sort = True, as_index = False).apply(measure_extraction_func)
    print('Aggregate by %s, return %s of x'%  (time_unit, measure))
    return inner_df.loc[:,[time_unit,'x']].rename(columns={time_unit: "time_gr", "x": "x_agg"})

# Second method 
    
def transformation_by_str(df, time_unit, measure):
    dictionary_time={'year': '%Y', 
                'month' : '%m',
                'week' : '%Y%w',
                'weekday' : '%U',
                'day' : '%Y%m%d'}
    
    dictionary_measure={'mean': lambda dt: dt.mean(), 
                'max' : lambda dt: dt.max(),
                'sum' : lambda dt: dt.sum(),
                'min' : lambda dt: dt.min(),
                'count' : lambda dt: dt.count(),
                'median' : lambda dt: dt.median()}
    
    time_unit_extraction_code = dictionary_time[time_unit]
    measure_extraction_func = dictionary_measure[measure]
    inner_df = df.copy()
    inner_df['timestamp'] = pd.to_datetime(inner_df['timestamp'])
    inner_df[time_unit]= inner_df['timestamp'].map(lambda dt: dt.strftime(time_unit_extraction_code)) # for ech element there f(datetime)
    inner_df = inner_df.loc[:,[time_unit,'x']]
    inner_df = inner_df.groupby(time_unit, sort = True, as_index = True).apply(measure_extraction_func)
    print('Aggregate by %s, return %s of x'%  (time_unit, measure))
    return inner_df.loc[:,['x']].rename(columns={ "x": "x_agg"})
    
# Running first method   
first_method = True
if first_method :
    time_units=['year', 'month', 'week', 'weekday', 'day']
    measures=['min','max','count','median','mean']
    
    for time_unit in time_units:
        for measure in measures:
            result = transformation_dict(mydf, time_unit, measure)
            print(result)

# Running second method            
second_method = False
if second_method :
    time_units=['year', 'month', 'week', 'weekday', 'day']
    measures=['count'] #'min','max',,'median','mean'
    
    for time_unit in time_units:
        for measure in measures:
            result = transformation_by_str(mydf, time_unit, measure)
            print(result)