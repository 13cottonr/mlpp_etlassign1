# -*- coding: utf-8 -*-
"""
Created on Sun Sep  5 15:28:54 2021

@author: Rebecca Cotton
Date: 9/7/2021
MLPP Assignment 1
"""

"""
This assignment collects data from the state of Hawaii about energy sources to heat homes 
and demographic data at the block track census level. As the US continues to shift towards
more renewable energy sources, it could be interesting to examine how the number of solar-
heated homes compares to the number of homes heated by conventional fuels, like gas. It would 
also be interesting to see if these numbers are correlated with personal income. Hawaii 
offers an interesting case study because it is an island, and therefore may have a different
energy portfolio than states on the mainland. 

I chose to use the 5 year census estimates centered on the year 2019 because a) 2019 was the 
most recent year of data collection. Additionally, since this assignment is examining small 
sample sizes at the block track level, a 5-year estimate will lessen the chance of errors driven
by random variation in the data. 
"""

import pandas as pd
import requests
import json
import numpy as np
import ohio
import ohio.ext.pandas
from sqlalchemy import create_engine
import psycopg2


# Retrieve API Census data 

apiKey = 'e23d9e095ab4a8979e065449c534696d6058c716'

# example = api.census.gov/data/2019/acs/acs5?get=NAME,group(B01001)&for=us:1&key=YOUR_KEY_GOES_HERE
# https://api.census.gov/data/2019/acs/acs5?get=NAME,B01001_001E&for=block%20group:*&in=state:01%20county:001&key=YOUR_KEY_GOES_HERE
# API coding info: https://www.census.gov/data/developers/guidance/api-user-guide.Core_Concepts.html
# API state number codes: https://www23.statcan.gc.ca/imdb/p3VD.pl?Function=getVD&TVD=53971

### Only at national level; can't use for block level work
# windTech = 'B24124_463E' # fulltime, year-round civilian employed wind turbine techs 16 yrs and older 
# windSal = 'B24121_463E' # median salary for windTech
# solarTech = 'B24124_418E' # fulltime, year-round civilian employed solar photovoltic installers 16 yrs and older 
# solarSal = 'B24121_418E' # median salary for solarTech 

gasEnergy = 'B25040_002E' # number of homes using gas for home heating 
solarEnergy = 'B25040_008E' # number of homes using solar energy for home heating 
pop = 'B01003_001E' # est total population
medSal = 'B20017_001E' # median salary for all 
salMale = 'B20017_003E' # median salary for males fulltime, year-round 16 yrs and older
salFemale = 'B20017_006E' # median salary for females fulltime, year-round 16 yrs and older

#http2 = 'https://api.census.gov/data/2019/acs/acs5?get=B24124_463E,B01003_001E&for=block%20group:*&in=state:01%20county:001&key=e23d9e095ab4a8979e065449c534696d6058c716'

http = 'https://api.census.gov/data/2019/acs/acs5?get='+'NAME'+','+gasEnergy+','+solarEnergy+','+pop+','+medSal+','+salMale+','+salFemale+'&for=block%20group:*&in=state:15%20county:*&key='+apiKey


response = requests.get(http, headers={"Content-type": "application/json"})
try:
    response.status_code == 200
except:
    print("error")

dictData = json.loads(response.content.decode("utf-8")) 
dfData = pd.DataFrame(dictData[1:], columns = ['Name', 'Gas_Heat', 'Solar_Heat', 'Pop', 'Med_Sal', 
                                               'Med_Male_Sal', 'Med_Female_Sal', 'State_No', 
                                               'County_No', 'Track', 'Block'])

# Transform data 
# Note cells with -666666666 are NAs

# dfData.dtypes # all objects

# Change numeric columns into numeric datatypes 
dfData[['Gas_Heat', 'Solar_Heat', 'Pop', 'Med_Sal', 
        'Med_Male_Sal', 'Med_Female_Sal', 'State_No', 
        'County_No', 'Track', 'Block']] = dfData[['Gas_Heat', 'Solar_Heat', 'Pop', 'Med_Sal', 
                                                  'Med_Male_Sal', 'Med_Female_Sal', 'State_No', 
                                                  'County_No', 'Track', 'Block']].apply(pd.to_numeric)

# Split Name column into appropriate descriptors 
dfData[['Block_del', 'Census_del', 'County', 'State']] = dfData['Name'].str.split(", ", expand = True)

# 'Name', 'Block_del', and 'Census_del' don't have unique or descriptive info. Delete columns
dfData.drop(labels = ['Name', 'Block_del', 'Census_del'], axis = 1, inplace = True)

# Replace -666666666 with NaN
dfData.replace(-666666666, np.nan, inplace = True)

# Add primary key 
dfData['Index'] = dfData.index 

# Save data as CSV to upload to Postgre -- did not need to do this
# dfData.to_csv('rcotton_acs.csv')





# Connecting to Postgre 

# dialect+driver://username:password@host:port/database
# Set schema to 'acs', use connect_args in engine 

conn_string = 'postgresql://mlpp_student:CARE-horse-most@acs-db.mlpolicylab.dssg.io:5432/acs_data_loading'
engine = create_engine(conn_string, connect_args={'options': '-csearch_path={}'.format('acs')})

dfData.pg_copy_to('rcotton_acs_data', engine)


# For exploring database 
'''
connection = psycopg2.connect(
    host="acs-db.mlpolicylab.dssg.io",
    database="acs_data_loading",
    user="mlpp_student",
    password="CARE-horse-most",
)
connection.autocommit = True

def create_table(cursor) -> None:
    cursor.execute("""
        DROP TABLE IF EXISTS rcotton_acs_data;
        CREATE TABLE rcotton_acs_data (
            Gas_Heat           int,
            Solar_Heat         int,
            Pop                int,
            Med_Sal            float(2),
            Med_Male_Sal       float(2),
            Med_Female_Sal     float(2),
            State_No           int,
            County_No          int, 
            Track              int,
            Block              int,
            County             varchar(50), 
            State              varchar(20), 
            Index              int
        );
    """)
    
def drop_table(cursor) -> None:
    cursor.execute("""
        DROP TABLE rcotton_acs_data2;
    """)

with connection.cursor() as cursor:
    drop_table(cursor)
'''


