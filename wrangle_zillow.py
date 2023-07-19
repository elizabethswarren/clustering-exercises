# import

import pandas as pd
import numpy as np
import os

from env import get_db_url

#stops truncating of columns and rows
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

############################################  WRANGLE  ###############################################

def wrangle_zillow():

    df = pd.read_csv('zillow.csv')

    df = clean_zillow(df)

    return df

############################################  ACQUIRE  ###############################################

def get_zillow_data():
    
    filename = 'zillow.csv'
    
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    
    else:
    
        url = get_db_url('zillow')
    
        sql = '''SELECT *

        FROM properties_2017
        FULL JOIN predictions_2017 USING (parcelid)
        LEFT JOIN airconditioningtype USING (airconditioningtypeid)
        LEFT JOIN architecturalstyletype USING (architecturalstyletypeid)
        LEFT JOIN buildingclasstype USING (buildingclasstypeid)
        LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid)
        LEFT JOIN propertylandusetype USING (propertylandusetypeid)
        LEFT JOIN storytype USING (storytypeid)
        LEFT JOIN typeconstructiontype USING (typeconstructiontypeid)
        WHERE transactiondate <= '2017-12-31';
        '''
    
        df = pd.read_sql(sql, url)
        
        df.to_csv(filename, index=False)
    
        return df

############################################  PREPARE  ###############################################


def handle_missing_values(df, prop_required_column, prop_required_row):
    '''This function takes in a dataframe, the desired threshold proportion, between 0 and 1, to drop null values from columns,
        and desired threshold proportion, between 0 and 1, to drop null values from rows. It drops the columns first
        and the rows next, and it returns the df.'''
    
    # computes proportion
    col_thresh = int(prop_required_column * df.shape[0])
    
    df = df.dropna(axis=1, thresh=col_thresh)
        
    row_thresh = int(prop_required_row * df.shape[1])
    
    df = df.dropna(axis=0, thresh=row_thresh)
    
    return df


def clean_zillow (df):
    ''' THIS FUNCTION IS INCOMPLETE'''

    # use only single residential properties
    single_use = [261, 262, 263, 264, 266, 268, 273, 276, 279]
    df = df[df.propertylandusetypeid.isin(single_use)]
    
    # drop the duplicate parcelid with the earliest date
    df = df.sort_values(by='transactiondate').drop_duplicates(subset='parcelid', keep='last')

    # drop all rows and columns if they are 60% nulls or more.
    df = handle_missing_values(df, .6, .6)

    # drop columns
    df = df.drop(columns = ['id', 'id.1','finishedsquarefeet12', 'fullbathcnt', 'heatingorsystemtypeid', 'heatingorsystemdesc', 'calculatedbathnbr',
    'propertycountylandusecode', 'propertylandusetypeid','propertyzoningdesc', 'censustractandblock', 'propertylandusedesc', 'unitcnt', 'assessmentyear',
    'regionidzip', 'regionidcounty'])

    # fill nulls in buildingqualitytypeid with median
    df.buildingqualitytypeid = df.buildingqualitytypeid.fillna(6.0)

    # fill nulls in calculatedfinishedsquarefeet with median
    df.calculatedfinishedsquarefeet = df.calculatedfinishedsquarefeet.fillna(1543.0)

    # fill nulls in lotsizesquarefeet with median
    df.lotsizesquarefeet = df.lotsizesquarefeet.fillna(7206.0)

    #drop the rest of nulls
    df = df.dropna()

    return df





############################################  DATA INFO  ###############################################

def null_info(df):
    '''This function returns a table with the number of nulls in each column
        and the percent of the data it comprises.'''
    
    nulls_table = pd.DataFrame(columns=['feature', 'num_rows_missing', 'percent_rows_missing'])
    
    for col in df.columns:
        
        num_rows_missing = pd.DataFrame({'feature': [col],'num_rows_missing': [df[col].isnull().sum()], 'percent_rows_missing': [round((df[col].isnull().sum() / df.shape[0]) * 100, 2)]})
        nulls_table = pd.concat([nulls_table, num_rows_missing] , ignore_index=True)

    
    return nulls_table