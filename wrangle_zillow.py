# import

import pandas as pd
import numpy as np
import os

from env import get_db_url

#stops truncating of columns and rows
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


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
        WHERE propertylandusetypeid = 261  AND transactiondate like '2017%%';
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
    prop_required_column = prop_required_column * df.shape[0]
        
    prop_required_row = prop_required_row * df.shape[1]
    
    df = df.dropna(axis='columns', thresh=prop_required_column)
    
    df = df.dropna(thresh = prop_required_row)
    
    return df


def clean_zillow (df):
    ''' THIS FUNCTION IS INCOMPLETE'''

    df = df.sort_values(by='transactiondate').drop_duplicates(subset='parcelid', keep='last')

    df = df.drop(columns='id.1')





############################################  VISUALIZE  ###############################################

def null_info(df):
    '''This function returns a table with the number of nulls in each column
        and the percent of the data it comprises.'''
    
    nulls_table = pd.DataFrame(columns=['feature', 'num_rows_missing', 'percent_rows_missing'])
    
    for col in df.columns
        
        num_rows_missing = pd.DataFrame({'feature': [col],'num_rows_missing': [df[col].isnull().sum()], 'percent_rows_missing': [round((df[col].isnull().sum() / df.shape[0]) * 100, 2)]})
        nulls_table = pd.concat([nulls_table, num_rows_missing] , ignore_index=True)

    
    return nulls_table