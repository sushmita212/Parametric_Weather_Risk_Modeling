import pandas as pd
import re
import calendar



def drop_unwanted_cols(df):
    """Remove columns that are not needed for analysis."""

    unwanted_cols = ['SOURCE', 'DATA_SOURCE', 'CATEGORY','EPISODE_NARRATIVE', 
                     'EVENT_NARRATIVE','STATE_FIPS','CZ_FIPS','TOR_OTHER_CZ_NAME', 
                     'TOR_OTHER_CZ_FIPS', 'TOR_OTHER_CZ_STATE','WFO','TOR_OTHER_WFO']
    
    df.drop(columns=unwanted_cols, errors='ignore', inplace=True)
     



def clean_id_cols(df):
    """Clean ID columns and cast them to category type"""
    
    # When df is the merged dataframe from details, fatalities, and locations. We get 'EPISODE_ID_x' and 'EPISODE_ID_y' 
    # form details and locations, but they are duplicates. We can drop one and rename the other.
    df.rename(columns={'EPISODE_ID_x': 'EPISODE_ID'}, inplace=True)
    df.drop(columns=['EPISODE_ID_y'], inplace=True)


    # Identify all ID columns
    ID_cols = [col for col in df.columns if re.search(r'_ID$', col.upper())]

    # Convert ID columns to category type
    for col in ID_cols:
        df[col] = df[col].astype('category') 



def clean_timing_cols(df):
    """
    Cleans timing columns in the dataframe. Modifies the dataframe in place with the following columns:
    - YEAR, BEGIN_MONTH, END_MONTH, BEGIN_MONTH_NAME
    - BEGIN_DAY, END_DAY, FAT_DAY
    - DURATION_DAYS (computed from datetime)
    Drops redundant _YEARMONTH and _TIME columns.
    """

    # YEAR
    df['YEAR'] = df['YEAR'].astype(int)
    
    # Create BEGIN_MONTH and END_MONTH from BEGIN_YEARMONTH, END_YEARMONTH columns 
    df['BEGIN_MONTH']=df['BEGIN_YEARMONTH'].astype(str).str[-2:].astype(int)
    df['END_MONTH']=df['END_YEARMONTH'].astype(str).str[-2:].astype(int)

    # Create BEGIN_MONTH_NAME categorical
    df['BEGIN_MONTH_NAME'] = df['BEGIN_MONTH'].apply(lambda x: calendar.month_abbr[x])
    df['BEGIN_MONTH_NAME'] = pd.Categorical(
        df['BEGIN_MONTH_NAME'],
        categories=list(calendar.month_abbr)[1:],  # Jan→Dec
        ordered=True
    )
 
    # Compute DURATION_DAYS using datetime columns
    # parse datetimes
    df['BEGIN_DATE_TIME'] = pd.to_datetime(df['BEGIN_DATE_TIME'], errors='coerce')
    df['END_DATE_TIME']   = pd.to_datetime(df['END_DATE_TIME'], errors='coerce')

    # duration (cross-month handled correctly)
    bd = df['BEGIN_DATE_TIME'].dt.floor('D')
    ed = df['END_DATE_TIME'].dt.floor('D')
    df['DURATION_DAYS'] = (ed - bd).dt.days + 1

    # FAT_DAY to Int64 (nullable integer)
    # df['FAT_DAY'] = pd.to_numeric(df['FAT_DAY'], errors='coerce').astype('Int64')
    df['FAT_DAY'] = df['FAT_DAY'].astype('Int64')

    # drop unused timing columns
    drop_cols = [c for c in df.columns if any(key in c.upper() for key in ['YEARMONTH', '_TIME','_DATE'])]
    df.drop(columns=drop_cols, inplace=True, errors='ignore')
    df.drop(columns=['MONTH_NAME'], inplace=True, errors='ignore')  # redundant with BEGIN_MONTH_NAME
