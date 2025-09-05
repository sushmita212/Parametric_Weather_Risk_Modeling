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
    if 'EPISODE_ID_x' in df.columns and 'EPISODE_ID_y' in df.columns:
        df.rename(columns={'EPISODE_ID_x': 'EPISODE_ID'}, inplace=True)
        df.drop(columns=['EPISODE_ID_y'], inplace=True)


    # Identify all ID columns
    ID_cols = [col for col in df.columns if re.search(r'_ID$', col.upper())]

    # Convert ID columns to category type
    for col in ID_cols:
        df[col] = df[col].astype("Int64")  # capital I, pandas nullable integer
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



def clean_location_cols(df):
    """
    Cleans the location-related columns in NOAA storm events DataFrame in place.
    
    Operations:
    - Keeps only relevant location columns
    - Casts categorical columns to 'category' type
    - Standardizes CZ_TYPE values
    - Strips whitespace from BEGIN_LOCATION
    - Creates a combined LOCATION_LABEL for tooltips
    - Does NOT drop any rows (keep missing coordinates)
    
    Changes are applied in place.
    """
    # Identify location-related columns
    location_cols = [col for col in df.columns 
                 if any(key in col.upper() for key in ['STATE', 'LAT', 'LON', 'LOCATION', 'RANGE', 'AZIMUTH', 'CZ_'])]
    # Keep only relevant columns (in place)
    keep_cols = ['STATE', 'CZ_TYPE', 'CZ_NAME', 'BEGIN_LOCATION', 'BEGIN_LAT', 'BEGIN_LON']
    for col in location_cols:
        if col not in keep_cols:
            df.drop(columns=col, inplace=True)
    
    # Cast categorical columns
    cat_cols = ['STATE', 'CZ_TYPE', 'CZ_NAME', 'BEGIN_LOCATION']
    for col in cat_cols:
        df[col] = df[col].astype('category')
    
    # Standardize CZ_TYPE
    df['CZ_TYPE'] = df['CZ_TYPE'].map({'C': 'County', 'Z': 'Zone'}).astype('category')
    
    # Clean BEGIN_LOCATION strings
    df['BEGIN_LOCATION'] = df['BEGIN_LOCATION'].str.strip()
    
    # Create combined LOCATION_LABEL for tooltips
    df['LOCATION_LABEL'] = (
        df['BEGIN_LOCATION'].fillna('') + ', ' +
        df['CZ_NAME'].astype(str) + ', ' +
        df['STATE'].astype(str)
    ).str.strip(', ')



def clean_damage_cols(df):
    """
    Cleans damage-related columns in NOAA storm events DataFrame.
    Converts damage amount columns to numeric, handling suffixes like 'K', 'M', 'B'.
    Converts count columns (fatalities, injuries) to Int64 (nullable integer).
    """
    # Drop unwanted columns
    # 'FATALITY_TYPE' is redudant with 'DEATHS_DIRECT' and 'DEATHS_INDIRECT'
    # We don't want to do demographic analysis so we can drop 'FATALITY_AGE' and 'FATALITY_SEX'
    cols_to_drop = ['FATALITY_TYPE','FATALITY_AGE','FATALITY_SEX']
    df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    def parse_damage(val):
        """
        Convert damage strings like '25K', '2.5M', '100B' into floats (dollars).
        Returns pd.NA if invalid.
        """
        if pd.isna(val):
            return pd.NA
        val = str(val).upper().strip()  # normalize
        multipliers = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
        if val[-1] in multipliers:
            try:
                return float(val[:-1]) * multipliers[val[-1]]
            except ValueError:
                return pd.NA
        try:
            return float(val)
        except ValueError:
            return pd.NA

    # Parse damage amount columns
    damage_amount_cols = ['DAMAGE_CROPS', 'DAMAGE_PROPERTY']
    for col in damage_amount_cols:
        if col in df.columns:
            df[col] = df[col].map(parse_damage)
            df[col] = pd.to_numeric(df[col], errors='coerce')  # converts to float dtype with NaNs
    # Create TOTAL_DAMAGE column (sum crops + property)
    if all(col in df.columns for col in damage_amount_cols):
        df["TOTAL_DAMAGE"] = df['DAMAGE_PROPERTY'].fillna(0) + df['DAMAGE_CROPS'].fillna(0)
        df["TOTAL_DAMAGE"].replace(0, pd.NA, inplace=True)  # optional: treat 0 as missing

           


def clean_severity_cols(df):
    """
    Cleans severity-related columns in NOAA storm events DataFrame.

    """
    # drop flood cause as it is mostly heavy rain
    df.drop(columns=['FLOOD_CAUSE'], inplace=True, errors='ignore')

    # Map TOR_F_SCALE scale to numeric
    tor_scale_map = {
        "EF0": 0, "EF1": 1, "EF2": 2, "EF3": 3, "EF4": 4, "EF5": 5
    }
    df["TOR_F_SCALE"] = df["TOR_F_SCALE"].map(tor_scale_map).astype("Int64")



# cleaning.py
import pandas as pd

def load_yearly_and_clean(year, base_dir="../data"):
    """Load and clean NOAA data for a given year (in place)."""
    
    fileDName = f"{base_dir}/details/details_{year}.csv.gz"
    fileFName = f"{base_dir}/fatalities/fatalities_{year}.csv.gz"
    fileLName = f"{base_dir}/locations/locations_{year}.csv.gz"

    df_d = pd.read_csv(fileDName)
    df_f = pd.read_csv(fileFName)
    df_l = pd.read_csv(fileLName)

    # Merge
    df_year = df_d.merge(df_f, on="EVENT_ID", how="left")
    df_year = df_year.merge(df_l, on="EVENT_ID", how="left")

    # Clean (mutates in place)
    drop_unwanted_cols(df_year)
    clean_id_cols(df_year)
    clean_timing_cols(df_year)
    clean_location_cols(df_year)
    clean_damage_cols(df_year)
    clean_severity_cols(df_year)
    
    return df_year
