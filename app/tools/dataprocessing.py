"""Data processing utilities for the GapDaysReports application.
"""
import pandas as pd
import numpy as np
from sqlalchemy import text
from connections import alchemy_connection
from config import DICT_COL_NAMES, CHART_COLUMNS

def load_data(conn, PTO_date) -> pd.DataFrame:
    """Loads data from the database into a DataFrame."""
    # Read from the reporting view containing daily employee hours summary
    start_date = (PTO_date - pd.Timedelta(days=26)).strftime('%Y-%m-%d')
    end_date = PTO_date.strftime('%Y-%m-%d')
    query = f"""SELECT * FROM vw_VT_DailyEEHoursSummary
                WHERE AT_Date BETWEEN '{start_date}' AND '{end_date}'
                AND EmployeeTypeDescription = 'Full-time'
                AND EmployeeStatusDescription = 'Active'
                AND [Company Project Code Desc Only] NOT LIKE '1000%'
                AND [Company Project Code Desc Only] NOT LIKE '%1050%'
                AND [Company Project Code Desc Only] NOT LIKE '%3300%'
                AND [Company Project Code Desc Only] NOT LIKE '%8600%';
                """

    result = conn.execute(text(query))
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the DataFrame by handling missing values and converting data types."""

    df = df.rename(columns=DICT_COL_NAMES)
    df[CHART_COLUMNS] = df[CHART_COLUMNS].fillna(0).astype(float)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Week'] = df['Date'].dt.to_period('W-SAT').dt.start_time
    df['Total Productive'] = df[['Productive Active Hours', 'Productive Passive Hours', 'PTO Hours', 'Holiday Hours']].sum(axis=1)
    return df

def gap_days_identifier(df: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray]:
    """Identifies users with gap days (users which at least on weekly daily productive average is less than 2 hours)."""
    weekly_df = df.groupby(['EEID', 'Week']).agg({col: 'sum' for col in CHART_COLUMNS} | {'Total Productive': lambda x: x[x > 0].mean()}).reset_index()
    weekly_df.rename(columns={'Total Productive': 'Daily Productive Average'}, inplace=True)
    weekly_df = weekly_df.reset_index()
    eeid_aux = weekly_df[weekly_df['Daily Productive Average'] < 2]['EEID'].unique()
    gap_days_df = weekly_df[weekly_df['EEID'].isin(eeid_aux)]
    gap_days_df['Total Hours'] = gap_days_df[CHART_COLUMNS].sum(axis=1)
    gap_days_df['Weekly Productivity Accumulated Average'] = (
        gap_days_df['Total Hours']
        .expanding()
        .mean()
    )
    return gap_days_df, eeid_aux

def prepare_data_weekly_chart(df: pd.DataFrame) -> pd.DataFrame:
    """Prepares data for weekly charting."""
    
    weekly_df = df.groupby('Week').agg({col: 'sum' for col in CHART_COLUMNS} | {'Total Productive': lambda x: x[x > 0].mean()}).reset_index()
    weekly_df.rename(columns={'Total Productive': 'Daily Productive Average'}, inplace=True)
    weekly_df['Total Hours'] = weekly_df[CHART_COLUMNS].sum(axis=1)
    weekly_df['Weekly Productivity Accumulated Average'] = (
        weekly_df['Total Hours']
        .expanding()
        .mean()
    )
    return weekly_df

def create_report(df):
    """Generates a report from the preprocessed DataFrame."""
    for employee_id in df['EEID'].unique():
        emp_df = df[df['EEID'] == employee_id]
        create_charts(emp_df)
        create_report(emp_df)
