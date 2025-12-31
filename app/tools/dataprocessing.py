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
    query = f"""SELECT * FROM vw_VT_DailyEEHoursSummary
                WHERE AT_Date BETWEEN '{(PTO_date - pd.Timedelta(days=26)).strftime('%Y-%m-%d')}' AND '{PTO_date.strftime('%Y-%m-%d')}'
                AND Employee_ID IN ('A25969')"""
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
