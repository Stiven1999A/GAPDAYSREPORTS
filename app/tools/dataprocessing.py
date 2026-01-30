"""Data processing utilities for the GapDaysReports application.
"""
from threading import local
import pandas as pd
import numpy as np
from sqlalchemy import text
from tools.generate_charts import weekly_bar_chart, daily_bar_chart
from tools.png_report_generator import generate_png_report
from pathlib import Path
from tools.config import DICT_COL_NAMES, CHART_COLUMNS

def load_data(conn, PTO_date: str) -> pd.DataFrame:
    """Loads data from the database into a DataFrame."""
    # Read from the reporting view containing daily employee hours summary
    PTO_date = pd.to_datetime(PTO_date)
    start_date = (PTO_date - pd.Timedelta(days=26)).strftime('%Y-%m-%d')
    end_date = PTO_date.strftime('%Y-%m-%d')
    query = f"""SELECT * FROM vw_VT_DailyEEHoursSummary
                WHERE AT_Date BETWEEN '{start_date}' AND '{end_date}'
                AND EmployeeTypeDescription = 'Full-time'
                AND EmployeeStatusDescription = 'Active'
                AND [Company Project Code Desc Only] NOT LIKE '1000%'
                AND [Company Project Code Desc Only] NOT LIKE '1050%'
                AND [Company Project Code Desc Only] NOT LIKE '3300%'
                AND [Company Project Code Desc Only] NOT LIKE '8600%';
                """

    result = conn.execute(text(query))
    df = pd.DataFrame(
                    result.fetchall(),
                    columns=result.keys()
                    )
    return df

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the DataFrame by handling missing values and converting data types."""

    df = df.rename(columns=DICT_COL_NAMES)
    df[CHART_COLUMNS] = df[CHART_COLUMNS].fillna(0).astype(float)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Week'] = df['Date'].dt.to_period('W-SAT').dt.start_time
    df['Total Hours'] = df[CHART_COLUMNS].sum(axis=1)
    return df

def generate_gapdays_reports(df: pd.DataFrame, input_folder_path: str, output_folder_path: str):
    """Identifies users with gap days (users which at least on weekly daily productive average is less than 2 hours)."""
    print("Segmenting users with gap days...")
    df['Not_Prod_Weekend'] = df['Date'].dt.weekday.isin([5,6]) & (df['Total Hours'] == 0)
    df = df[~df['Not_Prod_Weekend']]
    weekly_df = df.groupby(['EEID', 'Week'], as_index=False).agg(
                                                                {col: 'sum' for col in CHART_COLUMNS} |
                                                                {'Total Hours': 'mean'}
                                                                ).reset_index()
    weekly_df.rename(columns={'Total Hours': 'Daily Productive Average'}, inplace=True)
    weekly_df['Total Hours'] = weekly_df[CHART_COLUMNS].sum(axis=1)
    eeid_with_gaps = weekly_df[weekly_df['Daily Productive Average'] < 2]['EEID'].unique()
    print(f"Total users analyzed: {df['EEID'].nunique()}")
    print(f"Found {len(eeid_with_gaps)} users with gap days.")
    print(f"The proportion of users with gap days is {len(eeid_with_gaps) / df['EEID'].nunique()}")
    weekly_gap_days_df = weekly_df[weekly_df['EEID'].isin(eeid_with_gaps)].reset_index(drop=True)
    # Text Parameters
    week_start = min(df['Week']).strftime('%b %d, %Y')
    week_end = (max(df['Week']) + pd.Timedelta(days=6)).strftime('%b %d, %Y')
    j = 0
    print("Generating PNG report...")
    for eeid in eeid_with_gaps:
        user_df = df[df['EEID'] == eeid]
        user_name = f"{str(user_df['FName'].iloc[0]).title()} {str(user_df['LName'].iloc[0]).title()}" if pd.isna(user_df['AT_UserName'].iloc[0]) else str(user_df['AT_UserName'].iloc[0]).title()
        emp_weekly_gap_days_df = weekly_gap_days_df[(weekly_gap_days_df['EEID'] == eeid)]
        weekly_bar_chart(emp_weekly_gap_days_df, input_folder_path)
        
        for i, week in enumerate(sorted(df['Week'].unique())):
            week_df = user_df[user_df['Week'] == week]
            week_time_df = week_df[
                                    ['Date',
                                     'Productive Active Hours',
                                      'Productive Passive Hours',
                                      'Holiday Hours',
                                      'PTO Hours',
                                      'Undefined Hours',
                                      'Unproductive Hours',
                                      'Total Hours',]
                                    ].sort_values('Date')

            week_time_df['Daily Productive Accumulated Average'] = (
                                                                    week_time_df['Total Hours']
                                                                    .expanding()
                                                                    .mean()
                                                                    )
            daily_bar_chart(week_time_df, input_folder_path,week, f"daily_productive_hours_week{i + 1}")

        def create_text_parameters():
            # Title
            title = f"Gap Days Report ({week_start} - {week_end})"

            # User Info parameters
            reports_to = "[Manager Name]"
            days_zero_prod = user_df[user_df['Total Hours'] == 0].shape[0]
            days_zero_prod_proportion = days_zero_prod / user_df.shape[0]
            weeks_below_threshold = emp_weekly_gap_days_df[emp_weekly_gap_days_df['Daily Productive Average'] < 2].shape[0]
            # User info
            employee_info = f"Employee ID: {eeid}.|Name: {user_name}.|Reports To: {reports_to}.|Total Days with Zero Productive Hours: {days_zero_prod} ({days_zero_prod_proportion:.2%}).|Total Weeks Where Daily Productive Average is Below Threshold (2 hours): {weeks_below_threshold}."
            
            # Drescription
            description = f"""How to read this report?|The chart below displays the user's weekly working hours. Each bar corresponds to a specific category, as described in the legend beneath the chart. The magenta line shows the trend of the user's average hours worked each week, and the markers with data labels indicate the exact average for that week.|To dive deeper into each week, refer to the auxiliary charts on the right-hand side. These charts are arranged chronologically from top to bottom, with each one representing a single week. The bars show the total hours worked per day, the red arrows highlight days with zero activity, and the blue line represents the trend of the accumulated average working hours. The magenta value at the end of the line emphasizes the final average hours worked for that week."""
            return (title, employee_info, description)
        
        text_parameters = create_text_parameters()
        input_folder = Path(input_folder_path)
        output_folder_reports = Path(f"{output_folder_path}png_reports/")
        generate_png_report(text_parameters, input_folder, output_folder_reports, f"Gap Days Report - {eeid} {user_name}")
        if j == 1:
            break
        j += 1
    # Save CSV dataset
    print('Saving CSV dataset...')
    df_to_save = df[['Date', 'Week', 'EEID', 'AT_UserName', 'FName', 'LName', 'EmployeeTypeDescription', 'Title', 'Company Project Code Desc Only', 'Location']].copy()
    df_to_save['Gap_Status'] = df_to_save['EEID'].apply(lambda x: 'Gap' if x in eeid_with_gaps else 'No Gap')
    df_to_save.to_csv(f"{output_folder_path}csv_datasets/GapDaysDataset_{week_start}_{week_end}.csv", index=False)
