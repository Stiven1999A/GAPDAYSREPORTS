"""Data processing utilities for the GapDaysReports application.
"""
import os
import time
import pandas as pd
import numpy as np
from sqlalchemy import text
from tools.utils import eeids_reports_cache
from tools.connections import alchemy_connection
from tools.generate_charts import weekly_bar_chart, daily_bar_chart
from tools.png_report_generator import generate_png_report
from pathlib import Path
from tools.config import DICT_COL_NAMES, CHART_COLUMNS, EMPLOYEE_IDS
from tqdm import tqdm

def generate_query(report_type=1) -> str:
    """Generates the SQL query to fetch data."""
    # Read from the reporting view containing daily employee hours summary
    inputed_start_date = input("Enter the start date (YYYY-MM-DD): ")
    inputed_end_date = input("Enter the end date (YYYY-MM-DD): ")
    try:
        start_date = pd.to_datetime(inputed_start_date)
        end_date = pd.to_datetime(inputed_end_date)
    except ValueError:
        raise ValueError("Invalid date format. Please enter the date in YYYY-MM-DD format.")
    if report_type == 1:
        query = f"""SELECT * FROM vw_VT_DailyEEHoursSummary
                    WHERE AT_Date BETWEEN '{start_date}' AND '{end_date}'
                    AND EmployeeTypeDescription = 'Full-time'
                    AND EmployeeStatusDescription = 'Active'
                    AND [Company Project Code Desc Only] NOT LIKE '1000%'
                    AND [Company Project Code Desc Only] NOT LIKE '1050%'
                    AND [Company Project Code Desc Only] NOT LIKE '3300%'
                    AND [Company Project Code Desc Only] NOT LIKE '8600%';
                    """
    elif report_type == 3:
        query = f"""SELECT * FROM vw_VT_DailyEEHoursSummary
                WHERE AT_Date BETWEEN '{start_date}' AND '{end_date}'
                AND Employee_ID IN ({','.join(f"'{key}'" for key in EMPLOYEE_IDS.keys())});
                """
    return query

def load_data(conn, query) -> pd.DataFrame:
    """Loads data from the database into a DataFrame."""
    result = conn.execute(text(query))
    print(result)
    df = pd.DataFrame(
                    result.fetchall(),
                    columns=result.keys()
                    )
    return df
    
def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the DataFrame by handling missing values and converting data types."""

    df = df.rename(columns=DICT_COL_NAMES)
    agg_map = {col: 'sum' if pd.api.types.is_numeric_dtype(dtype) else 'first'
            for col, dtype in df.dtypes.items()}

    df[CHART_COLUMNS] = df[CHART_COLUMNS].fillna(0).astype(float)
    df['Date'] = pd.to_datetime(df['Date'])
    df_grouped = (
        df.groupby(['Date', 'EEID'], as_index=False)
        .agg(agg_map)
    )
    df_grouped['Week'] = df_grouped['Date'].dt.to_period('W-SAT').dt.start_time
    df_grouped['Productive Only'] = df_grouped[['Productive Active Hours', 'Productive Passive Hours', 'Undefined Hours', 'Unproductive Hours']].sum(axis=1)
    df_grouped['Total Hours'] = df_grouped[CHART_COLUMNS].sum(axis=1)
    return df_grouped

def delete_files(folder_path):
    """Deletes all files in the specified directory."""
    if folder_path.exists():
        for file in folder_path.glob('*'):
            if file.is_file():
                file.unlink()

def delete_weekend_zero_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Deletes weekend rows where total hours are zero."""
    df = df.copy()
    df['Not_Prod_Weekend'] = df['Date'].dt.weekday.isin([5,6]) & (df['Total Hours'] == 0)
    df = df[~df['Not_Prod_Weekend']]
    return df

def custom_weekly_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregates data on a weekly basis."""
    weekly_df = df.groupby(['EEID', 'Week'], as_index=False).agg(
                                                                {col: 'sum' for col in CHART_COLUMNS} |
                                                                {'Total Hours': 'mean'} |
                                                                {'Productive Only': 'sum'}
                                                                ).reset_index()
    weekly_df.rename(columns={'Total Hours': 'Daily Productive Average'}, inplace=True)
    weekly_df['Total Hours'] = weekly_df[CHART_COLUMNS].sum(axis=1)
    return weekly_df

def filter_missing_prod_users(weekly_df: pd.DataFrame) -> pd.DataFrame:
    """Filters users with missing productive hours (weekly total hours is 0)."""
    df_zero_prod = weekly_df.groupby('EEID', as_index=False)['Productive Only'].sum()
    eeid_missing_prod = df_zero_prod[df_zero_prod['Productive Only'] == 0]['EEID'].unique()
    filtered_missing_df = weekly_df[weekly_df['EEID'].isin(eeid_missing_prod)].reset_index(drop=True)
    return filtered_missing_df, eeid_missing_prod

def filter_gap_days_users(weekly_df: pd.DataFrame, eeid_missing_prod) -> pd.DataFrame:
    """Filters users with gap days (weekly daily productive average less than 2 hours)."""
    weekly_df = weekly_df[~weekly_df['EEID'].isin(eeid_missing_prod)].reset_index(drop=True)
    eeid_with_gaps = weekly_df[weekly_df['Daily Productive Average'] < 2]['EEID'].unique()
    filtered_gaps_df = weekly_df[weekly_df['EEID'].isin(eeid_with_gaps)].reset_index(drop=True)
    return filtered_gaps_df, eeid_with_gaps

def retrieve_username(eeid, reports_to=False):
    """
    Docstring for retrieve_username
    """
    
    if reports_to:
        query = f"""
                SELECT DISTINCT Reports_To
                FROM vw_VT_DailyEEHoursSummary
                WHERE Employee_ID = '{eeid}'
                AND Reports_To IS NOT NULL;
                """
    else:
        query = f"""
                SELECT DISTINCT FName, LName
                FROM vw_VT_DailyEEHoursSummary
                WHERE Employee_ID = '{eeid}'
                AND FName IS NOT NULL
                AND LName IS NOT NULL;
                """
    conn = None
    try:
        conn = alchemy_connection()
        df = load_data(conn, query)
        return f"{str(df.iloc[0, 0]).title()}" if reports_to else f"{str(df.iloc[0, 0]).title()} {str(df.iloc[0, 1]).title()}"
    except Exception as e:
        print(f"Error retrieving user name: {str(e)}")
        raise
    finally:
        # Always close the connection if it was opened
        if conn is not None:
            try:
                conn.close()
            except Exception:
                # Avoid masking the original exception
                pass


def create_text_parameters(
    report_type,
    week_start=None,
    week_end=None,
    eeid=None,
    daily_user_df=None,
    weekly_user_df=None
):
    """
    Build title, employee info, and description strings for reports.

    report_type: 1 = Gap Days, 2 = Zero Productivity, 3 = Productivity
    """

    # ---- Validation / Defaults ----
    if eeid is None:
        raise ValueError("eeid is required.")

    # Ensure DataFrames are provided
    if daily_user_df is None or weekly_user_df is None:
        raise ValueError("Both daily_user_df and weekly_user_df are required.")

    # ---- Title & User Name ----
    if report_type == 1:
        print("Creating Gap Days Report")
        title = f"Gap Days Report ({week_start} - {week_end})"
        user_name = str(retrieve_username(eeid) or "")
    elif report_type == 2:
        print("Creating Zero Productivity Report")
        title = f"Zero Productivity Report ({week_start} - {week_end})"
        user_name = str(retrieve_username(eeid) or "")
    elif report_type == 3:
        print("Creating Productivity Report")
        title = f"Productivity Report ({week_start} - {week_end})"
        # Safely get from EMPLOYEE_IDS with fallback to retrieve_username
        try:
            user_name = str(EMPLOYEE_IDS.get(eeid))  # .get avoids KeyError
        except NameError:
            # EMPLOYEE_IDS may not be defined in some contexts
            user_name = None
        if not user_name or user_name == "None":
            user_name = str(retrieve_username(eeid) or "")
    else:
        # Fallback for unexpected types to avoid UnboundLocalError
        print("Creating Generic Report")
        title = f"Report ({week_start} - {week_end})"
        user_name = str(retrieve_username(eeid) or "")

    # ---- Reports To ----
    reports_to = str(retrieve_username(eeid, reports_to=True) or "")

    # ---- Numeric Safety & Aggregations ----
    # Make sure columns exist
    if 'Total Hours' not in daily_user_df.columns:
        raise KeyError("daily_user_df must contain 'Total Hours' column.")

    # days with zero productive hours
    days_zero_prod = int(daily_user_df['Total Hours'].eq(0).sum())

    # denominator safety
    total_days = int(daily_user_df.shape[0])
    days_zero_prod_proportion = (days_zero_prod / total_days) if total_days > 0 else 0.0

    # weekly threshold counts
    if 'Daily Productive Average' not in weekly_user_df.columns:
        raise KeyError("weekly_user_df must contain 'Daily Productive Average' column.")

    # ensure numeric for comparison
    w = weekly_user_df.copy()
    w['Daily Productive Average'] = pd.to_numeric(
        w['Daily Productive Average'], errors='coerce'
    )
    weeks_below_threshold = int((w['Daily Productive Average'] < 2).sum())

    # User info
    employee_info = f"Employee ID: {eeid}.|Name: {user_name}.|Reports To: {reports_to}.|Total Days with Zero Productive Hours: {days_zero_prod} ({days_zero_prod_proportion:.2%}).|Total Weeks Where Daily Productive Average is Below Threshold (2 hours): {weeks_below_threshold}."
    
    # Drescription
    description = f"""How to read this report?|The chart below displays the user's weekly working hours. Each bar corresponds to a specific category, as described in the legend beneath the chart. The magenta line shows the trend of the user's average hours worked each week, and the markers with data labels indicate the exact average for that week.|To dive deeper into each week, refer to the auxiliary charts on the right-hand side. These charts are arranged chronologically from top to bottom, with each one representing a single week. The bars show the total hours worked per day, the red arrows highlight days with zero activity, and the blue line represents the trend of the accumulated average working hours. The magenta value at the end of the line emphasizes the final average hours worked for that week."""
    return (title, employee_info, description)

def users_chart_creator(daily_df: pd.DataFrame, weekly_df: pd.DataFrame, input_folder_path: str, output_folder_path: str, week_start: str, week_end: str, report_type):
    times = []
    for eeid in tqdm(weekly_df['EEID'].unique(), desc="Processing users"):
        start_time = time.perf_counter()
        daily_user_df = daily_df[daily_df['EEID'] == eeid]
        emp_weekly_gap_days_df = weekly_df[weekly_df['EEID'] == eeid]
        weekly_bar_chart(emp_weekly_gap_days_df, input_folder_path)
        sorted_weeks = sorted(daily_user_df['Week'].unique())
        for i, week in enumerate(sorted_weeks):
            week_df = daily_user_df[daily_user_df['Week'] == week]
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
            daily_bar_chart(week_time_df, input_folder_path, week, f"daily_productive_hours_week{i + 1}")
        text_parameters = create_text_parameters(report_type=report_type, week_start=week_start, week_end=week_end, eeid=eeid, daily_user_df=daily_user_df, weekly_user_df=emp_weekly_gap_days_df)

        if report_type == 1:
            if "gap_reports" not in os.listdir(f"{output_folder_path}"):
                os.makedirs(f"{output_folder_path}gap_reports/")
            output_folder_reports = Path(f"{output_folder_path}gap_reports/")
            user_name = str(retrieve_username(eeid) or "")
            generate_png_report(text_parameters, input_folder_path, output_folder_reports, f"Gap Days Report - {eeid} {user_name}", num_weeks=len(sorted_weeks))
        elif report_type == 2:
            if "zero_prod_reports" not in os.listdir(f"{output_folder_path}"):
                os.makedirs(f"{output_folder_path}zero_prod_reports/")
            output_folder_reports = Path(f"{output_folder_path}zero_prod_reports/")
            user_name = str(retrieve_username(eeid) or "")
            generate_png_report(text_parameters, input_folder_path, output_folder_reports, f"Zero Productivity Report - {eeid} {user_name}", num_weeks=len(sorted_weeks))
        elif report_type == 3:
            user_name = EMPLOYEE_IDS[eeid]
            if f"{eeid} - {user_name}" not in os.listdir(f"{output_folder_path}randy_reports/"):
                os.makedirs(f"{output_folder_path}randy_reports/{eeid} - {user_name}/")
            output_folder_reports = Path(f"{output_folder_path}randy_reports/{eeid} - {user_name}/")
            generate_png_report(text_parameters, input_folder_path, output_folder_reports, f"Productivity Report - {eeid} {user_name} ({week_start} - {week_end})", num_weeks=len(sorted_weeks))

        time.sleep(0.1)
        end_time = time.perf_counter()
        times.append(end_time - start_time)
    print(f"The average time for the creation of one report is {np.mean(times)}")

def generate_gapdays_missingprod_reports(daily_df: pd.DataFrame, input_folder_path: str, output_folder_path: str):
    """Identifies users with gap days (users which at least on weekly daily productive average is less than 2 hours)."""
    print("Segmenting users with gap days...")
    # Parameters
    input_folder = Path(input_folder_path)
    week_start = min(daily_df['Week']).strftime('%b %d, %Y')
    week_end = (max(daily_df['Week']) + pd.Timedelta(days=6)).strftime('%b %d, %Y')

    # Clear input folder if it contains files
    delete_files(input_folder)

    # Remove weekends with zero hours
    cleaned_daily_df = delete_weekend_zero_hours(daily_df)

    # Creating weekly aggregated dataframe for weekly chart generation
    weekly_df = custom_weekly_aggregation(cleaned_daily_df)

    print(f"Total users analyzed: {cleaned_daily_df['EEID'].nunique()}")

    # Determine users with zero productive hours
    weekly_filtered_missing_df, eeid_missing_prod = filter_missing_prod_users(weekly_df)
    print(f"Found {len(eeid_missing_prod)} users with all weeks having zero productive hours.")
    print(f"The proportion of users with all weeks having zero productive hours is {len(eeid_missing_prod) / daily_df['EEID'].nunique()}")
    
    miss_eeids_done = eeids_reports_cache(verbose=False)  # returns a set
    if miss_eeids_done:  # only proceed if there are reports
        miss_eeids_pending = list(set(eeid_missing_prod) - miss_eeids_done)
        if miss_eeids_pending:
            print(f"Proceeding with {len(miss_eeids_pending)} pending EEIDs...")
            users_chart_creator(cleaned_daily_df[cleaned_daily_df['EEID'].isin(miss_eeids_pending)], weekly_filtered_missing_df[weekly_filtered_missing_df['EEID'].isin(miss_eeids_pending)], input_folder_path, output_folder_path, week_start, week_end, report_type=2)
    else:
        print("No reports found in the folder. Skipping removal.")
        users_chart_creator(cleaned_daily_df[cleaned_daily_df['EEID'].isin(eeid_missing_prod)], weekly_filtered_missing_df[weekly_filtered_missing_df['EEID'].isin(eeid_missing_prod)], input_folder_path, output_folder_path, week_start, week_end, report_type=2)

    # Determine users with gap days
    weekly_filtered_gaps_df, eeid_with_gaps = filter_gap_days_users(weekly_df, eeid_missing_prod)
    print(f"Found {len(eeid_with_gaps)} users with gap days.")
    print(f"The proportion of users with gap days is {len(eeid_with_gaps) / daily_df['EEID'].nunique()}")
    
    gap_eeids_done = eeids_reports_cache(reports_folder_name="gap_reports", verbose=False)  # returns a set

    if gap_eeids_done:  # only proceed if there are reports
        gap_eeids_pending = list(set(eeid_with_gaps) - gap_eeids_done)
        if gap_eeids_pending:
            print(f"Proceeding with {len(gap_eeids_pending)} pending EEIDs...")
            users_chart_creator(cleaned_daily_df[cleaned_daily_df['EEID'].isin(gap_eeids_pending)], weekly_filtered_gaps_df[weekly_filtered_gaps_df['EEID'].isin(gap_eeids_pending)], input_folder_path, output_folder_path, week_start, week_end, report_type=1)
    else:
        print("No reports found in the folder. Skipping removal.")
        users_chart_creator(cleaned_daily_df[cleaned_daily_df['EEID'].isin(eeid_with_gaps)], weekly_filtered_gaps_df[weekly_filtered_gaps_df['EEID'].isin(eeid_with_gaps)], input_folder_path, output_folder_path, week_start, week_end, report_type=1)

    # Save CSV dataset
    print('Saving CSV dataset...')
    df_to_save = daily_df[['Date', 'Week', 'EEID', 'AT_UserName', 'FName', 'LName', 'EmployeeTypeDescription', 'Title', 'Company Project Code Desc Only', 'Location', 'Reports_To']].copy()
    df_to_save['Gap_Status'] = df_to_save['EEID'].apply(lambda x: 'Gap' if x in eeid_with_gaps else 'No Gap')
    df_to_save['Missing_Prod_Status'] = df_to_save['EEID'].apply(lambda x: 'Missing Prod' if x in eeid_missing_prod else 'Has Prod')
    df_to_save.to_csv(f"{output_folder_path}csv_datasets/GapDaysDataset_{week_start}_{week_end}.csv", index=False)

def generate_productivity_reports(daily_df: pd.DataFrame, input_folder_path: str, output_folder_path: str):
    """Create the productivity reports for each user in the df."""
    print("Process for report productivity started...")
    # Clear input folder if it contains files
    input_folder = Path(input_folder_path)
    delete_files(input_folder)

    # Week parameters
    week_start = min(daily_df['Week']).strftime('%b %d, %Y')
    week_end = (max(daily_df['Week']) + pd.Timedelta(days=6)).strftime('%b %d, %Y')

    # Remove weekends with zero hours
    cleaned_daily_df = delete_weekend_zero_hours(daily_df)

    # Creating weekly aggregated dataframe for weekly chart generation
    weekly_df = custom_weekly_aggregation(cleaned_daily_df)

    print(f"Total users analyzed: {cleaned_daily_df['EEID'].nunique()}")
    users_chart_creator(cleaned_daily_df, weekly_df, input_folder_path, output_folder_path, week_start=week_start, week_end=week_end, report_type=3)
    
    # Determine users with zero productive hours
    weekly_filtered_missing_df, eeid_missing_prod = filter_missing_prod_users(weekly_df)
    print(f"Found {len(eeid_missing_prod)} users with all weeks having zero productive hours.")
    print(f"The proportion of users with all weeks having zero productive hours is {len(eeid_missing_prod) / daily_df['EEID'].nunique()}")

    # Determine users with gap days
    weekly_filtered_gaps_df, eeid_with_gaps = filter_gap_days_users(weekly_filtered_missing_df, eeid_missing_prod)
    print(f"Found {len(eeid_with_gaps)} users with gap days.")
    print(f"The proportion of users with gap days is {len(eeid_with_gaps) / daily_df['EEID'].nunique()}")

    # Save CSV dataset
    print('Saving CSV dataset...')
    df_to_save = daily_df[['Date', 'Week', 'EEID', 'Productive Only', 'AT_UserName', 'FName', 'LName', 'EmployeeTypeDescription', 'Title', 'Company Project Code Desc Only', 'Location', 'Reports_To']].copy()
    df_to_save['Gap_Status'] = df_to_save['EEID'].apply(lambda x: 'Gap' if x in eeid_with_gaps else 'No Gap')
    df_to_save['Missing_Prod_Status'] = df_to_save['EEID'].apply(lambda x: 'Missing Prod' if x in eeid_missing_prod else 'Has Prod')
    df_to_save.to_csv(f"{output_folder_path}csv_datasets/AnalysisRandyRequest_{week_start}_{week_end}.csv", index=False)
