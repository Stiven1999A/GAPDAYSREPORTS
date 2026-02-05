"""
Export data from VT
"""
import pandas as pd
from tools.config import EMPLOYEE_IDS
from tools.connections import alchemy_connection
from tools.dataprocessing import load_data, preprocess_data, generate_gapdays_missingprod_reports, generate_productivity_reports

def export_data(query, start_date, end_date):
    """Main function to run the report generation"""
    output_folder_path = '/Users/Estiben.Gonzalez/Downloads/Daily_AT_Report/GapDaysReports/app/data/output/exported_data/'
    
    print("Exporting data...")
    conn = None
    try:
        conn = alchemy_connection()
        df = load_data(conn, query)
        preprocessed_df = preprocess_data(df)
        preprocessed_df.to_csv(f"{output_folder_path}Data_Export_{start_date}_{end_date}", index=False)
        print("Data successfully exported")
    except Exception as e:
        print(f"Error exporting data: {str(e)}")
        raise
    finally:
        # Always close the connection if it was opened
        if conn is not None:
            try:
                conn.close()
            except Exception:
                # Avoid masking the original exception
                pass

if __name__ == "__main__":
    inputed_start_date = input("Enter the start date (YYYY-MM-DD): ")
    inputed_end_date = input("Enter the end date (YYYY-MM-DD): ")

    query = f"""SELECT * FROM vw_VT_DailyEEHoursSummary
            WHERE AT_Date BETWEEN '{inputed_start_date}' AND '{inputed_end_date}'
            AND Employee_ID IN ({','.join(f"'{key}'" for key in EMPLOYEE_IDS.keys())});
            """
    export_data(query, inputed_start_date, inputed_end_date)
