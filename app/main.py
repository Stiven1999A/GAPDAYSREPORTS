"""
Main script to generate GapDays report
"""
from tools.connections import alchemy_connection
from tools.dataprocessing import generate_query, load_data, preprocess_data, generate_gapdays_missingprod_reports, generate_productivity_reports

def main():
    """Main function to run the report generation"""
    input_folder_path = '/Users/Estiben.Gonzalez/Downloads/Daily_AT_Report/GapDaysReports/app/data/input/'
    output_folder_path = '/Users/Estiben.Gonzalez/Downloads/Daily_AT_Report/GapDaysReports/app/data/output/'
    
    print("Generating GapDays report...")
    conn = None
    try:
        # Fix: remove .lower() before int()
        report_type = int(input("Enter report type (gap_days: 1. productivity: 3): ").strip())
        conn = alchemy_connection()
        query = generate_query(report_type=report_type)
        df = load_data(conn, query)
        print(df.head())
        preprocessed_df = preprocess_data(df)
        if report_type == 1:
            generate_gapdays_missingprod_reports(preprocessed_df, input_folder_path, output_folder_path)
        elif report_type == 3:
            generate_productivity_reports(preprocessed_df, input_folder_path, output_folder_path)
        print("Report successfully generated")
    except Exception as e:
        print(f"Error generating report: {str(e)}")
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
    main()
