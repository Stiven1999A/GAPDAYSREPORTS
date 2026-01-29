"""
Main script to generate GapDays report
"""
from tools.connections import alchemy_connection
from tools.dataprocessing import load_data, preprocess_data, generate_gapdays_reports

def main():
    """Main function to run the report generation"""
    input_folder_path = '/Users/Estiben.Gonzalez/Downloads/Daily_AT_Report/GapDaysReports/app/data/input/'
    output_folder_path = '/Users/Estiben.Gonzalez/Downloads/Daily_AT_Report/GapDaysReports/app/data/output/'
    
    print("Generating GapDays report...")
    
    try:
        conn = alchemy_connection()
        df = load_data(conn, PTO_date='2025-11-28')
        preprocessed_df = preprocess_data(df)
        generate_gapdays_reports(preprocessed_df, input_folder_path, output_folder_path)
        print("Report successfully generated")
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        raise

if __name__ == "__main__":
    main()
