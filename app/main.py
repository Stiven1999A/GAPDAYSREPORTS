"""
Main script to generate GapDays report
"""
from tools.report_generator import generate_gapdays_report

def main():
    """Main function to run the report generation"""
    input_folder_path = '/Users/Estiben.Gonzalez/OneDrive - Employer Solutions/Desktop/Daily_AT_Report/app/data/input/GapDaysData/'
    output_folder_path = '/Users/Estiben.Gonzalez/OneDrive - Employer Solutions/Desktop/Daily_AT_Report/app/data/output/GapDaysReports/'
    
    print("Generating GapDays report...")
    
    try:
        result_path = generate_gapdays_report(input_folder_path, output_folder_path)
        print(f"Report successfully generated: {result_path}")
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        raise

if __name__ == "__main__":
    main()
