"""Data processing utilities for the GapDaysReports application.
"""
import pandas as pd
import numpy as np
from connections import connect_to_db

def load_data(conn) -> pd.DataFrame:
    """Loads data from the database into a DataFrame."""
    # Read from the reporting view containing daily employee hours summary
    query = "SELECT TOP 5 * FROM vw_VT_DailyEEHoursSummary"
    df = pd.read_sql(query, conn)
    return df

print(load_data(connect_to_db()))