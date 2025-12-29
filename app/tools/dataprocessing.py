"""Data processing utilities for the GapDaysReports application.
"""
import pandas as pd
import numpy as np
from sqlalchemy import text
from connections import alchemy_connection

def load_data(conn) -> pd.DataFrame:
    """Loads data from the database into a DataFrame."""
    # Read from the reporting view containing daily employee hours summary
    query = "SELECT TOP 5 * FROM vw_VT_DailyEEHoursSummary"
    result = conn.execute(text(query))
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    df = df.infer_objects(copy=False).fillna(0)
    return df

print(load_data(alchemy_connection()))