"""
Utility functions for data processing and formatting
"""

def hours_to_hhmm(x):
    """Format y-axis as hh:mm (hours:minutes)"""
    h = int(x)
    m = int(round((x - h) * 60))
    if m == 60:
        h += 1
        m = 0
    return f"{h:02d}h:{m:02d}m"