
"""
Configuration module for charts
"""
import matplotlib.patches as mpatches

DICT_COL_NAMES = {
    'AT_Date': 'Date',
    'Employee_ID': 'EEID',
    'HOLHrs': 'Holiday Hours',
    'PTOHrs': 'PTO Hours',
    'Productive_Active': 'Productive Active Hours',
    'Productive_Passive': 'Productive Passive Hours',
    'Undefined': 'Undefined Hours',
    'Unproductive': 'Unproductive Hours',
}

# Color scheme
PROD_COLORS = {
    'Productive Active Hours': "#17E9DF",
    'Productive Passive Hours': "#8CF5F0",
    'Holiday Hours': "#F5E2A8",
    'PTO Hours': '#FFC000',
    'Undefined Hours': '#2F2F2F',
    'Unproductive Hours': '#B764EB'
}

# Chart configurations
CHART_COLUMNS = ['Productive Active Hours', 'Productive Passive Hours', 'Holiday Hours', 'PTO Hours', 'Undefined Hours', 'Unproductive Hours']

# Generate legend patches
LEGEND_PATCHES = [mpatches.Patch(color=c, label=p) for p, c in PROD_COLORS.items()]
