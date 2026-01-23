"""Data processing utilities for the GapDaysReports application."""
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from pathlib import Path
from dataprocessing import load_data, preprocess_data, prepare_data_weekly_chart, gap_days_identifier
from connections import alchemy_connection
from utils import hours_to_hhmm
from config import CHART_COLUMNS, PROD_COLORS
from datetime import datetime

def stacked_bar_chart(df: pd.DataFrame) -> go.Figure:
    """Generates a stacked bar chart for the given DataFrame."""
    fig = go.Figure(layout={
        
    })

    for col in CHART_COLUMNS:

        fig.add_bar(
            x=df['Week'],
            y=df[col],
            name=col,
            marker_color=PROD_COLORS[col],
            texttemplate=[hours_to_hhmm(y) for y in df[col]],
            textposition="inside",
            hovertemplate = f"<b>{col}</b><br>Week: %{{x}}<br>Hours: %{{texttemplate}}<extra></extra>",
        )

    fig.add_trace(
        go.Scatter(
            x=df['Week'],
            y=df['Daily Productive Average'],
            mode='lines+markers+text',
            name='Daily Productive Average per Week',
            line=dict(color="#CE089C", width=3, shape='spline'),
            #text=[f"{hours_to_hhmm(y)}" for y in df['Daily Productive Average']],
            textposition="top center",
            yaxis="y",
            hoveron="points+fills"
        )    
    )

    fig.add_trace(
        go.Scatter(
            x=df['Week'],
            y=df['Weekly Productivity Accumulated Average'],
            mode='lines+markers',
            name='Weekly Productivity Accumulated Average',
            line=dict(color="#00ADC4", width=3, shape='spline'),
            yaxis="y"
        )    
    )

    # Format week labels as "Mon Dth - Mon Dth"
    week_labels = [f"{start.strftime('%b %d')} - {(start + pd.Timedelta(days=6)).strftime('%b %d')}" 
                   for start in pd.to_datetime(df['Week'])]

    fig.update_layout(
        width=1000,
        margin=dict(l=80, r=80),
        title="Weekly Productive Hours",
        xaxis_title="Week",
        yaxis_title="Hours",
        barmode="stack",
        xaxis=dict(ticktext=week_labels, tickvals=df['Week']),
        yaxis=dict(
            tickformat=".0f",
            ticktext=[hours_to_hhmm(h) for h in range(0, int(max(df[CHART_COLUMNS].sum(axis=1))) + 2, 3)],
            tickvals=list(range(0, int(max(df[CHART_COLUMNS].sum(axis=1))) + 2, 3))
        ),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5,
        ),
        template="plotly_white",
        hoverlabel=dict(namelength=-1)
    )

    return fig

df = load_data(alchemy_connection(), pd.to_datetime('2025-11-28'))
print(df.shape)
df1, list_eeid = gap_days_identifier(preprocess_data(df))
print(len(list_eeid))

#fig = stacked_bar_chart(prepare_data_weekly_chart(preprocess_data(load_data(alchemy_connection(), pd.to_datetime('2025-11-28')))))
#pio.show(fig)

#output_path = Path("/Users/Estiben.Gonzalez/Downloads/Daily_AT_Report/GapDaysReports/app/data/output/weekly_productive_hours.png").resolve()
#output_path.parent.mkdir(parents=True, exist_ok=True)
#fig.write_image(str(output_path))
#fig.write_html(str(output_path.with_suffix('.html')))
