"""Data processing utilities for the GapDaysReports application."""
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from tools.utils import hours_to_hhmm
from tools.config import CHART_COLUMNS, PROD_COLORS

def weekly_bar_chart(df: pd.DataFrame, output_folder_path: str) -> go.Figure:
    """Generates a stacked bar chart for the given DataFrame."""
    fig = go.Figure()

    for col in CHART_COLUMNS:

        fig.add_bar(
            x=df['Week'],
            y=df[col],
            name=col,
            marker_color=PROD_COLORS[col],
            texttemplate=[hours_to_hhmm(y) for y in df[col]],
            textposition="none",
            hovertemplate = f"<b>{col}</b><br>Week: %{{x}}<br>Hours: %{{texttemplate}}<extra></extra>",
        )

    fig.add_trace(
        go.Scatter(
            x=df['Week'],
            y=df['Daily Productive Average'],
            mode='lines+markers+text',
            name='Daily Productive Average per Week',
            line=dict(color="#CE089C", width=4, shape='spline'),
            yaxis="y",
        )    
    )

    # Format week labels as "Mon Dth - Mon Dth"
    week_labels = [f"{start.strftime('%b %d')} - {(start + pd.Timedelta(days=6)).strftime('%b %d')}" 
                   for start in pd.to_datetime(df['Week'])]

    fig.update_layout(
        width=5000,
        height=10000,
        margin=dict(l=80, r=0, t=80, b=100),
        title=dict(
            text=f"<b>Weekly Productive Hours ({min(df['Week']).strftime('%b %d, %Y')} - {(max(df['Week']) +  pd.Timedelta(days=6)).strftime('%b %d, %Y')})</b>",
            x=0.5,
            xanchor='center'
        ),
        barmode='stack',
        xaxis=dict(
            title=dict(text="Week", font=dict(size=28)),
            ticktext=week_labels,
            tickvals=df['Week'],
            tickfont=dict(size=28, color="black"),
            showline=True,
            linewidth=3,
            linecolor='black'
        ),
        yaxis=dict(
            title=dict(text="Hours", font=dict(size=28)),
            tickformat=".0f",
            ticktext=[hours_to_hhmm(h) for h in range(0, int(max(df[CHART_COLUMNS].sum(axis=1))) + 2, 3)],
            tickvals=list(range(0, int(max(df[CHART_COLUMNS].sum(axis=1))) + 2, 3)),
            tickfont=dict(size=24, color="black"),
            showline=True,
            linewidth=3,
            linecolor='black'
        ),
        title_font=dict(size=32),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5,
            font=dict(size=21, color="black")
        ),
        template="plotly_white",
        hoverlabel=dict(namelength=-1),
        plot_bgcolor='white',
        paper_bgcolor='white'
    )

    # Add total hours on top of each bar
    for i, week in enumerate(df['Week']):
        total_hours = df['Total Hours'].iloc[i]
        daily_prod_avg = df['Daily Productive Average'].iloc[i]

        fig.add_annotation(
            x=week,
            y=total_hours,
            text=str(hours_to_hhmm(total_hours)),
            showarrow=True,
            arrowhead=2,
            ax=0,
            ay=-10,
            font=dict(size=28, color="black")
        )

        fig.add_annotation(
            x=week,
            y=daily_prod_avg,
            text=str(hours_to_hhmm(daily_prod_avg)),
            showarrow=True,
            arrowhead=2,
            ax=0,
            ay=-10,
            font=dict(size=28, color="#CE089C")
        )
    # Save as high-definition PNG
    output_path = Path(f"{output_folder_path}/weekly_productive_hours.png").resolve()
    fig.write_image(str(output_path), width=1400, height=850, scale=2)


def daily_bar_chart(df: pd.DataFrame, output_folder_path: str, week: pd.Timestamp, name: str) -> go.Figure:
    """Generates a daily chart for a specific EEID."""

    fig = go.Figure()

    for col in CHART_COLUMNS:

        fig.add_bar(
            x=df['Date'],
            y=df[col],
            name=col,
            marker_color=PROD_COLORS[col],
        )
        
    total_hours = df['Total Hours'].sum()
    days_labels = [date.strftime('%a, %b %e') for date in pd.to_datetime(df['Date'])]
    fig.update_layout(
        width=1000,
        margin=dict(l=80, r=0, t=80, b=100),
        title=f"""Daily Productive Hours for the Week Between {week.strftime('%b %d, %Y')} and {(week + pd.Timedelta(days=6)).strftime('%b %d, %Y')}""",
        title_font=dict(size=34, color="black"),
        barmode='stack',
        yaxis=dict(
            title=dict(text="Hours", font=dict(size=24)),
            tickformat=".0f",
            ticktext=[hours_to_hhmm(h) for h in range(0, int(total_hours + 2))],
            tickvals=list(range(0, int(total_hours + 2))),
            tickfont=dict(size=24, color="black"),
            showline=True,
            linewidth=2,
            linecolor='black'
        ),
        xaxis=dict(
            title=dict(text="Day", font=dict(size=24)),
            ticktext=days_labels,
            tickvals=df['Date'],
            tickfont=dict(size=24, color="black"),
            showline=True,
            linewidth=2,
            linecolor='black'
        ),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5,
            font=dict(size=14, color="black")
        ),
        template="plotly_white",
        hoverlabel=dict(namelength=-1)
    )

    fig.add_trace(
        go.Scatter(
            x=df['Date'],
            y=df['Daily Productive Accumulated Average'],
            mode='lines+markers+text',
            name='Daily Productive Accumulated Average',
            line=dict(color="#0FB9B1", width=3, shape='spline'),
            yaxis="y",
        )    
    )
    
    for j, date in enumerate(df['Date']):

        daily_prod_acc_avg = df['Daily Productive Accumulated Average'].iloc[j]
        total_hours_per_day = df['Total Hours'].iloc[j]
        
        if total_hours_per_day == 0:
            fig.add_annotation(
            x=date,
            y= 1,
            text="↓",
            showarrow=True,
            arrowhead=2,
            ax=0,
            ay=-10,
            font=dict(size=60, color="red")
            )

        fig.add_annotation(
            x=date,
            y=daily_prod_acc_avg,
            text=str(hours_to_hhmm(daily_prod_acc_avg)),
            showarrow=True,
            arrowhead=2,
            ax=0,
            ay=-10,
            font=dict(size=28, color="#CE089C" if j == len(df) - 1 else "#0FB9B1")
        )

    # Save as high-definition PNG
    output_path = Path(f"{output_folder_path}/{name}.png").resolve()
    fig.write_image(str(output_path), width=1400, height=700, scale=2)
