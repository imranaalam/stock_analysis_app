import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import sqlite3

# Connect to SQLite database
def load_data(ticker):
    conn = sqlite3.connect('data/tick_data.db')
    query = f"SELECT * FROM Ticker WHERE Ticker = '{ticker}'"
    df = pd.read_sql_query(query, conn)
    conn.close()
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', inplace=True)
    return df

# Calculate Fair Value Gaps (FVG)
def calculate_fvg(df):
    # Identify upward FVGs: current low > previous high
    fvg_up = (df['Low'].shift(1) > df['High']).astype(int)

    # Identify downward FVGs: current high < previous low
    fvg_down = (df['High'].shift(1) < df['Low']).astype(int)

    # Add FVG start and end positions for shading
    df['FVG_Up_Start'] = np.where(fvg_up, df['Date'], pd.NaT)
    df['FVG_Up_End'] = np.where(fvg_up, df['Date'].shift(-1), pd.NaT)
    df['FVG_Up_High'] = np.where(fvg_up, df['Low'], np.nan)
    df['FVG_Up_Low'] = np.where(fvg_up, df['High'].shift(1), np.nan)

    df['FVG_Down_Start'] = np.where(fvg_down, df['Date'], pd.NaT)
    df['FVG_Down_End'] = np.where(fvg_down, df['Date'].shift(-1), pd.NaT)
    df['FVG_Down_High'] = np.where(fvg_down, df['Low'].shift(1), np.nan)
    df['FVG_Down_Low'] = np.where(fvg_down, df['High'], np.nan)

    return df

# Plot the data using Plotly
def plot_chart(df):
    fig = go.Figure()

    # Candlestick chart
    fig.add_trace(go.Candlestick(
        x=df['Date'],
        open=df['Open'],
        high=df['High'],
        low=df['Low'],
        close=df['Close'],
        name='Candlestick'
    ))

    # Add Fair Value Gaps as shaded areas
    for i in range(len(df)):
        # Plotting FVG Up
        if pd.notna(df['FVG_Up_Start'].iloc[i]):
            fig.add_shape(type='rect',
                          x0=df['FVG_Up_Start'].iloc[i], x1=df['FVG_Up_End'].iloc[i],
                          y0=df['FVG_Up_Low'].iloc[i], y1=df['FVG_Up_High'].iloc[i],
                          fillcolor='rgba(255, 165, 0, 0.5)',  # Orange color with transparency
                          line=dict(color='rgba(255, 165, 0, 0.5)'))

        # Plotting FVG Down
        if pd.notna(df['FVG_Down_Start'].iloc[i]):
            fig.add_shape(type='rect',
                          x0=df['FVG_Down_Start'].iloc[i], x1=df['FVG_Down_End'].iloc[i],
                          y0=df['FVG_Down_Low'].iloc[i], y1=df['FVG_Down_High'].iloc[i],
                          fillcolor='rgba(138, 43, 226, 0.5)',  # Purple color with transparency
                          line=dict(color='rgba(138, 43, 226, 0.5)'))

    # Customize layout
    fig.update_layout(
        title='Stock Analysis with Fair Value Gaps',
        xaxis_title='Date',
        yaxis_title='Price',
        legend_title='Indicators',
        xaxis_rangeslider_visible=False
    )

    st.plotly_chart(fig)

# Streamlit app layout
st.title('Stock Analysis Suite')

# User input for ticker selection
ticker = st.text_input('Enter the stock ticker:', 'MCB')

# Load and process data
df = load_data(ticker)
df = calculate_fvg(df)

# Display data and plot
st.write(f'Data for {ticker}', df.tail())
plot_chart(df)
