# analysis/mxwll_suite_indicator.py

import pandas as pd
import plotly.graph_objects as go  # Essential for Plotly figures
from ta.volatility import AverageTrueRange
import warnings
from datetime import datetime, timedelta

def mxwll_suite_indicator(df, ticker, params):
    """
    Generates a Plotly figure based on the mxwll suite indicator analysis and provides summary statistics.

    Args:
        df (pd.DataFrame): DataFrame containing stock data.
        ticker (str): Stock ticker symbol.
        params (dict): Dictionary of analysis parameters.

    Returns:
        tuple:
            - plotly.graph_objects.Figure: The generated Plotly figure.
            - dict: Summary statistics for the analysis, including AOI metrics.
    """
    # Suppress warnings for cleaner output
    warnings.filterwarnings("ignore")
    
    # === Derived Parameters Based on Data Frequency ===
    
    if params['data_frequency'] == '15m':
        atr_window = 14  # ATR window
        aoi_length = 50  # AOE window
        session_enabled = True
        session_times = {
            'New York': {'start': '09:30', 'end': '16:00'},
            'Asia': {'start': '20:00', 'end': '02:00'},
            'London': {'start': '03:00', 'end': '11:30'}
        }
    elif params['data_frequency'] == '4h':
        atr_window = 14
        aoi_length = 50
        session_enabled = True
        session_times = {
            'New York': {'start': '09:30', 'end': '16:00'},
            'Asia': {'start': '20:00', 'end': '02:00'},
            'London': {'start': '03:00', 'end': '11:30'}
        }
    elif params['data_frequency'] == '1D':
        atr_window = 14
        aoi_length = 50
        session_enabled = False  # Sessions are not time-based for daily data
        session_times = {}
    else:
        raise ValueError("Invalid data_frequency. Choose from '15m', '4h', or '1D'.")
    
    # === Session Colors ===
    session_colors = {
        'New York': params['bear_color'],  # Bear Color (Red)
        'Asia': params['bull_color'],      # Bull Color (Green)
        'London': params['fvg_color']      # FVG Color
    }
    
    # === Helper Functions ===
    
    def calculate_pivots_vectorized(df, sensitivity):
        """
        Vectorized pivot calculation for enhanced performance.
        Identifies swing highs and lows based on the specified sensitivity.
        """
        # Swing Highs
        rolling_max = df['High'].rolling(window=2*sensitivity+1, center=True).max()
        swing_highs = df[df['High'] == rolling_max].index.to_list()
        
        # Swing Lows
        rolling_min = df['Low'].rolling(window=2*sensitivity+1, center=True).min()
        swing_lows = df[df['Low'] == rolling_min].index.to_list()
        
        return swing_highs, swing_lows
    
    def identify_fvg(df):
        """
        Identifies Fair Value Gaps (FVG) in the data.
        """
        fvg_up = []
        fvg_down = []
        for i in range(1, len(df)):
            prev_high = df['High'].iloc[i-1]
            prev_low = df['Low'].iloc[i-1]
            current_high = df['High'].iloc[i]
            current_low = df['Low'].iloc[i]
            if prev_high < current_low:
                fvg_up.append({'x0': df.index[i], 'y0': prev_high, 'x1': df.index[i], 'y1': current_low})
            if prev_low > current_high:
                fvg_down.append({'x0': df.index[i], 'y0': current_high, 'x1': df.index[i], 'y1': prev_low})
        return fvg_up, fvg_down
    
    def plot_fibonacci_levels(fig, last_high, last_low):
        """
        Plots Fibonacci retracement levels based on the latest swing high and low.
        """
        fib_diff = last_high - last_low
        for level, color in zip(params['fib_levels'], params['fib_colors']):
            if level == 0.5 and not params['show_fib5']:
                continue
            fib_level = last_low + fib_diff * level
            fig.add_hline(y=fib_level, line=dict(color=color, dash='dash'), 
                          annotation_text=f'Fib {level}', annotation_position="top left")
    
    def draw_aoe(fig, df):
        """
        Draws the Area of Interest (AOE) boxes based on ATR and recent price action.
        Returns the highest and lowest points of the AOI.
        """
        atr_indicator = AverageTrueRange(high=df['High'], low=df['Low'], close=df['Close'], window=atr_window)
        df['ATR'] = atr_indicator.average_true_range()
        
        # Define AOE window
        aoi = df.iloc[-aoi_length:]
        max_aoi_high = max(aoi['High'].max(), aoi['Open'].iloc[-aoi_length:].max())
        min_aoi_low = min(aoi['Low'].min(), aoi['Open'].iloc[-aoi_length:].min())
        atr_latest = df['ATR'].iloc[-1]
        
        # High AOE Box
        high_aoe_y0 = max_aoi_high * 1.01
        high_aoe_y1 = max_aoi_high
        high_aoe_x0 = aoi.index[0]
        high_aoe_x1 = aoi.index[-1]
        
        fig.add_shape(type="rect",
                      x0=high_aoe_x0,
                      y0=high_aoe_y0,
                      x1=high_aoe_x1,
                      y1=high_aoe_y1,
                      fillcolor=params['bear_color'],
                      opacity=0.2,
                      line=dict(width=0),
                      layer='below',
                      name='High AOE')
        
        # Low AOE Box
        low_aoe_y0 = min_aoi_low
        low_aoe_y1 = min_aoi_low * 0.99
        low_aoe_x0 = aoi.index[0]
        low_aoe_x1 = aoi.index[-1]
        
        fig.add_shape(type="rect",
                      x0=low_aoe_x0,
                      y0=low_aoe_y0,
                      x1=low_aoe_x1,
                      y1=low_aoe_y1,
                      fillcolor=params['bull_color'],
                      opacity=0.2,
                      line=dict(width=0),
                      layer='below',
                      name='Low AOE')
        
        # Return AOI points for summary
        return high_aoe_y0, low_aoe_y1
    
    def highlight_sessions(fig, df):
        """
        Highlights trading sessions (New York, Asia, London) on the chart.
        Applicable only for intra-day data frequencies.
        """
        if not session_enabled:
            return
        
        for session, props in session_times.items():
            start_time = datetime.strptime(props['start'], '%H:%M').time()
            end_time = datetime.strptime(props['end'], '%H:%M').time()
            
            for date in df.index.normalize().unique():
                start_datetime = datetime.combine(date, start_time)
                end_datetime = datetime.combine(date, end_time)
                # Handle sessions that span over midnight
                if end_datetime <= start_datetime:
                    end_datetime += timedelta(days=1)
                
                fig.add_vrect(
                    x0=start_datetime,
                    x1=end_datetime,
                    fillcolor=session_colors.get(session, 'rgba(0,0,0,0)'),
                    opacity=params['transparency'],
                    layer="below",
                    line_width=0,
                    annotation_text=session if (date == df.index[-1].normalize()) else "",
                    annotation_position="top left",
                    annotation_font_size=10,
                    annotation_font_color="white",
                    name=session
                )
    
    def volume_activity(df):
        """
        Categorizes volume into different activity levels based on quantiles.
        """
        vol_perc1 = df['Volume'].quantile(0.1)
        vol_perc2 = df['Volume'].quantile(0.33)
        vol_perc3 = df['Volume'].quantile(0.5)
        vol_perc4 = df['Volume'].quantile(0.66)
        vol_perc5 = df['Volume'].quantile(0.9)
        
        def categorize_volume(vol):
            if vol <= vol_perc1:
                return "Very Low"
            elif vol <= vol_perc2:
                return "Low"
            elif vol <= vol_perc3:
                return "Average"
            elif vol <= vol_perc4:
                return "High"
            else:
                return "Very High"
        
        df['VolumeActivity'] = df['Volume'].apply(categorize_volume)
        return df
    
    def add_volume_annotation(fig, df):
        """
        Adds a volume activity annotation to the latest data point.
        """
        latest = df.iloc[-1]
        latest_time = latest.name
        latest_volume_activity = latest['VolumeActivity']
        
        current_session = "Dead Zone"
        time_until_change = "N/A"
        
        if params['data_frequency'] in ['15m', '4h']:
            # Determine current session based on the latest timestamp
            for session, props in session_times.items():
                start_time = datetime.strptime(props['start'], '%H:%M').time()
                end_time = datetime.strptime(props['end'], '%H:%M').time()
                if start_time <= latest_time.time() <= end_time:
                    current_session = session
                    break
            
            # Calculate time until next session change
            def calculate_time_until_change(current_time):
                for session, props in session_times.items():
                    start_time = datetime.strptime(props['start'], '%H:%M').time()
                    end_time = datetime.strptime(props['end'], '%H:%M').time()
                    if start_time <= current_time.time() <= end_time:
                        # Next session is the following session in the list
                        session_names = list(session_times.keys())
                        current_index = session_names.index(session)
                        next_session = session_names[(current_index + 1) % len(session_names)]
                        next_start_str = session_times[next_session]['start']
                        next_start_time = datetime.strptime(next_start_str, '%H:%M').time()
                        next_start_datetime = datetime.combine(current_time.date(), next_start_time)
                        if next_start_time <= current_time.time():
                            next_start_datetime += timedelta(days=1)
                        time_diff = next_start_datetime - current_time
                        hours, remainder = divmod(int(time_diff.total_seconds()), 3600)
                        minutes, _ = divmod(remainder, 60)
                        return f"{hours}h {minutes}m"
                return "N/A"
            
            time_until_change = calculate_time_until_change(latest_time)
        else:
            # For daily data, sessions are not time-based
            current_session = "Dead Zone"
        
        annotation_text = f"""
        Session: {current_session}<br>
        Session Close: {time_until_change}<br>
        Volume Activity: {latest_volume_activity}
        """
        
        fig.add_annotation(
            x=latest_time,
            y=latest['High'],
            text=annotation_text,
            showarrow=True,
            arrowhead=1,
            align="left",
            bgcolor="rgba(0,0,0,0.5)",
            font=dict(color="white")
        )
    
    def draw_main_line(fig, big_upper, big_lower):
        """
        Draws the main line connecting the latest swing high and low.
        """
        if not big_upper or not big_lower:
            return None
        
        # Get the latest swing high and low
        latest_swing_high = big_upper[-1]
        latest_swing_low = big_lower[-1]
        
        # Define main line based on the latest swing points
        main_line = {
            'x1': latest_swing_low,
            'y1': df.loc[latest_swing_low, 'Low'],
            'x2': latest_swing_high,
            'y2': df.loc[latest_swing_high, 'High']
        }
        
        fig.add_trace(go.Scatter(
            x=[main_line['x1'], main_line['x2']],
            y=[main_line['y1'], main_line['y2']],
            mode='lines',
            line=dict(color='blue', dash='dash'),
            name='Main Line'
        ))
        
        return main_line
    
    # === Calculate Pivots ===
    big_upper, big_lower = calculate_pivots_vectorized(df, params['external_sensitivity'])
    if params['show_internals']:
        small_upper, small_lower = calculate_pivots_vectorized(df, params['internal_sensitivity'])
    else:
        small_upper, small_lower = [], []
    
    # === Identify FVG ===
    fvg_up, fvg_down = identify_fvg(df)
    
    # === Volume Activity ===
    df = volume_activity(df)
    
    # === Create Plotly Figure ===
    fig = go.Figure()
    
    # --- Plot Candlestick ---
    fig.add_trace(go.Candlestick(
        x=df.index,
        open=df['Open'],
        high=df['High'],
        low=df['Low'],
        close=df['Close'],
        name='Price',
        increasing_line_color='green',
        decreasing_line_color='red'
    ))
    
    # --- Plot Swing Highs ---
    for swing_time in big_upper[-params['swing_order_blocks']:]:
        try:
            swing_price = df.loc[swing_time, 'High']
        except KeyError:
            continue
        label_text = 'HH'
        if params['show_hhlh']:
            fig.add_trace(go.Scatter(
                x=[swing_time],
                y=[swing_price],
                mode='markers+text',
                marker=dict(color=params['bear_color'], size=10, symbol='triangle-up'),
                text=[label_text],
                textposition='bottom center',
                name='Swing High',
                showlegend=False  # Remove from legend to avoid repetition
            ))
    
    # --- Plot Swing Lows ---
    for swing_time in big_lower[-params['swing_order_blocks']:]:
        try:
            swing_price = df.loc[swing_time, 'Low']
        except KeyError:
            continue
        label_text = 'LL'
        if params['show_hlll']:
            fig.add_trace(go.Scatter(
                x=[swing_time],
                y=[swing_price],
                mode='markers+text',
                marker=dict(color=params['bull_color'], size=10, symbol='triangle-down'),
                text=[label_text],
                textposition='top center',
                name='Swing Low',
                showlegend=False  # Remove from legend to avoid repetition
            ))
    
    # --- Plot Internal Swing Highs ---
    if params['show_internals']:
        for swing_time in small_upper:
            try:
                swing_price = df.loc[swing_time, 'High']
            except KeyError:
                continue
            fig.add_trace(go.Scatter(
                x=[swing_time],
                y=[swing_price],
                mode='markers',
                marker=dict(color=params['bear_color'], size=6, symbol='triangle-up'),
                name='Internal Swing High',
                showlegend=False  # Remove from legend to avoid repetition
            ))
    
    # --- Plot Internal Swing Lows ---
    if params['show_internals']:
        for swing_time in small_lower:
            try:
                swing_price = df.loc[swing_time, 'Low']
            except KeyError:
                continue
            fig.add_trace(go.Scatter(
                x=[swing_time],
                y=[swing_price],
                mode='markers',
                marker=dict(color=params['bull_color'], size=6, symbol='triangle-down'),
                name='Internal Swing Low',
                showlegend=False  # Remove from legend to avoid repetition
            ))
    
    # --- Plot Fair Value Gaps (FVG) ---
    if params['show_fvg']:
        for gap in fvg_up:
            fig.add_shape(type="rect",
                          x0=gap['x0'],
                          y0=gap['y0'],
                          x1=gap['x1'],
                          y1=gap['y1'],
                          fillcolor=params['fvg_color'],
                          opacity=params['fvg_transparency'] / 100,
                          line=dict(width=0),
                          layer='below',
                          name='FVG Up')
        for gap in fvg_down:
            fig.add_shape(type="rect",
                          x0=gap['x0'],
                          y0=gap['y0'],
                          x1=gap['x1'],
                          y1=gap['y1'],
                          fillcolor=params['fvg_color'],
                          opacity=params['fvg_transparency'] / 100,
                          line=dict(width=0),
                          layer='below',
                          name='FVG Down')
    
    # --- Draw Area of Interest (AOE) ---
    if params['show_aoe']:
        try:
            high_aoi_y0, low_aoi_y1 = draw_aoe(fig, df)
        except Exception as e:
            high_aoi_y0, low_aoi_y1 = None, None
            print(f"Error drawing AOE: {e}")
    else:
        high_aoi_y0, low_aoi_y1 = None, None
    
    # --- Highlight Trading Sessions ---
    highlight_sessions(fig, df)
    
    # --- Draw Main Line (Connecting Latest Swing Points) ---
    main_line = draw_main_line(fig, big_upper, big_lower)
    
    # --- Draw Fibonacci Levels ---
    if main_line and params['show_fibs']:
        plot_fibonacci_levels(fig, main_line['y1'], main_line['y2'])
    
    # --- Add Volume Activity Annotation ---
    add_volume_annotation(fig, df)
    
    # === Summary Calculations ===
    summary_data = {}
    if params['show_aoe'] and high_aoi_y0 is not None and low_aoi_y1 is not None:
        try:
            # Last Candle (excluding wicks)
            last_candle = df.iloc[-1]
            last_candle_bottom = min(last_candle['Open'], last_candle['Close'])
            last_candle_upper = max(last_candle['Open'], last_candle['Close'])
            
            # Differences
            difference_bottom_to_AOI_top = high_aoi_y0 - last_candle_bottom
            difference_upper_to_AOI_bottom = last_candle_upper - low_aoi_y1
            
            # Percentages
            percentage_diff_bottom_to_AOI_top = (difference_bottom_to_AOI_top / high_aoi_y0) * 100 if high_aoi_y0 != 0 else None
            percentage_diff_upper_to_AOI_bottom = (difference_upper_to_AOI_bottom / low_aoi_y1) * 100 if low_aoi_y1 != 0 else None
            
            # Populate summary data
            summary_data = {
                'Ticker': ticker,
                'Highest AOI (Red)': round(high_aoi_y0, 2),
                'Lowest AOI (Green)': round(low_aoi_y1, 2),
                'Difference (Last Candle Bottom to AOI Top)': round(difference_bottom_to_AOI_top, 2),
                'Difference (Last Candle Upper to AOI Bottom)': round(difference_upper_to_AOI_bottom, 2),
                'Percentage (Bottom to AOI Top)': round(percentage_diff_bottom_to_AOI_top, 2) if percentage_diff_bottom_to_AOI_top is not None else None,
                'Percentage (Upper to AOI Bottom)': round(percentage_diff_upper_to_AOI_bottom, 2) if percentage_diff_upper_to_AOI_bottom is not None else None
            }
        except Exception as e:
            print(f"Error in summary calculations: {e}")
            summary_data = {
                'Ticker': ticker,
                'Highest AOI (Red)': None,
                'Lowest AOI (Green)': None,
                'Difference (Last Candle Bottom to AOI Top)': None,
                'Difference (Last Candle Upper to AOI Bottom)': None,
                'Percentage (Bottom to AOI Top)': None,
                'Percentage (Upper to AOI Bottom)': None
            }
    else:
        summary_data = {
            'Ticker': ticker,
            'Highest AOI (Red)': None,
            'Lowest AOI (Green)': None,
            'Difference (Last Candle Bottom to AOI Top)': None,
            'Difference (Last Candle Upper to AOI Bottom)': None,
            'Percentage (Bottom to AOI Top)': None,
            'Percentage (Upper to AOI Bottom)': None
        }
    
    # --- Final Layout Adjustments ---
    fig.update_layout(
        title=f'AOI for {ticker}',
        yaxis_title='Price',
        xaxis_title='Date',
        legend=dict(orientation="h"),
        margin=dict(l=50, r=50, t=50, b=50),
        hovermode='x unified'
    )
    
    # --- Remove Separate Small Charts for Zooming ---
    fig.update_xaxes(rangeslider_visible=False)
    
    # --- Remove Repeated Legends ---
    # Since showlegend=False for individual traces, only the main 'Price' and 'Main Line' legends will appear
    fig.update_layout(showlegend=True)
    
    return fig, summary_data
