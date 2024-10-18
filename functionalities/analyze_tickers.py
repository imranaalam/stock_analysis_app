# functionalities/analyze_tickers.py

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import logging
from utils.db_manager import (
    get_unique_tickers_from_db, 
    get_all_portfolios, 
    get_tickers_by_group,
    get_all_indexes,
    get_tickers_by_index
)
from analysis.mxwll_suite_indicator import mxwll_suite_indicator

def analyze_tickers(conn):
    st.header("🔍 Analyze Tickers")
    tickers = get_unique_tickers_from_db(conn)
    if not tickers:
        st.warning("No tickers available for analysis. Please add tickers first.")
        logging.warning("No tickers available for analysis.")
        return

    # Removed MarketWatch data for debugging as per your request
    # if st.checkbox("🔍 Show Latest MarketWatch Data for Debugging"):
    #     display_marketwatch_data(conn)

    # Select analysis type
    analysis_type = st.selectbox("Select Analysis Type", ["All Tickers", "By Portfolio", "By Ticker Group", "By Index"])

    selected_tickers = []

    if analysis_type == "All Tickers":
        selected_tickers = st.multiselect("Select Tickers for Analysis", tickers, default=tickers)
    elif analysis_type == "By Portfolio":
        portfolios = get_all_portfolios(conn)
        if portfolios:
            portfolio_names = [portfolio['Name'] for portfolio in portfolios]
            selected_portfolio = st.selectbox("Select a Portfolio", portfolio_names)
            portfolio = next((p for p in portfolios if p['Name'] == selected_portfolio), None)
            if portfolio:
                tickers_in_portfolio = [ticker.strip().upper() for ticker in portfolio.get('Stocks', [])]
                st.info(f"Selected Portfolio: {selected_portfolio} with {len(tickers_in_portfolio)} tickers.")
                logging.info(f"Selected Portfolio '{selected_portfolio}' with tickers: {tickers_in_portfolio}.")

                # Allow user to select which tickers to analyze from the portfolio
                selected_tickers = st.multiselect(
                    "Select Tickers for Analysis",
                    options=tickers_in_portfolio,
                    default=tickers_in_portfolio,
                    help="Select the tickers you wish to analyze from the selected portfolio."
                )
            else:
                st.error("Selected portfolio not found.")
                logging.error(f"Selected portfolio '{selected_portfolio}' not found.")
        else:
            st.warning("No portfolios available. Please create a portfolio first.")
            logging.warning("No portfolios available for analysis by portfolio.")
            return
    elif analysis_type == "By Ticker Group":
        group_types = ["Topers Today", "Decliners Today", "Advancers Today"]
        selected_group = st.selectbox("Select Ticker Group", group_types)

        # Map display names to internal group identifiers
        group_mapping = {
            "Topers Today": "topers_today",
            "Decliners Today": "decliners_today",
            "Advancers Today": "advancers_today"
        }

        group_identifier = group_mapping.get(selected_group)
        if group_identifier:
            selected_tickers = get_tickers_by_group(conn, group_identifier)

            # # Debugging Output: Show fetched tickers
            # # st.write(f"🔍 **Fetched Tickers for '{selected_group}':**")
            # if selected_tickers:
            #     st.write(selected_tickers)
            # else:
            #     st.write("No tickers found for this group.")

            if selected_tickers:
                selected_tickers = st.multiselect(
                    f"Select Tickers from '{selected_group}' for Analysis",
                    options=selected_tickers,
                    default=selected_tickers,
                    help=f"Select the tickers you wish to analyze from '{selected_group}'."
                )
            else:
                st.warning(f"No tickers found for group '{selected_group}'.")
                logging.warning(f"No tickers found for group '{selected_group}'.")
                return
        else:
            st.error("Invalid ticker group selected.")
            logging.error(f"Invalid ticker group selected: {selected_group}")
            return
    elif analysis_type == "By Index":
        # Fetch all indexes
        indexes = get_all_indexes(conn)
        if indexes:
            selected_index = st.selectbox("Select an Index", indexes)
            selected_tickers = get_tickers_by_index(conn, selected_index)

            # # Debugging Output: Show fetched index tickers
            # # st.write(f"🔍 **Fetched Tickers for Index '{selected_index}':**")
            # if selected_tickers:
            #     st.write(selected_tickers)
            # else:
            #     st.write("No tickers found for this index.")

            if selected_tickers:
                selected_tickers = st.multiselect(
                    f"Select Tickers from Index '{selected_index}' for Analysis",
                    options=selected_tickers,
                    default=selected_tickers,
                    help=f"Select the tickers you wish to analyze from index '{selected_index}'."
                )
            else:
                st.warning(f"No tickers found for index '{selected_index}'.")
                logging.warning(f"No tickers found for index '{selected_index}'.")
                return
        else:
            st.warning("No indexes available for analysis. Please ensure that 'LISTED_IN' field is populated.")
            logging.warning("No indexes found in MarketWatch.")
            return

    if not selected_tickers:
        st.warning("Please select at least one ticker for analysis.")
        return

    # Define time period options with tabs
    st.subheader("📅 Select Time Period for Analysis")
    time_period_tabs = st.tabs(["1 Month", "6 Months", "1 Year", "3 Years", "5 Years"])
    time_period_options = {
        "1 Month": 30,
        "6 Months": 180,
        "1 Year": 365,
        "3 Years": 1095,
        "5 Years": 1825
    }

    # Initialize session state for selected_period
    if 'selected_period' not in st.session_state:
        st.session_state['selected_period'] = "1 Month"  # Default

    with time_period_tabs[0]:
        if st.button("Select 1 Month"):
            st.session_state['selected_period'] = "1 Month"
    with time_period_tabs[1]:
        if st.button("Select 6 Months"):
            st.session_state['selected_period'] = "6 Months"
    with time_period_tabs[2]:
        if st.button("Select 1 Year"):
            st.session_state['selected_period'] = "1 Year"
    with time_period_tabs[3]:
        if st.button("Select 3 Years"):
            st.session_state['selected_period'] = "3 Years"
    with time_period_tabs[4]:
        if st.button("Select 5 Years"):
            st.session_state['selected_period'] = "5 Years"

    selected_period = st.session_state['selected_period']
    days = time_period_options.get(selected_period, 30)

    # Calculate start_date and end_date based on selected period
    end_date = pd.Timestamp.today()
    start_date = end_date - pd.Timedelta(days=days)

    # Initialize list to collect summary data and comparison metrics
    summary_list = []
    comparison_metrics = []

    # Removed "Set Filters for Analysis" as per your request

    # Button to perform analysis
    if st.button("Run Analysis"):
        for ticker in selected_tickers:
            st.subheader(f"📊 Analysis for {ticker}")

            # Fetch data from database within the selected date range
            query = 'SELECT * FROM Ticker WHERE Ticker = ? AND Date BETWEEN ? AND ? ORDER BY Date ASC;'
            cursor = conn.cursor()
            cursor.execute(query, (ticker, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
            fetched_data = cursor.fetchall()
            columns = [description[0] for description in cursor.description]

            if not fetched_data:
                st.warning(f"No data available for ticker '{ticker}' in the selected period.")
                logging.warning(f"No data available for ticker '{ticker}' between {start_date.date()} and {end_date.date()}.")
                continue

            # Convert fetched data to list of dictionaries
            data = [dict(zip(columns, row)) for row in fetched_data]

            # Convert to DataFrame for analysis
            df = pd.DataFrame(data)

            # Convert 'Date' column to datetime and set as index
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)

            # Enforce correct dtypes
            try:
                df = df.astype({
                    "Open": "float64",
                    "High": "float64",
                    "Low": "float64",
                    "Close": "float64",
                    "Change": "float64",
                    "Change (%)": "float64",
                    "Volume": "int64"
                })
            except Exception as e:
                st.error(f"Data type conversion error for ticker '{ticker}': {e}")
                logging.error(f"Data type conversion error for ticker '{ticker}': {e}")
                continue

            # Ensure all necessary columns are present and correct
            required_columns = ["Open", "High", "Low", "Close", "Change", "Change (%)", "Volume"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                st.error(f"Missing columns in data: {missing_columns}")
                logging.error(f"Missing columns for ticker '{ticker}': {missing_columns}")
                continue

            # Check for any NaN or infinite values
            if df[required_columns].isnull().any().any():
                st.warning("Data contains NaN values. These will be dropped before analysis.")
                logging.warning(f"Data for ticker '{ticker}' contains NaN values.")
                df.dropna(subset=required_columns, inplace=True)

            if not np.isfinite(df[required_columns]).all().all():
                st.warning("Data contains infinite values. These will be dropped before analysis.")
                logging.warning(f"Data for ticker '{ticker}' contains infinite values.")
                df = df[np.isfinite(df[required_columns]).all(axis=1)]

            if df.empty:
                st.warning(f"All data for ticker '{ticker}' was dropped due to NaN or infinite values.")
                logging.warning(f"All data for ticker '{ticker}' was dropped due to NaN or infinite values.")
                continue

            # Define analysis parameters (use your original params)
            analysis_params = {
                "bull_color": '#14D990',
                "bear_color": '#F24968',
                "show_internals": True,
                "internal_sensitivity": 3,  # Options: 3, 5, 8
                "internal_structure": "All",  # Options: "All", "BoS", "CHoCH"
                "show_externals": True,
                "external_sensitivity": 25,  # Options: 10, 25, 50
                "external_structure": "All",  # Options: "All", "BoS", "CHoCH"
                "show_order_blocks": True,
                "swing_order_blocks": 10,
                "show_hhlh": True,
                "show_hlll": True,
                "show_aoe": True,
                "show_prev_day_high": True,
                "show_prev_day_labels": True,
                "show_4h_high": True,
                "show_4h_labels": True,
                "show_fvg": True,
                "contract_violated_fvg": False,
                "close_only_fvg": False,
                "fvg_color": '#F2B807',
                "fvg_transparency": 80,  # Percentage
                "show_fibs": True,
                "show_fib236": True,
                "show_fib382": True,
                "show_fib5": True,
                "show_fib618": True,
                "show_fib786": True,
                "fib_levels": [0.236, 0.382, 0.5, 0.618, 0.786],
                "fib_colors": ['gray', 'lime', 'yellow', 'orange', 'red'],
                "transparency": 0.98,  # For session highlighting
                "data_frequency": '1D'  # Adjust as needed
            }

            # Perform analysis with a spinner
            with st.spinner(f"Performing analysis for '{ticker}'..."):
                try:
                    fig, summary = mxwll_suite_indicator(df, ticker, analysis_params)

                    # Validate that fig is a Plotly figure
                    if not isinstance(fig, go.Figure):
                        st.error(f"Generated figure for ticker '{ticker}' is invalid.")
                        logging.error(f"Generated figure for ticker '{ticker}' is invalid.")
                        continue

                    # Determine which index the ticker belongs to (if any)
                    cursor.execute('SELECT "LISTED_IN" FROM MarketWatch WHERE SYMBOL = ? LIMIT 1;', (ticker,))
                    result = cursor.fetchone()
                    listed_in = result[0] if result and result[0] else "Unknown"

                    # Customize figure based on 'LISTED_IN'
                    if analysis_type == "By Index":
                        fig.update_layout(title=f"{ticker} - {listed_in}")

                    # Display the figure
                    st.plotly_chart(fig, use_container_width=True)
                    st.success(f"Analysis for ticker '{ticker}' completed successfully.")
                    logging.info(f"Analysis for ticker '{ticker}' completed successfully.")

                    # --- Real-Time High_AOI and Potential Profit Calculation ---

                    # Calculate AOI based on the analysis parameters
                    if analysis_params['show_aoe']:
                        try:
                            # Assuming 'summary' contains 'Highest AOI (Red)' and 'Lowest AOI (Green)'
                            high_aoi = summary.get('Highest AOI (Red)')
                            low_aoi = summary.get('Lowest AOI (Green)')
                            last_close = df['Close'].iloc[-1]

                            if low_aoi == 0:
                                st.error(f"Low AOI for ticker '{ticker}' is zero, cannot calculate distance.")
                                logging.error(f"Low AOI for ticker '{ticker}' is zero, cannot calculate distance.")
                                continue

                            # Calculate Potential Profit (%) based on High_AOI
                            potential_profit = ((high_aoi - last_close) / high_aoi) * 100 if high_aoi else None

                            # Calculate Volatility
                            df['Return'] = df['Close'].pct_change()
                            volatility = df['Return'].std() * np.sqrt(252)  # Annualized volatility

                            # Calculate Distance from Close to Low AOI (Percentage)
                            distance_percentage = ((last_close - low_aoi) / low_aoi) * 100 if low_aoi else None

                            # Calculate Range (TP - SL)
                            range_value = high_aoi - low_aoi if high_aoi and low_aoi else None

                            # Append to comparison metrics if calculation was successful
                            if (potential_profit is not None) and (distance_percentage is not None) and (range_value is not None):
                                comparison_metrics.append({
                                    'Ticker': ticker,
                                    'SL': round(low_aoi, 2),
                                    'TP': round(high_aoi, 2),
                                    'Close': round(last_close, 2),
                                    'Dist (%)': round(distance_percentage, 2),  # Percentage distance
                                    'Range': round(range_value, 2),  # Range between TP and SL
                                    'Profit (%)': round(potential_profit, 2),
                                    'Vol': df['Volume'].iloc[-1],
                                    'Volatility': round(volatility, 2),
                                    'Listed_in': listed_in  # Include listed_in
                                })
                            else:
                                logging.info(f"Ticker '{ticker}' does not meet the profit calculation criteria.")
                        except Exception as e:
                            st.error(f"Error calculating AOI, Potential Profit, or Volatility for '{ticker}': {e}")
                            logging.error(f"Error calculating AOI, Potential Profit, or Volatility for '{ticker}': {e}")

                    # Append summary data to the list
                    summary_list.append(summary)
                except Exception as e:
                    st.error(f"An error occurred during analysis for ticker '{ticker}': {e}")
                    logging.error(f"Error during analysis for ticker '{ticker}': {e}")

        # --- Remove "📊 Potential Profit Data" Table ---
        # Removed as per your request

        # --- Generate Charts and Tables After All Analyses ---

        if comparison_metrics:
            comparison_df = pd.DataFrame(comparison_metrics).drop_duplicates(subset=['Ticker'])

            # --- Add Another Chart Favoring Low Dist, High Profit %, More Volume, Less Volatility ---
            # Calculate a composite score
            # Normalize the metrics
            comparison_df['Norm_Dist'] = 1 - (comparison_df['Dist (%)'] - comparison_df['Dist (%)'].min()) / (comparison_df['Dist (%)'].max() - comparison_df['Dist (%)'].min()) if comparison_df['Dist (%)'].max() != comparison_df['Dist (%)'].min() else 1
            comparison_df['Norm_Profit'] = (comparison_df['Profit (%)'] - comparison_df['Profit (%)'].min()) / (comparison_df['Profit (%)'].max() - comparison_df['Profit (%)'].min()) if comparison_df['Profit (%)'].max() != comparison_df['Profit (%)'].min() else 1
            comparison_df['Norm_Vol'] = (comparison_df['Vol'] - comparison_df['Vol'].min()) / (comparison_df['Vol'].max() - comparison_df['Vol'].min()) if comparison_df['Vol'].max() != comparison_df['Vol'].min() else 1
            comparison_df['Norm_Volatility'] = 1 - (comparison_df['Volatility'] - comparison_df['Volatility'].min()) / (comparison_df['Volatility'].max() - comparison_df['Volatility'].min()) if comparison_df['Volatility'].max() != comparison_df['Volatility'].min() else 1

            # Assign weights to each normalized metric
            weights = {
                'Norm_Dist': 0.4,        # Higher weight for Dist
                'Norm_Profit': 0.3,      # Next for Profit
                'Norm_Vol': 0.2,          # Then Volume
                'Norm_Volatility': 0.1   # Least for Volatility
            }

            # Calculate composite score
            comparison_df['Composite_Score'] = (
                comparison_df['Norm_Dist'] * weights['Norm_Dist'] +
                comparison_df['Norm_Profit'] * weights['Norm_Profit'] +
                comparison_df['Norm_Vol'] * weights['Norm_Vol'] +
                comparison_df['Norm_Volatility'] * weights['Norm_Volatility']
            )

            # Scatter Plot: Composite Score vs Ticker
            # Favoring: High Composite Score
            fig_favor = px.scatter(
                comparison_df,
                x='Ticker',
                y='Composite_Score',
                size='Vol',
                color='Composite_Score',
                hover_data=['Dist (%)', 'Profit (%)', 'Vol', 'Volatility'],
                text='Ticker',
                title='Favoring Stocks: Low Dist, High Profit %, More Volume, Less Volatility',
                labels={
                    'Ticker': 'Ticker',
                    'Composite_Score': 'Composite Score'
                },
                size_max=15,
                color_continuous_scale='RdYlGn'  # Red to Yellow to Green
            )

            fig_favor.update_traces(textposition='top center')
            fig_favor.update_layout(showlegend=True)

            # --- Remove "📈 Potential Profit Scatter Plot" ---
            # Removed as per your request

            # --- Arrange the New Favoring Chart Beside Any Other Chart if Needed ---
            # Since the "Potential Profit Scatter Plot" is removed, we'll only display the new chart.

            # --- Display the New Favoring Chart ---
            st.subheader("📈 Favoring Stocks Chart")
            st.plotly_chart(fig_favor, use_container_width=True)

            # --- Additional Functionality: Table Sorted by Percentage Distance ---

            st.subheader("📋 Stocks Closest to Lowest Floor")

            # Sort the DataFrame by 'Dist (%)' ascending to have closest stocks on top
            sorted_comparison_df = comparison_df.sort_values(by='Dist (%)').reset_index(drop=True)

            # Select and rename columns as per requirement
            distance_table = sorted_comparison_df[['Ticker', 'SL', 'TP', 'Close', 'Dist (%)', 'Range', 'Profit (%)', 'Vol', 'Volatility']].copy()
            distance_table.rename(columns={
                'SL': 'SL',
                'TP': 'TP',
                'Close': 'Close',
                'Dist (%)': 'Dist',
                'Range': 'Range',
                'Profit (%)': 'Profit (%)',
                'Vol': 'Vol',
                'Volatility': 'Volatility'
            }, inplace=True)

            # Display the table
            st.dataframe(distance_table)

            # --- Export the distance sorted table as CSV with Date in Filename ---

            csv_distance = distance_table.to_csv(index=False).encode('utf-8')
            csv_filename = f"TAKE PROFIT AND STOP LOSS FOR THE LATEST DATE {end_date.strftime('%Y-%m-%d')}.csv"
            st.download_button(
                label="📥 Download Distance Sorted Data as CSV",
                data=csv_distance,
                file_name=csv_filename,
                mime='text/csv',
            )

            # --- Display Top 5 Performers ---
            st.subheader("🏆 Top 5 Performers")

            # Sort by Profit (%) descending to get top performers
            top_performers = comparison_df.sort_values(by='Profit (%)', ascending=False).head(5)

            for index, row in top_performers.iterrows():
                st.success(f"**Top Performer:** {row['Ticker']} with a potential profit of {row['Profit (%)']:.2f}% and volatility of {row['Volatility']}%")

        else:
            st.warning("No comparison metrics available to generate the charts and tables.")
            logging.warning("No comparison metrics available after analysis.")
