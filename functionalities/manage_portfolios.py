# functionalities/manage_portfolios.py

import streamlit as st
import logging
from utils.db_manager import (
    get_all_portfolios,
    delete_portfolio,
    get_unique_tickers_from_db,
    get_portfolio_by_name,
    create_portfolio,
    update_portfolio
)
import pandas as pd

def manage_portfolios(conn):
    st.header("📁 Manage Portfolios")

    # Create a tabbed interface
    tabs = st.tabs(["➕ Create New Portfolio", "📋 View Portfolios", "🔄 Update Portfolio", "🗑️ Delete Portfolio"])

    # ---- Tab 1: Create New Portfolio ---- #
    with tabs[0]:
        create_new_portfolio(conn)

    # ---- Tab 2: View Portfolios ---- #
    with tabs[1]:
        view_portfolios(conn)

    # ---- Tab 3: Update Portfolio ---- #
    with tabs[2]:
        update_existing_portfolio(conn)

    # ---- Tab 4: Delete Portfolio ---- #
    with tabs[3]:
        delete_existing_portfolio(conn)  # Ensure this function is defined

def delete_existing_portfolio(conn):
    st.subheader("🗑️ Delete Portfolio")
    portfolios = get_all_portfolios(conn)

    if portfolios:
        portfolio_names = [portfolio['Name'] for portfolio in portfolios]
        selected_portfolio_name = st.selectbox("Select Portfolio to Delete", portfolio_names)

        portfolio = next((p for p in portfolios if p['Name'] == selected_portfolio_name), None)
        if portfolio:
            st.markdown(f"### **Deleting Portfolio: {selected_portfolio_name}**")
            with st.form(key='delete_portfolio_form'):
                confirm = st.checkbox(f"⚠️ Are you sure you want to delete portfolio '{selected_portfolio_name}'?")
                submit_button = st.form_submit_button("🗑️ Delete Portfolio")

            if submit_button:
                if confirm:
                    success = delete_portfolio(conn, portfolio['Portfolio_ID'])
                    if success:
                        st.success(f"✅ Portfolio '{selected_portfolio_name}' deleted successfully.")
                        logging.info(f"Portfolio '{selected_portfolio_name}' deleted successfully.")
                        
                        # Force rerun to update the "View Portfolios" tab
                        st.rerun(scope="app")
                    else:
                        st.error(f"❌ Failed to delete portfolio '{selected_portfolio_name}'.")
                        logging.error(f"Failed to delete portfolio '{selected_portfolio_name}'.")
                else:
                    st.warning("⚠️ Deletion canceled.")
    else:
        st.info("No portfolios available to delete. Please create a portfolio first.")
        logging.info("No portfolios available for deletion.")

def create_new_portfolio(conn):
    st.subheader("➕ Create New Portfolio")

    max_tickers = 50  # Maximum number of tickers allowed in a portfolio

    # Fetch all unique symbols from the Ticker table for validation
    all_symbols = get_unique_tickers_from_db(conn)  # Ensure this fetches the correct tickers

    # Initialize form counter
    if 'portfolio_form_counter' not in st.session_state:
        st.session_state['portfolio_form_counter'] = 0

    # Portfolio Creation Form with dynamic key
    with st.form(key=f'create_portfolio_form_{st.session_state["portfolio_form_counter"]}'):
        portfolio_name = st.text_input(
            "Enter Portfolio Name:",
            max_chars=50,
            key='portfolio_name',
            help="Provide a unique name for your portfolio."
        )

        # Bulk Add Tickers
        bulk_tickers_input = st.text_area(
            "📥 Bulk Add Tickers to Portfolio (Enter multiple symbols separated by commas or spaces)",
            "",
            key='bulk_tickers_input',
            help="Provide symbols separated by commas (e.g., AAPL, GOOGL, MSFT) or spaces."
        )

        submit_button = st.form_submit_button(label='Create Portfolio')

    if submit_button:
        # Validate Portfolio Name
        if not portfolio_name:
            st.error("❗ Portfolio name cannot be empty. Please enter a valid name.")
            logging.error("User attempted to create a portfolio without a name.")
            st.stop()

        # Validate Bulk Tickers Input
        if not bulk_tickers_input:
            st.error("❗ No tickers provided. Please add at least one ticker.")
            logging.error("User attempted to create a portfolio without tickers.")
            st.stop()

        # Parse Bulk Tickers
        bulk_symbols = [symbol.strip().upper() for symbol in bulk_tickers_input.replace(',', ' ').split()]
        unique_bulk_symbols = list(set(bulk_symbols))  # Remove duplicates
        added_symbols = []
        non_existent_symbols = []

        for symbol in unique_bulk_symbols:
            if symbol not in added_symbols:
                if len(added_symbols) < max_tickers:
                    if symbol in all_symbols:
                        added_symbols.append(symbol)
                    else:
                        non_existent_symbols.append(symbol)
                        st.warning(f"⚠️ Ticker '{symbol}' does not exist in Ticker table.")
                        logging.warning(f"Ticker '{symbol}' does not exist in Ticker table.")
                else:
                    st.error(f"❌ Cannot add ticker '{symbol}'. Maximum limit of {max_tickers} tickers reached.")
                    logging.error(f"Cannot add ticker '{symbol}'. Maximum limit of {max_tickers} tickers reached.")

        # Check for Portfolio Name Uniqueness
        existing_portfolio = get_portfolio_by_name(conn, portfolio_name)
        if existing_portfolio:
            st.error(f"❌ Portfolio '{portfolio_name}' already exists. Please choose a different name.")
            logging.error(f"Failed to create portfolio '{portfolio_name}'. It already exists.")
            st.stop()

        if added_symbols:
            # Create Portfolio
            success = create_portfolio(conn, portfolio_name, added_symbols)
            if success:
                st.success(f"✅ Portfolio '{portfolio_name}' created successfully with {len(added_symbols)} tickers.")
                logging.info(f"Portfolio '{portfolio_name}' created with tickers: {added_symbols}.")

                # Increment the form counter to reset the form
                st.session_state['portfolio_form_counter'] += 1

                # Optionally, display a success message or perform other actions
            else:
                st.error(f"❌ Failed to create portfolio '{portfolio_name}'. It may already exist.")
                logging.error(f"Failed to create portfolio '{portfolio_name}'.")
        else:
            st.error("❌ No valid tickers were added to the portfolio. Please check your inputs.")
            logging.error("No valid tickers were added to the portfolio.")

def view_portfolios(conn):
    st.subheader("📋 View Portfolios")
    portfolios = get_all_portfolios(conn)
    if portfolios:
        for portfolio in portfolios:
            st.markdown(f"### **{portfolio['Name']}**")
            st.write(f"**Tickers ({len(portfolio['Stocks'])}):** {', '.join(portfolio['Stocks'])}")
            st.markdown("---")
    else:
        st.info("No portfolios available. Please create a portfolio first.")
        logging.info("No portfolios available to view.")

def update_existing_portfolio(conn):
    st.subheader("🔄 Update Portfolio")
    portfolios = get_all_portfolios(conn)

    if portfolios:
        portfolio_names = [portfolio['Name'] for portfolio in portfolios]
        selected_portfolio_name = st.selectbox("Select Portfolio to Update", portfolio_names)

        portfolio = next((p for p in portfolios if p['Name'] == selected_portfolio_name), None)
        if portfolio:
            st.markdown(f"### **Updating Portfolio: {selected_portfolio_name}**")
            with st.form(key='update_portfolio_form'):
                new_name = st.text_input("New Portfolio Name (leave blank to keep current)", value=portfolio['Name'])
                new_bulk_tickers_input = st.text_area(
                    "📥 Update Tickers (Enter multiple symbols separated by commas or spaces)",
                    ', '.join(portfolio['Stocks']),
                    help="Provide symbols separated by commas (e.g., AAPL, GOOGL, MSFT) or spaces."
                )
                submit_button = st.form_submit_button("🔄 Update Portfolio")

            if submit_button:
                # Parse new tickers
                new_bulk_symbols = [symbol.strip().upper() for symbol in new_bulk_tickers_input.replace(',', ' ').split()]
                unique_new_bulk_symbols = list(set(new_bulk_symbols))
                updated_symbols = []
                non_existent_symbols = []

                for symbol in unique_new_bulk_symbols:
                    if symbol not in updated_symbols:
                        if symbol in get_unique_tickers_from_db(conn):
                            updated_symbols.append(symbol)
                        else:
                            non_existent_symbols.append(symbol)
                            st.warning(f"⚠️ Ticker '{symbol}' does not exist in Ticker table.")
                            logging.warning(f"Ticker '{symbol}' does not exist in Ticker table.")

                # Update Portfolio
                success = update_portfolio(conn, portfolio['Portfolio_ID'], new_name=new_name, new_stocks=updated_symbols)
                if success:
                    st.success(f"✅ Portfolio '{selected_portfolio_name}' updated successfully.")
                    logging.info(f"Portfolio '{selected_portfolio_name}' updated successfully to '{new_name}' with tickers: {updated_symbols}.")

                    # Optionally, force rerun to reflect changes
                    st.rerun(scope="app")
                else:
                    st.error(f"❌ Failed to update portfolio '{selected_portfolio_name}'.")
                    logging.error(f"Failed to update portfolio '{selected_portfolio_name}'.")
    else:
        st.info("No portfolios available to update. Please create a portfolio first.")
        logging.info("No portfolios available to update.")
