# Import Python packages
import os
import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from datetime import datetime

st.title(':book: CSAT Annotation Tool')

#more secure version using environment variables
def create_session():
   #Creates a Snowflake session and caches it for performance optimization.
    return Session.builder.configs(
        {"user" :os.getenv("SNOWFLAKE_USER"),
        "password" : os.getenv("SNOWFLAKE_PASSWORD"),
        "account" : os.getenv("SNOWFLAKE_ACCOUNT")}).create()

# Get Snowflake session
session=create_session()
session = get_active_session()

if session is None:
    st.error("Failed to connect to Snowflake. Please check your credentials.")
else:
    st.success("Connected to Snowflake!")

######### READ FUNCTION #########

# Function to fetch data based on case number
def fetch_case_data(case_number):
    query = f"""
    SELECT CASE_NUMBER, PARTICIPANT_NAME, ACCOUNT_NAME, CARE_AGENT 
    FROM STAGING_DB.DEV.CSAT_SCORES_MATERIALIZED
    WHERE CASE_NUMBER = '{case_number}'
    """
    data = session.sql(query).to_pandas()
    return data

# Function to fetch distinct owner names for the dropdown
def fetch_owner_names():
    query = """
    SELECT NAME 
    FROM ANALYTICS_DB.SALESFORCE.USER
    WHERE (LOWER(department) LIKE '%care%' OR LOWER(department) LIKE '%payment%')
    AND is_active = TRUE
    ORDER BY NAME;
    """
    result = session.sql(query).collect()
    owner_names = [row[0] for row in result]  # Assuming row[0] contains NAME
    return owner_names

# Streamlit UI for reading data
def read():
    st.title('Select a CSAT Case')

    # Input box for case number
    case_number = st.text_input("Enter Case Number and Hit ENTER:")

    # Store the case number in session state
    if case_number:
        st.session_state.case_number = case_number
        with st.spinner('Searching for case data...'):
            # Fetch and display data for the provided case number
            case_data = fetch_case_data(case_number)
            if not case_data.empty:
                st.session_state.case_searched = True  # Set flag to True
                st.dataframe(case_data)
            else:
                st.session_state.case_searched = False  # Case number not found
                st.write("Case Not Found - Please Try Another Number.")
    else:
        st.session_state.case_searched = False  # No case number entered yet

######## WRITE FUNCTION ########

# Function to insert data into a specified table in Snowflake
def insert_into_table(values_dict):
    # Check if any field is empty
    for key, value in values_dict.items():
        if not value:
            st.error(f"Field '{key}' cannot be empty.")
            return
            
    columns = ', '.join(values_dict.keys())
    values = ', '.join([f"'{value}'" for value in values_dict.values()])
    # Building the INSERT query
    query = f"INSERT INTO ANALYTICS_DB.CUSTOMER_CARE.CSAT_NOTES ({columns}) VALUES ({values})"

    # Execute the INSERT INTO statement
    try:
        session.sql(query).collect()
        st.success(f"Your notes have been saved!")
    except Exception as e:
        st.error(f"Error inserting values: {str(e)}")

# Streamlit UI for writing data
def write():
    # Ensure the WRITE section is displayed only if a case number has been searched
    if 'case_searched' in st.session_state and st.session_state.case_searched:
        st.title('Add Notes to your CSAT Case')

        st.write(f"Case Number: {st.session_state.case_number}")

        # Fetch distinct owner names for dropdown
        owner_names = fetch_owner_names()

        # Initialize session state for form fields if not present
        if 'selected_commenter' not in st.session_state:
            st.session_state.selected_commenter = None
            st.session_state.sentiment_good = False
            st.session_state.sentiment_bad = False
            st.session_state.follow_up_yes = False
            st.session_state.follow_up_no = False
            st.session_state.notes = ""
            st.session_state.date_created = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Form to collect data
        with st.form(key='insert_form'):
            st.subheader('Please complete all of the following fields:')

            # Fetch and set default values for form fields
            selected_commenter = st.selectbox('Commenter:', options=owner_names, key='selected_commenter')

            st.markdown('---')  # Divider line for separation

            # Sentiment section
            st.subheader('Sentiment')
            sentiment_col1, sentiment_col2 = st.columns(2)
            with sentiment_col1:
                sentiment_good = st.checkbox('Good', key='sentiment_good')
            with sentiment_col2:
                sentiment_bad = st.checkbox('Bad', key='sentiment_bad')

            sentiment = None
            if sentiment_good and not sentiment_bad:
                sentiment = 'Good'
            elif sentiment_bad and not sentiment_good:
                sentiment = 'Bad'

            st.markdown('---')  # Divider line for separation

            # Follow-up section
            st.subheader('Follow-up?')
            follow_up_col1, follow_up_col2 = st.columns(2)
            with follow_up_col1:
                follow_up_yes = st.checkbox('Yes', key='follow_up_yes')
            with follow_up_col2:
                follow_up_no = st.checkbox('No', key='follow_up_no')

            follow_up = None
            if follow_up_yes and not follow_up_no:
                follow_up = 'Y'
            elif follow_up_no and not follow_up_yes:
                follow_up = 'N'

            st.markdown('---')  # Divider line for separation

            # Notes section
            st.subheader('Notes')
            notes = st.text_area('Enter your notes here:', max_chars=499, key='notes')

            # The current timestamp for the `DATE_CREATED` field
            date_created = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Handle form submission
            submit_button = st.form_submit_button(label='Enter')

            if submit_button:
                if selected_commenter and sentiment and follow_up and notes:
                    with st.spinner('Saving your notes...'):
                        # Collect data into dictionary
                        values_dict = {
                            'COMMENTER': selected_commenter,
                            'DATE_CREATED': date_created,
                            'CASE_NUMBER': st.session_state.case_number,
                            'SENTIMENT': sentiment,
                            'NOTES': notes,
                            'FOLLOW_UP': follow_up
                        }

                        insert_into_table(values_dict)
                else:
                    st.error("Please fill out all required fields before submitting.")

        # # Add a button to reset the form and state
        # if st.button('Reset Form'):
        #     st.session_state.clear()  # Clear all session state
        #     st.experimental_rerun()    # Rerun the app to reset the form and read section

# Run the Streamlit app
if __name__ == "__main__":
    read()
    write()
