import streamlit as st
import pandas as pd

# Function to process the data
def process_data(airflow_file, location_file):
    # Load the data
    df = pd.read_csv(airflow_file)

    # Count the number of rows based on the permalink column
    original_row_count = df['permalink'].count()

    # Remove specified columns
    columns_to_remove_1 = ['category_groups', 'company_industry', 'cb_markets', 'country_code',
                           'num_funding_rounds', 'total_funding', 'total_funding_currency_code',
                           'linkedin_url', 'last_funding_on', 'organizations_msa']
    df.drop(columns=columns_to_remove_1, inplace=True)

    # Process min and max columns
    df['min'] = df['min'].fillna(0).astype(int)
    df['max'] = df['max'].fillna(0).astype(int)
    df['min-max'] = df.apply(
        lambda row: f"{row['min']}+" if row['max'] == 0 and row['min'] == 10000 else
                    f"{row['min']}-{row['max']}" if row['min'] != 0 and row['max'] != 0 else
                    "-", axis=1)

    # Remove rows where status is blank or contains "closed"
    rows_before_status_filter = df['permalink'].count()
    df = df[df['status'].notna()]
    df = df[~df['status'].str.contains('closed', case=False, na=False)]
    rows_after_status_filter = df['permalink'].count()
    rows_removed_due_to_status = rows_before_status_filter - rows_after_status_filter

    # Split organizations_location and create new columns
    location_split = df['organizations_location'].str.split(',', expand=True)
    location_split.columns = [f'organizations_location_{i+1}' for i in range(location_split.shape[1])]
    df = pd.concat([df, location_split], axis=1)
    df['organizations_location_3'] = df['organizations_location_1'] + ',' + df['organizations_location_2']

    # Further processing
    df['total_funding_usd'] = round(df['total_funding_usd'] / 1000000, 2)
    df['founded_on'] = pd.to_datetime(df['founded_on'])
    df['founded_on'] = df['founded_on'].dt.year

    # Remove additional columns
    columns_to_remove = ['max', 'min', 'organizations_location', 'city', 'region']
    df.drop(columns=columns_to_remove, inplace=True)

    # Handle potential columns that may or may not exist
    if 'organizations_location_4' in df.columns:
        df.drop(columns=['organizations_location_4'], inplace=True)

    # Rename columns
    df.rename(columns={'organizations_location_3': 'Headquarter location'}, inplace=True)
    df.rename(columns={'organizations_location_1': 'Hq1'}, inplace=True)
    df.rename(columns={'organizations_location_2': 'Hq2'}, inplace=True)
    df.rename(columns={'founded_on': 'Year of establishment'}, inplace=True)
    df.rename(columns={'total_funding_usd': 'Total funding'}, inplace=True)
    df.rename(columns={'min-max': 'Employee count'}, inplace=True)

    # Merge with location data
    location_df = pd.read_csv(location_file)
    result_df = pd.merge(df, location_df[['City', 'Country', 'Region']], left_on='Hq1', right_on='City', how='left')

    # Return the processed dataframe and counts for the report
    final_row_count = result_df['permalink'].count()
    return result_df, original_row_count, rows_removed_due_to_status, final_row_count

# Streamlit UI
st.title("Data Processing Application")

# Upload the files
airflow_file = st.file_uploader("Upload the Airflow file", type="csv")
location_file = st.file_uploader("Upload the Location file", type="csv")

if airflow_file and location_file:
    operator = st.sidebar.radio("Select Filter Logic 1 (AND/OR)", ["AND", "OR"])
    result_df, original_row_count, rows_removed_due_to_status, final_row_count = process_data(airflow_file, location_file)

    # Display overall view
    st.subheader("Overall Dataset Description")
    st.write(f"**Total number of rows in the original dataset:** {original_row_count}")
    st.write(f"**Number of rows removed due to 'closed' in status column:** {rows_removed_due_to_status}")
    st.write(f"**Final number of rows in the dataset:** {final_row_count}")

    # Year of Establishment Filter
    year_min, year_max = result_df["Year of establishment"].min(), result_df["Year of establishment"].max()
    year_range = st.sidebar.slider("Filter by Year of Establishment", min_value=year_min, max_value=year_max, value=(year_min, year_max))

    # Total Funding Filter
    funding_min, funding_max = result_df["Total funding"].min(), result_df["Total funding"].max()
    funding_range = st.sidebar.slider("Filter by Total Funding (in million USD)", min_value=funding_min, max_value=funding_max, value=(funding_min, funding_max))
    
    result_df.fillna('-', inplace=True)
    st.sidebar.header("Filter Options")

    filtered_df = result_df.copy()

    # Dynamic Filter Options
    filter_columns = st.sidebar.multiselect("Select Columns to Filter", options=result_df.columns)

    for col in filter_columns:
        filter_logic = st.sidebar.selectbox(f"Select Filter Logic for {col}", ["Startswith", "Endswith", "Contains", "==", "!=", "IN", "NOT IN", "IsNull", "IsNotNull", "Suggestion"])
        
        if filter_logic == "Startswith":
            value = st.sidebar.text_input(f"{col} starts with")
            filtered_df = filtered_df[filtered_df[col].str.startswith(value, na=False)]
        elif filter_logic == "Endswith":
            value = st.sidebar.text_input(f"{col} ends with")
            filtered_df = filtered_df[filtered_df[col].str.endswith(value, na=False)]
        elif filter_logic == "Contains":
            value = st.sidebar.text_input(f"{col} contains")
            filtered_df = filtered_df[filtered_df[col].str.contains(value, na=False)]
        elif filter_logic == "==":
            value = st.sidebar.text_input(f"{col} equals")
            filtered_df = filtered_df[filtered_df[col] == value]
        elif filter_logic == "!=":
            value = st.sidebar.text_input(f"{col} not equals")
            filtered_df = filtered_df[filtered_df[col] != value]
        elif filter_logic == "IN":
            values = st.sidebar.text_area(f"{col} in (comma-separated values)")
            values = [v.strip() for v in values.split(",")]
            filtered_df = filtered_df[filtered_df[col].isin(values)]
        elif filter_logic == "NOT IN":
            values = st.sidebar.text_area(f"{col} not in (comma-separated values)")
            values = [v.strip() for v in values.split(",")]
            filtered_df = filtered_df[~filtered_df[col].isin(values)]
        elif filter_logic == "IsNull":
            filtered_df = filtered_df[filtered_df[col].isna()]
        elif filter_logic == "IsNotNull":
            filtered_df = filtered_df[filtered_df[col].notna()]
        elif filter_logic == "Suggestion":
            unique_items = filtered_df[col].unique()
            selected_items = st.sidebar.multiselect(f"Select {col}", options=unique_items)
            filtered_df = filtered_df[filtered_df[col].isin(selected_items)]

    # Display query result
    query_row_count = filtered_df['permalink'].count()
    st.subheader("Query Description")
    st.write(f"**Current number of rows after applying the query:** {query_row_count}")

    st.write("Filtered Data")
    st.dataframe(filtered_df)

    # Option to download the filtered data
    csv = filtered_df.to_csv(index=False).encode('utf-8')
    st.download_button(label="Download Filtered Data", data=csv, file_name='filtered_data.csv', mime='text/csv')
