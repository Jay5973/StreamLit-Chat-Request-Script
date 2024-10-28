import streamlit as st
import pandas as pd
import json

st.title("Chat Event Data Processor")

# Step 1: Upload raw data file and JSON extraction
uploaded_file = st.file_uploader("Upload CSV file", type="csv")

if uploaded_file:
    # Parameters
    json_column = st.text_input("Enter JSON column name for extraction:", "other_data")
    output_file = 'combined_data.csv'

    # Extract JSON data and save
    def extract_json_and_save_all(file_path, json_column, output_file):
        df = pd.read_csv(file_path)
        json_data = []
        for item in df[json_column]:
            try:
                data = json.loads(item)
                json_data.append(data)
            except (json.JSONDecodeError, TypeError):
                continue
        json_df = pd.json_normalize(json_data)
        combined_df = pd.concat([df, json_df], axis=1)
        combined_df.to_csv(output_file, index=False)
        st.success(f"All data with extracted JSON columns saved to {output_file}")
        return combined_df

    raw_data = extract_json_and_save_all(uploaded_file, json_column, output_file)

    # Step 2: Process Unique Users for 'chat_intake_submit'
    class UniqueUsersProcessor:
        def __init__(self, raw_data):
            self.raw_data = raw_data

        def process_unique_users_intake(self):
            chat_accepted_events = self.raw_data[self.raw_data['event_name'] == 'chat_intake_submit']
            chat_accepted_events['event_time'] = pd.to_datetime(chat_accepted_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            chat_accepted_events['date'] = chat_accepted_events['event_time'].dt.date
            chat_accepted_events['hour'] = chat_accepted_events['event_time'].dt.hour
            return chat_accepted_events.groupby(['date', 'hour']).agg(
                chat_intake_users=('user_id', 'nunique'),
                chat_intake_submits=('waitingListId', 'nunique')
            ).reset_index()

        def process_unique_users_accept(self):
            chat_intake_submit_events = self.raw_data[self.raw_data['event_name'] == 'chat_intake_submit']
            valid_user_ids = chat_intake_submit_events['user_id'].unique()
            chat_accepted_events = self.raw_data[(self.raw_data['event_name'] == 'accept_chat') & (self.raw_data['clientId'].isin(valid_user_ids))]
            chat_accepted_events['event_time'] = pd.to_datetime(chat_accepted_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            chat_accepted_events['date'] = chat_accepted_events['event_time'].dt.date
            chat_accepted_events['hour'] = chat_accepted_events['event_time'].dt.hour
            return chat_accepted_events.groupby(['date', 'hour']).agg(
                chat_accept_users=('clientId', 'nunique'),
                chat_accept_total=('waitingListId', 'nunique')
            ).reset_index()

    processor = UniqueUsersProcessor(raw_data)
    intake_data = processor.process_unique_users_intake()
    accept_data = processor.process_unique_users_accept()

    # Step 3: Merge and Display Final Results
    if 'final_results' not in st.session_state or st.session_state.final_results.empty:
        st.session_state.final_results = intake_data
    else:
        st.session_state.final_results = pd.merge(st.session_state.final_results, intake_data, on=['date', 'hour'], how='outer')

    if 'final_results' not in st.session_state or st.session_state.final_results.empty:
        st.session_state.final_results = accept_data
    else:
        st.session_state.final_results = pd.merge(st.session_state.final_results, accept_data, on=['date', 'hour'], how='outer')

    st.write("Final Merged Results:")
    st.dataframe(st.session_state.final_results)

    # Option to Download Results
    csv_data = st.session_state.final_results.to_csv(index=False).encode('utf-8')
    st.download_button("Download Final Results as CSV", data=csv_data, file_name="final_results.csv")
