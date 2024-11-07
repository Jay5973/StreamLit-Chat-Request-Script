import streamlit as st
import pandas as pd
import json

# Streamlit App Setup
st.title("Astrology Chat Data Processor")

# Step 1: Upload Files
raw_file = st.file_uploader("Upload raw_data.csv", type="csv")
completed_file = st.file_uploader("Upload chat_completed_data.csv", type="csv")

if raw_file and completed_file:
    
    # Step 2: Extract JSON Data from raw_data.csv and Save to a DataFrame
    def extract_json(raw_df, json_column):
        json_data = []
        for item in raw_df[json_column]:
            try:
                data = json.loads(item)
                json_data.append(data)
            except (json.JSONDecodeError, TypeError):
                continue
        json_df = pd.json_normalize(json_data)
        combined_df = pd.concat([raw_df, json_df], axis=1)
        return combined_df

    # Step 3: Process Events to Calculate Unique Users
    class UniqueUsersProcessor:
        def __init__(self, raw_df, completed_df):
            self.raw_df = raw_df
            self.completed_df = completed_df

        def process_chat_intake_requests(self):
            # Filter for chat intake events
            intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_intake_submit')]
            
            # Convert event_time to datetime and adjust timezone
            intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            
            # Create date and hour columns for grouping
            intake_events['date'] = intake_events['event_time'].dt.date
            intake_events['hour'] = intake_events['event_time'].dt.hour
            
            # Count unique users for each astrologer by date and hour
            user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
            
            # Rename columns for clarity
            user_counts.rename(columns={'user_id': 'chat_intake_requests'}, inplace=True)
            
            return user_counts

        def process_chat_completed_events(self):
            completed_events = self.completed_df[(self.completed_df['status'] == 'COMPLETED') & (self.completed_df['type'].isin(['FREE', 'PAID']))]
            completed_events['createdAt'] = pd.to_datetime(completed_events['createdAt'], utc=True)
            completed_events['date'] = completed_events['createdAt'].dt.date
            completed_events['hour'] = completed_events['createdAt'].dt.hour
            completed_counts = completed_events.groupby(['date', 'hour'])['userId'].nunique().reset_index()
            completed_counts.rename(columns={'userId': 'chat_completed'}, inplace=True)
            return completed_counts

        def process_paid_chat_completed_events(self):
            paid_events = self.completed_df[(self.completed_df['status'] == 'COMPLETED') & (self.completed_df['type'] == 'PAID')]
            paid_events['createdAt'] = pd.to_datetime(paid_events['createdAt'], utc=True)
            paid_events['date'] = paid_events['createdAt'].dt.date
            paid_events['hour'] = paid_events['createdAt'].dt.hour
            paid_counts = paid_events.groupby(['date', 'hour'])['userId'].nunique().reset_index()
            paid_counts.rename(columns={'userId': 'paid_chats_completed'}, inplace=True)
            return paid_counts

    # Read CSV files
    raw_df = pd.read_csv(raw_file)
    completed_df = pd.read_csv(completed_file)

    # Step 4: Process Data
    raw_df = extract_json(raw_df, 'other_data')
    processor = UniqueUsersProcessor(raw_df, completed_df)
    
    # Process each event type
    intake_data = processor.process_chat_intake_requests()
    completed_data = processor.process_chat_completed_events()
    paid_completed_data = processor.process_paid_chat_completed_events()

    # Combine results
    final_results = intake_data
    final_results = pd.merge(final_results, completed_data, on=['date', 'hour'], how='outer')
    final_results = pd.merge(final_results, paid_completed_data, on=['date', 'hour'], how='outer')
    
    # Display final output
    st.write("### Final Processed Data")
    st.dataframe(final_results)

    # Option to download final data
    csv = final_results.to_csv(index=False)
    st.download_button("Download Final Data as CSV", data=csv, file_name="combined_data_final_hour_wise.csv", mime="text/csv")
else:
    st.info("Please upload all required CSV files to proceed.")
