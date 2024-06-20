import streamlit as st
import os
import weaviate
import time
import re

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

collectionname = os.getenv('COLLECTIONNAME')

st.set_page_config(
    page_title="Simple Live Transcription Summary",
    page_icon="🧊",
    layout="wide",
)

st.title('Simple Live Transcription Summary')

st.markdown('A continuously updating summary of the ongoing discussion based on a live transcript.')

# ADD DEBUG INFO
st.write(f" * Using collection name: {collectionname}")
st.write(f" * Using URL: {os.getenv('WEAVIATE_REST_ENDPOINT')}") 

auth_config = weaviate.auth.AuthApiKey(api_key=os.getenv('WEAVIATE_APIKEY'))

client = weaviate.Client(
    url=os.getenv("WEAVIATE_REST_ENDPOINT"),  # URL of your Weaviate instance
    auth_client_secret=auth_config,  # (Optional) If the Weaviate instance requires authentication
    timeout_config=(5, 15),  # (Optional) Set connection timeout & read timeout time in seconds
    additional_headers={  # (Optional) Any additional headers; e.g. keys for API inference services
        "X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY"),  # Replace with your OpenAI key
    }
)

class_name = collectionname

# Define the filter to get entries created after a certain time
where_filter = {
    "path": ["earliestTimestamp"],
    "operator": "GreaterThan",
    # Use either `valueDate` with a `RFC3339` datetime or `valueText` as Unix epoch milliseconds
    "valueDate": "2024-06-05T19:32:58Z"  # Example Unix epoch milliseconds
}

# Create the stop button
stop_button_placeholder = st.empty()
stop_button = stop_button_placeholder.button("Stop")

# Create a placeholder for the transcription summary
summary_placeholder = st.empty()

# Loop until the stop button is clicked
while not stop_button:
    # Query the database
    response = (
        client.query
        .get(class_name, ["speaker", "segment", "summary", "earliestTimestamp"])
        .with_additional("creationTimeUnix")
        .with_where(where_filter)
        .do()
    )

    transcription_chunks = response['data']['Get'][class_name]

    try:
        if response['errors']:
            print(f"ERRORS: {response}")
    except:
        pass

    data = []

    for chunk in transcription_chunks:
        modified_segment = re.sub(r'\d{4}-\d{2}-\d{2}T', '', chunk['segment'])
        entry = {
            'speaker': chunk['speaker'],
            'segment': modified_segment,
            'summary': chunk['summary']
        }
        data.append(entry)

    # print(data)
    # Create DataFrame
    # df = pd.DataFrame(data)

    # Clear the placeholder and update it with the latest data
    with summary_placeholder.container():
        for entry in data:
            st.markdown("___")
            st.markdown(f"### {entry['segment']}")
            st.markdown(f"**SPEAKER:** {entry['speaker']}")
            st.markdown(f"**SUMMARY:** \n\n{entry['summary']}")

    # Sleep for a while before the next iteration
    print("Sleeping for 30 seconds...")
    time.sleep(30)