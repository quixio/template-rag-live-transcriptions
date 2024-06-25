import os
from quixstreams import Application, State
from datetime import datetime, timedelta

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(
    consumer_group=os.environ["groupname"],
    auto_create_topics=True, 
    auto_offset_reset="earliest",
    use_changelog_topics=False
)

input_topic = app.topic(os.environ["input"], value_deserializer='json')
output_topic = app.topic(os.environ["output"], value_serializer='json')

sdf = app.dataframe(input_topic)

sdf = sdf[sdf.contains('createdTimestamp')]

chunkid = 0
deltaminutes = int(os.environ["deltaminutes"])
time_window = timedelta(minutes=deltaminutes)

def chunk_transcriptions(row: dict, state: State):
    global chunkid

    # Retrieve current chunk, earliest timestamp from state
    chunk = state.get('chunk', [])
    earliest_timestamp_str = state.get('earliest_timestamp', None)
    earliest_timestamp = datetime.fromisoformat(earliest_timestamp_str) if earliest_timestamp_str else None


      # Parse the current row's earliest timestamp
    currentrow_timestamp = datetime.fromisoformat(row['createdTimestamp'])

    # Initialize earliest timestamp if not set
    if earliest_timestamp is None:
        earliest_timestamp = currentrow_timestamp

    # Check if the current timestamp falls outside the 5-minute window
    if currentrow_timestamp - earliest_timestamp > time_window:
        # Send the current chunk to the downstream topic
        finalchunks = " ".join(chunk)
        row_to_send = {
            "speaker": row["speaker"],
            "segment": f"FROM: {earliest_timestamp.isoformat(timespec='seconds')} TO: {currentrow_timestamp.isoformat(timespec='seconds')}",
            "chunks": finalchunks,
            "chunklen": len(finalchunks.split()),  # Word count
            "windowlen": f"{deltaminutes} minute(s)",
            "earliestTimestamp": earliest_timestamp.isoformat()
        }
        state.set('chunk', [row['transcription']])  # Start new chunk with current row
        state.set('earliest_timestamp', currentrow_timestamp.isoformat())
        chunkid += 1
        return row_to_send

    # Append new transcription words to chunk
    chunk.extend(row['transcription'].split())

    # Update state with current chunk and earliest timestamp
    state.set('chunk', chunk)
    state.set('earliest_timestamp', earliest_timestamp.isoformat())

    return None

# Apply the chunk_transcriptions function to the row
sdf = sdf.apply(chunk_transcriptions, stateful=True)

# Filter out None rows
sdf = sdf.filter(lambda row: row is not None)

# Print the row with this inline function
sdf = sdf.update(lambda row: print(row))

# Publish the updated row to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)