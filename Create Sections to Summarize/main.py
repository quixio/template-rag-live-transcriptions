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

    # Retrieve current chunks and timestamps from state
    chunks = state.get('chunks', {})
    timestamps = state.get('timestamps', {})

    speaker = row['speaker']
    currentrow_timestamp = datetime.fromisoformat(row['createdTimestamp'])

    # Initialize speaker-specific chunk and timestamp if not set
    if speaker not in chunks:
        chunks[speaker] = []
        timestamps[speaker] = currentrow_timestamp

    earliest_timestamp = timestamps[speaker]

    # Check if the current timestamp falls outside the time window
    if currentrow_timestamp - earliest_timestamp > time_window:
        # Send the current chunk to the downstream topic
        finalchunks = " ".join(chunks[speaker])
        row_to_send = {
            "speaker": speaker,
            "segment": f"FROM: {earliest_timestamp.isoformat(timespec='seconds')} TO: {currentrow_timestamp.isoformat(timespec='seconds')}",
            "chunks": finalchunks,
            "chunklen": len(finalchunks.split()),  # Word count
            "windowlen": f"{deltaminutes} minute(s)",
            "earliestTimestamp": earliest_timestamp.isoformat()
        }
        # Start new chunk with current row for the speaker
        chunks[speaker] = [row['transcription']]
        timestamps[speaker] = currentrow_timestamp
        chunkid += 1
        state.set('chunks', chunks)
        state.set('timestamps', timestamps)
        return row_to_send

    # Append new transcription words to speaker-specific chunk
    chunks[speaker].extend(row['transcription'].split())

    # Update state with current chunks and timestamps
    state.set('chunks', chunks)
    state.set('timestamps', timestamps)

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