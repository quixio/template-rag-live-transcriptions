import os
from quixstreams import Application, State

app = Application(
    consumer_group=os.environ["groupname"],
    auto_create_topics=True, 
    auto_offset_reset="earliest",
    use_changelog_topics=False
)

input_topic = app.topic(os.environ["input"], value_deserializer='json')
output_topic = app.topic(os.environ["output"], value_serializer='json')

sdf = app.dataframe(input_topic)

chunksize = int(os.environ["chunksize"]) # chunk size in words
overlapsize = int(os.environ["overlapsize"]) # overlap size in words
chunkid = 0

def chunk_transcriptions(row, state):
    global chunkid

    # Retrieve current chunk, overlap from state
    chunk = state.get('chunk', [])
    overlap = state.get('overlap', [])

    # Append new transcription words to chunk
    chunk.extend(row['transcription'].split())

    chunks_to_send = []

    while len(chunk) >= chunksize:
        try:
            # Create a chunk of chunksize words
            chunk_to_send = chunk[:chunksize]

            # Add the chunk to the list to be sent to downstream topic
            print(f"Created chunk: '{' '.join(chunk_to_send)}'...")
            chunks_to_send.append(' '.join(chunk_to_send))

            # Set new overlap of overlapsize words
            overlap = chunk[chunksize-overlapsize:chunksize]

            # Reset chunk to overlap plus new data
            chunk = overlap + chunk[chunksize:]
            
        except Exception as e:
            print(f"An error occurred in chunk_transcriptions: {e}")
            raise  # Re-raise the exception to ensure it's not silently swallowed

    # Update state with current chunk, overlap
    state.set('chunk', chunk)
    state.set('overlap', overlap)
    
    chunkid += 1
    finalchunks = " ".join(chunks_to_send)
    row = {
        "speaker": row["speaker"],
        "chunkid": chunkid,
        "chunks": finalchunks,
        "chunklen": int(len(finalchunks.split()))  # Word count
    }

    return row

# apply the result of the count_names function to the row
sdf = sdf.apply(chunk_transcriptions, stateful=True)

sdf = sdf[sdf['chunklen'] > (chunksize-1)]

# print the row with this inline function
sdf = sdf.update(lambda row: print(row))

# publish the updated row to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)