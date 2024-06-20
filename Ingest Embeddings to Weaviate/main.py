from quixstreams import Application
import weaviate
import weaviate.classes as wvc
import os
import logging
import uuid as uu

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"WEAVIATE URL {os.environ['weaviate_rest_endpoint']}")

# Initialize the Weaviate client. Replace the placeholder values with your actual Weaviate instance details.
wclient = weaviate.connect_to_wcs(
    cluster_url=os.environ["weaviate_rest_endpoint"],  # Replace with your WCS URL
    auth_credentials=weaviate.auth.AuthApiKey(os.environ["weaviate_apikey"]),  # Replace with your WCS key
    # OPENAI KEY ONLY NEEDED FOR GENERATIVE SEARCH
    # headers={'X-OpenAI-Api-key': os.getenv("OPENAI_APIKEY")}  # Replace with your OpenAI API key
)

collectionname = os.environ["collectionname"]
if not wclient.collections.exists(collectionname): # if the schema/collection is missing create it
    transcripts = wclient.collections.create(
        name=collectionname,
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
        vector_index_config=wvc.config.Configure.VectorIndex.hnsw(
            distance_metric=wvc.config.VectorDistances.COSINE # select prefered distance metric
        ),
        properties=[
            wvc.config.Property(
                name="speaker",
                data_type=wvc.config.DataType.TEXT
            ),
            wvc.config.Property(
                name="segment",
                data_type=wvc.config.DataType.TEXT
            ),
            wvc.config.Property(
                name="chunks",
                data_type=wvc.config.DataType.TEXT
            ),
            wvc.config.Property(
                name="chunklen",
                data_type=wvc.config.DataType.TEXT
            ),
            wvc.config.Property(
                name="windowlen",
                data_type=wvc.config.DataType.TEXT
            ),
            wvc.config.Property(
                name="earliestTimestamp",
                data_type=wvc.config.DataType.TEXT
            ),
        ]
    )

else:
    transcripts = wclient.collections.get(collectionname) # if the collection already existed just refer to it

# Define the ingestion function
def ingest_vectors(row):
    try:
        uuid = transcripts.data.insert(
        properties={
                "summary": row["summary"],
                "speaker": row["speaker"],
                "segment": row["segment"],
                "chunks": row["chunks"],
                "chunklen": str(row["chunklen"]),
                "windowlen": row["windowlen"],
                "earliestTimestamp": row["earliestTimestamp"]
            },
        vector=row["embeddings"])

        print(f'Ingested vector entry id: "{uuid}"...')

    except Exception as e:
        print(f"Error because of: {e}")
 

app = Application(
    consumer_group=os.environ["groupname"],
    auto_offset_reset="earliest",
    auto_create_topics=True,  # Quix app has an option to auto create topics
)

# Define an input topic with JSON deserializer
input_topic = app.topic(os.environ['input'], value_deserializer="json")

# Initialize a streaming dataframe based on the stream of messages from the input topic:
sdf = app.dataframe(topic=input_topic)

# INGESTION HAPPENS HERE
### Trigger the embedding function for any new messages(rows) detected in the filtered SDF
sdf = sdf.update(lambda row: ingest_vectors(row))

if __name__ == "__main__":
    try:
        # Start message processing
        app.run(sdf)
    except KeyboardInterrupt:
        logger.info("Exiting.")
        run = False
    finally:
        wclient.close()
        logger.info("Connection to Weaviate closed")
        logger.info("Exiting")
    
    