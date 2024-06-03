import os
import weaviate
from langchain.vectorstores.weaviate import Weaviate
from langchain_community.retrievers import (
    WeaviateHybridSearchRetriever,
)
from langchain_core.documents import Document
import logging
from quixstreams import Application

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"WEAVIATE URL {os.environ['weaviate_rest_endpoint']}")

auth_config = weaviate.auth.AuthApiKey(api_key=os.getenv("weaviate_apikey"))

# Initialize the Weaviate client. Replace the placeholder values with your actual Weaviate instance details.
client = weaviate.Client(
  url=os.getenv("weaviate_rest_endpoint"),  # URL of your Weaviate instance
  auth_client_secret=auth_config,  # (Optional) If the Weaviate instance requires authentication
  timeout_config=(5, 15),  # (Optional) Set connection timeout & read timeout time in seconds
  additional_headers={  # (Optional) Any additional headers; e.g. keys for API inference services
    #"X-Cohere-Api-Key": "YOUR-COHERE-API-KEY",            # Replace with your Cohere key
    #"X-HuggingFace-Api-Key": "YOUR-HUGGINGFACE-API-KEY",  # Replace with your Hugging Face key
    "X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY"),            # Replace with your OpenAI key
  }
)

retriever = WeaviateHybridSearchRetriever(
    client=client,
    index_name=os.getenv("collectionname"),
    text_key="chunks",
    attributes=[],
    create_schema_if_missing=True,
)

# Define the ingestion function
def ingest_vectors(row):

    try:
        docs = [
            Document(
                metadata={
                    "chunkid": str(row['chunkid']),
                    "speaker": row['speaker'],
                },
                page_content=row['chunks'],
            )]

        print(retriever.add_documents(docs))

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
    
    