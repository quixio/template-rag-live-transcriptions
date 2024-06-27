import os
import weaviate
import atexit
from langchain_weaviate.vectorstores import WeaviateVectorStore
from langchain_openai import OpenAIEmbeddings

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

collection = os.getenv("collection")
textkey = os.getenv("textkey")

try:
    # Initialize the Weaviate client
    client = weaviate.connect_to_wcs(
            cluster_url=os.environ["weaviate_rest_endpoint"],  # Replace with your WCS URL
            auth_credentials=weaviate.auth.AuthApiKey(os.environ["weaviate_api_key"]),  # Replace with your WCS key
            headers={'X-OpenAI-Api-key': os.getenv("OPENAI_API_KEY")}  # Replace with your OpenAI API key
        )
    print("Weaviate client initialized successfully.")
except Exception as e:
    print(f"Error initializing Weaviate client: {e}")
    raise

# Define a cleanup function to close the Weaviate client
def close_weaviate_client():
    try:
        client.close()
        print("Weaviate client closed successfully.")
    except Exception as e:
        print(f"Error closing Weaviate client: {e}")

# Register the cleanup function to be called on exit
atexit.register(close_weaviate_client)

def weaviate_retriever():
    try:
        # Initialize the embedding model
        embedding_model = OpenAIEmbeddings()
        
        # Initialize the vector store with the embedding model
        vs = WeaviateVectorStore(
            client=client,
            index_name=os.getenv("collection"),
            text_key=os.getenv("textkey"),
            embedding=embedding_model
        )
        print("WeaviateVectorStore initialized successfully.")
        
        # Create the retriever
        vs_retriever = vs.as_retriever()
        print("Retriever created successfully.")
        
        return vs_retriever
    except Exception as e:
        print(f"Error in weaviate_retriever: {e}")
        raise

