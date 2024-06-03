import os

from langchain_community.retrievers import BM25Retriever
from langchain.retrievers import EnsembleRetriever
from langchain_core.output_parsers import StrOutputParser
from langchain.vectorstores.weaviate import Weaviate

from basic_chain import get_model
from rag_chain import make_rag_chain
from remote_loader import load_web_page
from dotenv import load_dotenv
import weaviate

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

auth_config = weaviate.auth.AuthApiKey(api_key=os.getenv("weaviate_apikey"))
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

def ensemble_retriever_from_docs(docs, embeddings=None):
    # MC: Commenting out for now, don't need
    # texts = split_documents(docs)
    # vs = create_vector_db(texts, embeddings)

    vs = Weaviate(client, "TranscriptLCSpeechmatics", "chunks")
    vs_retriever = vs.as_retriever()

    # MC: Commenting out for now, don't need
    # bm25_retriever = BM25Retriever.from_texts([t.page_content for t in texts])

    # ensemble_retriever = EnsembleRetriever(
    #     retrievers=[bm25_retriever, vs_retriever],
    #     weights=[0.5, 0.5])

    return vs_retriever


def main():
    load_dotenv()

    problems_of_philosophy_by_russell = "https://www.gutenberg.org/ebooks/5827.html.images"
    docs = load_web_page(problems_of_philosophy_by_russell)
    ensemble_retriever = ensemble_retriever_from_docs(docs)
    model = get_model("ChatGPT")
    chain = make_rag_chain(model, ensemble_retriever) | StrOutputParser()

    result = chain.invoke("What are the key problems of philosophy according to Russell?")
    print(result)


if __name__ == "__main__":
    # this is to quiet the parallel tokenizers warning.
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    main()

