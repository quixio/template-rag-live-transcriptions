# Ingest data into Weaviate using Weaviate embedding computation

This service ingests text into Weaviate while computing the embeddings on-the-fly and ingestion time using Weaviate's built-in embedding integration. The default is to use [OpenAI's embeddings API](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-openai) 

It uses an older version of the Weaviate Python client, V3, because LangChain has only very recently been updated to support V4. Most of the LangChain + Weaviate tutorials and examples in the wild still seem to use V3 code.

The LangChain-based RAG UI seems to expect data to be stored according to a specific schema, and the easiest way to meet this requirement is to use the LangChain-Weaviate integration to ingest the data too (rather than using the pure Weaviate client)


## Environment variables

The code sample uses the following environment variables:

- **weaviate_rest_endpoint**: The URL to the Weaviate cluster or instance.
- **weaviate_apikey**: API key for the Weaviate instance.
- **groupname**: The name of the current consumer group
- **collectionname**: The name of the collection in Weaviate where the data should be written to.
