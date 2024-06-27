# Ingest data into Weaviate using custom embeddings

This service ingests pre-computed embeddings into Weaviate (as opposed to letting Weaviate handle embedding computation). Note that this is not currently used for the RAG demo, rather it is simply there to illustrate the option of decoupling of embedding computation and vector ingestion.

It uses the latest V4 version of the Weaviate Python client.

Weaviate lets you ingest your own vectors that you have computed elsewhere (as described in [this article](https://weaviate.io/developers/weaviate/starter-guides/custom-vectors)). However, the Weaviate client has to be configured to expect this, with the setting  `vectorizer_config=wvc.config.Configure.Vectorizer.none()`

The resulting collection is not currently used for RAG because of unexpected errors that occurred when attempting to use a LangChain retriever on a collection where the embeddings were computed outside of Weaviate. 

## Environment variables

The code sample uses the following environment variables:

- **weaviate_rest_endpoint**: The URL to the Weaviate cluster or instance.
- **weaviate_apikey**: API key for the Weaviate instance.
- **groupname**: The name of the current consumer group
- **collectionname**: The name of the collection in Weaviate where the vectors are stored.
- **input**: The topic that stores the embeddings.