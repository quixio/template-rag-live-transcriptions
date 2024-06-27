import os

from langchain.memory import ChatMessageHistory
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from memory import create_memory_chain
from rag_chain import make_rag_chain

def create_full_chain(retriever, openai_api_key=None, chat_memory=ChatMessageHistory()):
    model = ChatOpenAI(openai_api_key=openai_api_key)
    system_prompt = """You are a helpful AI assistant that is designed to help a users recount what a speaker said during a live talk.
    Use the following excerpts from the speaker's live transcript and the users' chat history to help the user:
    Bear in mind that there are words that might be garbled or mistranscribed, so do your best to extract the original sense of the text.
    For example, the company name "Quix" is often mistranscribed as "quicks" or "clicks" so if you see that word, assume it is the company name Quix".
    If you can't find the answer in the transcription, just say there's nothing in the transcription that can help you answer the question.
    
    Context: {context}
    
    Question: """

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            ("human", "{question}"),
        ]
    )

    rag_chain = make_rag_chain(model, retriever, rag_prompt=prompt)
    chain = create_memory_chain(model, rag_chain, chat_memory)
    return chain


def ask_question(chain, query):
    response = chain.invoke(
        {"question": query},
        config={"configurable": {"session_id": "foo"}}
    )
    return response
