## Code adpated from https://github.com/streamlit/example-app-langchain-rag by Richard Anton
import streamlit as st
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
import logging

logging.basicConfig(level=logging.INFO)

# from ensemble import ensemble_retriever_from_docs
from retriever_weaviate import weaviate_retriever
from full_chain import create_full_chain, ask_question

st.set_page_config(page_title="Weaviate & LangChain RAG")
st.title("Weaviate & LangChain RAG")


def show_ui(qa, prompt_to_user="How may I help you?"):
    if "messages" not in st.session_state.keys():
        st.session_state.messages = [{"role": "assistant", "content": prompt_to_user}]

    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])

    # User-provided prompt
    if prompt := st.chat_input():
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

    # Generate a new response if last message is not from assistant
    if st.session_state.messages[-1]["role"] != "assistant":
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = ask_question(qa, prompt)
                st.markdown(response.content)
        message = {"role": "assistant", "content": response.content}
        st.session_state.messages.append(message)

def get_chain(openai_api_key=None):
    chain = create_full_chain(weaviate_retriever(),
                              openai_api_key=openai_api_key,
                              chat_memory=StreamlitChatMessageHistory(key="langchain_messages"))
    return chain


def get_secret_or_input(secret_key, secret_name, info_link=None):
    if secret_key in st.secrets:
        logging.info("Found %s secret" % secret_key)
        secret_value = st.secrets[secret_key]
    else:
        st.write(f"Please provide your {secret_name}")
        secret_value = st.text_input(secret_name, key=f"input_{secret_key}", type="password")
        if secret_value:
            st.session_state[secret_key] = secret_value
        if info_link:
            st.markdown(f"[Get an {secret_name}]({info_link})")
    return secret_value


def run():
    ready = True

    openai_api_key = st.session_state.get("OPENAI_API_KEY")
    huggingfacehub_api_token = st.session_state.get("HUGGINGFACEHUB_API_TOKEN")


    if not openai_api_key:
        openai_api_key = get_secret_or_input('OPENAI_API_KEY', "OpenAI API key",
                                                info_link="https://platform.openai.com/account/api-keys")
    if not huggingfacehub_api_token:
        huggingfacehub_api_token = get_secret_or_input('HUGGINGFACEHUB_API_TOKEN', "HuggingFace Hub API Token",
                                                        info_link="https://huggingface.co/docs/huggingface_hub/main/en/quick-start#authentication")

    if not openai_api_key:
        logging.warning("Missing OPENAI_API_KEY")
        ready = False
    if not huggingfacehub_api_token:
        logging.warning("Missing HUGGINGFACEHUB_API_TOKEN")
        ready = False

    if ready:
        chain = get_chain(openai_api_key=openai_api_key)
        st.subheader("Ask me questions about the conversation that is taking place.")
        # st.subheader("Ask me questions about some products.")
        show_ui(chain, "What would you like to know?")
    else:
        st.stop()


run()
