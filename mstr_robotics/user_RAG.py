import json
from openai import OpenAI
from flashtext import KeywordProcessor
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings

class keyword_processor():

    def __init__(self):
        self.KeywordProcessor = KeywordProcessor()

    def load_keyword_processor(self,key_l):
        for val in key_l:
            self.KeywordProcessor.add_keyword(val)
        return self.KeywordProcessor

    def extract_keywords(self,msg_t):
        keywords_l = self.KeywordProcessor.extract_keywords(msg_t)
        return keywords_l

    def check_keyword(self,filt_obj_str):
        keywords_l = self.KeywordProcessor.extract_keywords(filt_obj_str)
        return keywords_l[0]

class vectorDB_faisst():

    def __init__(self,sKey):
        self.i_embeddings = OpenAIEmbeddings(openai_api_key=sKey)

    def load_vector_store(self,key_l):
        # Flatten and ensure all items are strings
        # Create FAISS vector store
        self.vector_store = FAISS.from_texts(key_l, self.i_embeddings)

        return self.vector_store

    def check_keyword(self, filt_obj_str):
        results = self.vector_store.similarity_search_with_score(query=filt_obj_str, top_k=1)
        filter_obj_name = results[0][0].page_content
        return filter_obj_name

class mstr_openAI():

    def __init__(self,sKey):
        #self.sKey=sKey
        self.i_embeddings = OpenAIEmbeddings(openai_api_key=sKey)


    def check_trans_chatGPT(self,sKey,messages,*args,**kwargs):
        chat_completion = self.call_open_AI(sKey=sKey,messages=messages,*args,**kwargs)
        js = json.loads(chat_completion.json())
        return js

    def call_open_AI(self, sKey, messages, max_tokens=1000, temperature=0.3, model="gpt-4o-mini"):
        # This is the default and can be omitted
        client = OpenAI(api_key=sKey)

        chat_completion = client.chat.completions.create(
            messages=messages,
            # model="gpt-3.5-turbo",
            model=model,
            max_tokens=max_tokens,
            temperature=temperature
        )
        js = json.loads(chat_completion.json())
        return js

class chat_bot_RAG():

    def filter_RAG_l(self,msg_t, key_word_l, msg_filter_t):
        messages = [
            {
                "role": "system",
                "content": f"You will be provided unstructured filter definition of the BI report request. the full message is {msg_t}"
            },
            {
                "role": "system",
                "content": "Always use as Filteroperator expressions like In, GreaterEqual,Equals,IsNull,IsNotNull,Greater,Between,BeginsWith,Like"
            },
            {
                "role": "system",
                "content": "When filtering a list, please ensure, that you use In od NotIn as operator"
            },
            {
                "role": "system",
                "content": "Ensure, that the out pu is a dictionary consiting of keywords"
            },
            {"role": "system",
             "content": "top_n or bottom_n is part of the filter statement"
             },
            {
                "role": "system",
                "content": f"keywrods are: {key_word_l} "
            },
            {
                "role": "system",
                "content": " Please provide the filters as a valid Python dictionary without any code block formatting or syntax highlighting, where the kefilter objects"
            },
            {
                "role": "system",
                "content": "Set brackets to split multiple filter criteria"
            },
            {
                "role": "system",
                "content": "Ignore keywords, if they are not relevant for filtering"
            },
            {
                "role": "system",
                "content": f"keywrods are: {key_word_l} "
            },
            {
                "role": "user",
                "content": f"{msg_filter_t}"
            }
        ]
        return messages

    def split_AI_msg(self,msg_t, key_word_l):
        messages = [
            {
                "role": "system",
                "content": "You will be provided unstructured BI report request."
            },
            {
                "role": "system",
                "content": " Please provide the content as a valid Python dictionary without any code block formatting or syntax highlighting, where the keywords are splited in template and filter objects"
            },
            {
                "role": "system",
                "content": "Please use the key template and filter split"
            },
            {
                "role": "system",
                "content": "Set brackets to split multiple filter criteria"
            },
            {
                "role": "system",
                "content": "Please include in the JSON file, the filter critria as string"
            },
            {
                "role": "system",
                "content": f"keywrods are: {key_word_l} "
            },
            {
                "role": "user",
                "content": f"{msg_t}"
            }
        ]
        return messages