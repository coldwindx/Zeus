from contextlib import asynccontextmanager
import json
import logging
from time import time
from typing import Annotated, Optional, TypedDict
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from gradio import on
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph, RunnableConfig
from langgraph.graph.message import add_messages
from langgraph.store.memory import BaseStore, InMemoryStore
from pydantic import BaseModel, Field
from langgraph.checkpoint.memory import MemorySaver
import re
import pymysql

import uvicorn
from psycopg_pool import ConnectionPool
from langgraph.checkpoint.mysql.aio import AIOMySQLSaver
from langgraph.checkpoint.mysql.pymysql import PyMySQLSaver
from langgraph.store.mysql.aio import AIOMySQLStore
from langgraph.store.mysql.pymysql import PyMySQLStore
from llms import get_llm



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the Message for API communication
class Message(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    messages: list[Message]
    stream: Optional[bool] = False
    user: Optional[str] = None
    conversation: Optional[str] = None

class ChatCompletionResponseChoice(BaseModel):
    index: int
    message: Message
    finish: Optional[str] = None

class ChatCompletionResponse(BaseModel):
    id: str = Field(default_factory=lambda: f"chatcmpl-{uuid.uuid4().hex}")
    object: str = "chat.completion"
    created: int = Field(default_factory=lambda: int(time()))
    choices: list[ChatCompletionResponseChoice]
    fingerprint: Optional[str] = None

class State(TypedDict):
    messages: Annotated[list, add_messages]

def create(llm, checkpointer, store)->StateGraph:
    try:

        builder = StateGraph(State)

        def chatbot(state: State, config: RunnableConfig, *, store: BaseStore)->dict:
            # Long term memory retrieval
            namespace = ("memories", config["configurable"]["user"])
            memories = store.search(namespace, query=str(state["messages"][-1].content))
            info = "\n".join([d.value["data"] for d in memories])

            last_message = state["messages"][-1].content
            if "记住" in last_message:
                store.put(namespace, str(uuid.uuid4()), {"data": last_message})
            
            # Short term memory retrieval
            messages = state["messages"][-min(3, len(state["messages"])):]

            # Here you would add the actual code to invoke the LLM and get the response
            response = llm.invoke([{"role": "system", "content": f"You are a helpful assistant. Here is some relevant information from your memories:\n{info}"}] + messages)
            return {"messages": [response]}

        builder.add_node("chatbot", chatbot)
        builder.add_edge(START, "chatbot")
        builder.add_edge("chatbot", END)

        return builder.compile(checkpointer=checkpointer, store=store)
    except Exception as e:
        logger.error(f"Error creating graph: {e}")
        raise RuntimeError(f"Failed to create graph: {e}")
    
def visualization(graph: StateGraph, filename="graph.png"):
    try:
        with open(filename, "wb") as f:
            f.write(graph.get_graph().draw_mermaid_png())
        logger.info(f"Graph visualization saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving graph visualization: {e}")

def format(response):
    paragraphs = re.split(r'\n{2,}', response.strip())

    def _format(paragraph):
        # If the paragraph contains code blocks, split it into text and code parts
        if "```" in paragraph:
            parts = paragraph.split("```")
            for i, part in enumerate(parts):
                if i % 2 == 1: # This is a code block
                    parts[i] = f"\n```\n{part.strip()}\n```\n"
            return "".join(parts)
        else:
            return paragraph.replace(". ", ".\n")
    paragraphs = [_format(p).strip() for p in paragraphs]
    return "\n\n".join(paragraphs)

### API endpoint
graph: Optional[CompiledStateGraph] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global graph
    try:
        logger.info("Initializing LLM...")
        llm, embedding = get_llm("modelscope")

        # Add a real database connection here and replace the InMemoryStore with a database-backed store
        connection = pymysql.connect(host='172.16.1.223', port=3306, user='root', password='mysql123', database='db_zeus', autocommit=True)
        checkpointer = PyMySQLSaver(connection)
        checkpointer.setup()

        connection2 = pymysql.connect(host='172.16.1.223', port=3306, user='root', password='mysql123', database='db_zeus', autocommit=True)
        store = PyMySQLStore(connection2)
        store.setup()

        graph = create(llm, checkpointer, store)
        visualization(graph)
        logger.info("LLM and graph initialized successfully.")
    except Exception as e:
        logger.error(f"Error during application startup: {e}")
        raise RuntimeError(f"Failed to initialize application: {e}")
    yield
    logger.info("Application shutdown.")
    connection.close()
    connection2.close()

app = FastAPI(lifespan=lifespan)
@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest) -> ChatCompletionResponse:
    if not graph:
        logger.error("Graph is not initialized.")
        raise HTTPException(status_code=500, detail="Graph is not initialized.")
    try:
        logger.info(f"Received chat completion request: {request}")
        
        query = request.messages[-1].content
        logger.info(f"Q: {query}")

        connfig = {"configurable": {"thread_id": request.user + "@" + request.conversation, "user": request.user}}
        logger.info(f"Conversation: {connfig}")

        prompt = [
            # Add a system message to set the context for the assistant
            {"role": "system", "content": "You are a helpful assistant."},

            # Add the user messages to the prompt
            {"role": "user", "content": query}
        ]

        if request.stream:
            async def stream_response():
                chunk_id = f"chatcmpl-{uuid.uuid4().hex}"
                
                async for message_chunk in graph.astream({"messages": prompt}, connfig, stream_model="message"):
                    chunk = message_chunk["chatbot"]["messages"][-1].content
                    logger.info(f"Chunk: {chunk}")
                    yield f"data: {json.dumps({'id': chunk_id,'object': 'chat.completion.chunk','created': int(time()),'choices': [{'index': 0,'delta': {'content': chunk},'finish': None}]})}\n\n"
                yield f"data: {json.dumps({'id': chunk_id,'object': 'chat.completion.chunk','created': int(time()),'choices': [{'index': 0,'delta': {},'finish': 'stop'}]})}\n\n"
            return StreamingResponse(stream_response(), media_type="text/event-stream")
        else:
            try:
                events = graph.stream({"messages": prompt}, connfig)

                for event in events:
                    for value in event.values():
                        result = value['messages'][-1].content
            except Exception as e:
                logger.error(f"Error processing response: {e}")
            
            formatted_response = str(format(result))
            logger.info(f"A: {formatted_response}")

            response = ChatCompletionResponse(
                choices=[ChatCompletionResponseChoice(
                    index=0, 
                    message=Message(role="assistant", content=formatted_response),
                    finish="stop"
                    )])
            logger.info(f"Response: {response}")
            return JSONResponse(content=response.model_dump())
    except Exception as e:
        logger.error(f"Error handling chat completion request: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {e}")

if __name__ == "__main__":
    logger.info("Starting FastAPI application...")
    uvicorn.run(app, host="0.0.0.0", port=8088)