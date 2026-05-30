import os
import logging
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LLMInitializationError(Exception):
    """Custom exception for LLM initialization errors."""
    pass

def initialize_llm(llm_type):
    """Initialize the LLM based on the specified type."""
    try:
        # Here you would add the actual initialization code for the LLM
        llm = ChatOpenAI(
            api_key=os.environ["MODEL_SCOPE_KEY"],
            base_url="https://api-inference.modelscope.cn/v1",
            model="Qwen/Qwen3-235B-A22B-Instruct-2507",
            temperature=0.7,
            timeout=30,
            max_retries=2
        )
        logger.info(f"Successfully initialized LLM of type: {llm_type}")
        return llm
    except LLMInitializationError as e:
        logger.error(f"LLM initialization error: {e}")
        raise LLMInitializationError(f"Failed to initialize LLM: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during LLM initialization: {e}")
        raise LLMInitializationError(f"An unexpected error occurred during LLM initialization: {e}")
    
def get_llm(llm_type)->ChatOpenAI:
    """Get the initialized LLLM instance."""
    try:
        llm = initialize_llm(llm_type)
        return llm
    except LLMInitializationError as e:
        logger.error(f"Error getting LLM: {e}")
        raise LLMInitializationError(f"Failed to get LLM: {e}")
