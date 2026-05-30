from openai import OpenAI
from dotenv import load_dotenv
import os
load_dotenv()
client = OpenAI(
    api_key=os.environ["MODEL_SCOPE_KEY"],
    base_url="https://api-inference.modelscope.cn/v1"
)
response = client.chat.completions.create(
    model="Qwen/Qwen3-235B-A22B-Instruct-2507", # ModleScope Model-Id
    messages=[
        {
            'role': 'system',
            'content': 'You are a helpful assistant.'
        },
        {
            'role': 'user',
            'content': '用python写一下快排'
        }
    ],
    stream=True
)
for chunk in response:
    print(chunk.choices[0].delta.content, end='', flush=True)