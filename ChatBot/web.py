import logging
import re
import gradio as gr
import json
import requests


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

url = "http://localhost:8088/v1/chat/completions"
headers = {"Content-Type": "application/json"}
stream = True

def send(message, history=None):
    data = {
        "messages": [{"role": "user", "content": message}],
        "stream": stream,
        "user": "123",
        "conversation": "123"
    }
    history = history + [["user", message], ["assistant", "正在生成回复..."]]
    yield history

    def format(input: str):
        output = input.strip()
        output = re.sub(r"<tool_call>", "**思考过程**:\n", output)
        output = re.sub(r"<tool_call>", "\n\n**最终回复**:\n", output)
        return output.strip()
    
    if stream:
        assistant_response = ""
        try:
            with requests.post(url, headers=headers, json=data, stream=True) as response:
                for line in response.iter_lines():
                    if line:
                        json_str =  line.decode('utf-8').strip("data: ")
                        if not json_str:
                            logger.warning("Received empty line in stream response")
                            continue
                        if json_str.startswith("{") and json_str.endswith("}"):
                            try:
                                response_data = json.loads(json_str)
                                if "delta" in response_data["choices"][0]:
                                    content = response_data["choices"][0]["delta"].get("content", "")
                                    formatted_content = format(content)
                                    logger.info(f"Received content chunk: {formatted_content}")
                                    assistant_response += formatted_content
                                    updated_history = history[:-1] + [["assistant", assistant_response]]
                                    yield updated_history
                                if response_data.get("choices", [{}])[0].get("finish") == "stop":
                                    logger.info("Stream response finished with stop signal")
                                    break
                                    
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to decode JSON: {e}")
                                yield history[:-1] + [["assistant", "抱歉，解析回复时发生错误。"]]
                                break
                        else:
                            logger.warning(f"Received non-JSON line in stream response: {json_str}")
                    else:
                        logger.warning("Received empty line in stream response")
                else:
                    logger.info("Stream response ended")
                    yield history[:-1] + [["assistant", "抱歉，回复未完成。"]]
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            yield history[:-1] + [["assistant", "抱歉，发送请求时发生错误。"]]
    else:
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            response_data = response.json()
            content = response_data["choices"][0]["message"]["content"]
            formatted_content = format(content)
            logger.info(f"Received full response: {formatted_content}")
            yield history[:-1] + [["assistant", formatted_content]]
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            yield history[:-1] + [["assistant", "抱歉，发送请求时发生错误。"]]
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
            yield history[:-1] + [["assistant", "抱歉，解析回复时发生错误。"]]

with gr.Blocks() as demo:
    chatbot = gr.Chatbot(label="ChatBot")
    with gr.Row():
        with gr.Column(scale=8):
            message = gr.Textbox(show_label=False, placeholder="请输入消息并按回车发送")
        with gr.Column(scale=2):
            send_button = gr.Button("发送")
    
    send_button.click(send, [message, chatbot], chatbot)
    message.submit(send, [message, chatbot], chatbot)
    send_button.click(lambda: "", None, message)
    message.submit(lambda: "", None, message)

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)