import os
import ast
import json
import random
import aiohttp
from openai import AsyncOpenAI
from typing import List, Union, Dict, Any


def _normalize_messages_for_api(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    将 messages 规范为 OpenAI API 支持的多模态格式，同时兼容现有纯文本格式。

    - 若某条消息的 content 为 str，保持不变（纯文本）。
    - 若某条消息的 content 为 list，则应为 [{"type": "text", "text": "..."}, {"type": "image_url", "image_url": {"url": "..."}}] 等形式，直接透传。
    """
    normalized = []
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content")
        if content is None:
            normalized.append(msg)
            continue
        if isinstance(content, str):
            normalized.append({"role": role, "content": content})
        elif isinstance(content, list):
            parts = []
            for part in content:
                if isinstance(part, dict):
                    if part.get("type") == "text" and "text" in part:
                        parts.append({"type": "text", "text": part["text"]})
                    elif part.get("type") == "image_url" and "image_url" in part:
                        url_spec = part["image_url"]
                        url = url_spec.get("url") if isinstance(url_spec, dict) else url_spec
                        if url:
                            parts.append({"type": "image_url", "image_url": {"url": url}})
                else:
                    continue
            normalized.append({"role": role, "content": parts})
        else:
            normalized.append(msg)
    return normalized


async def call_llm(
    sem,
    prompt: List[Dict[str, Any]],
    max_tokens: Union[int, float],
    model_name: str,
    client=None,
    mode: str = "agent",
) -> Union[str, None]:
    """
    调用 LLM API，支持纯文本与多模态输入。

    Args:
        sem: 并发控制信号量（需包含 'llm' 键）。
        prompt: OpenAI 格式的消息列表。每条消息为 {"role": "user"|"assistant"|"system", "content": ...}。
               content 可为：
               - str：纯文本，与现有行为兼容；
               - list：多模态内容，如 [{"type": "text", "text": "..."}, {"type": "image_url", "image_url": {"url": "data:image/...;base64,..."}}]。
        max_tokens: 最大生成 token 数。
        model_name: 模型名称（当前实现中由环境变量等决定实际模型）。
        client: 可选，未使用时内部会创建 AsyncOpenAI 客户端。
        mode: "agent" 或 "summary"。

    Returns:
        模型回复的文本内容；失败时返回 None。
    """
    if mode == "agent":
        LLM_API_KEY = os.getenv("AGENT_LLM_API_KEY")
        LLM_BASE_URL = os.getenv("AGENT_LLM_BASE_URL")
    elif mode == "summary":
        LLM_API_KEY = os.getenv("SUMMARY_LLM_API_KEY", os.getenv("AGENT_LLM_API_KEY"))
        LLM_BASE_URL = os.getenv("SUMMARY_LLM_BASE_URL", os.getenv("AGENT_LLM_BASE_URL"))
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    try:
        LLM_BASE_URL = random.choice(ast.literal_eval(LLM_BASE_URL))
    except Exception:
        pass

    assert isinstance(prompt, list), "For nest_browse, prompt must be a list of messages"
    messages = _normalize_messages_for_api(prompt)

    async with sem["llm"]:
        for retry in range(10):
            max_tokens = int(max_tokens)
            try:
                api_client = AsyncOpenAI(
                    api_key=LLM_API_KEY,
                    base_url=LLM_BASE_URL,
                )
                if mode == "agent":
                    response = await api_client.chat.completions.create(
                        model=model_name,
                        messages=messages,
                        stop=["\n<tool_response>", "<tool_response>"],
                        temperature=0.6,
                        top_p=0.95,
                        presence_penalty=1.1,
                        max_tokens=max_tokens,
                    )
                else:
                    response = await api_client.chat.completions.create(
                        model=model_name,
                        messages=messages,
                        temperature=0.6,
                        top_p=0.95,
                        max_tokens=max_tokens,
                    )
                result_text = response.choices[0].message.content
                return result_text

            except Exception as e:
                print(f"[CALL LLM async error] {e}")
                if "time out" not in str(e).lower():
                    max_tokens = max_tokens / 2

    return None


def read_jsonl(file_path):
    result = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    result.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return result


def count_tokens(text, tokenizer):
    if isinstance(text, str):
        return len(tokenizer.encode(text))

    tokens = tokenizer.apply_chat_template(text, tokenize=True)
    return len(tokens)