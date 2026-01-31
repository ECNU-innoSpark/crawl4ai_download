import re
import os
import sys
import json
import copy
import random
import asyncio
import socket
import traceback
from urllib.parse import urlparse
from tqdm import tqdm
from collections import Counter
from transformers import AutoTokenizer

from prompts import *
from toolkit.tool_search import Search
from toolkit.mcp_client import mcp_client
from toolkit.browser import Visit, Click, Fill
from utils import read_jsonl, count_tokens, call_llm


async def call_tool(sem, tool_name: str, tool_args: dict, client, lock):
    global tokenizer
    async with sem['tool']:
        if tool_name == "search":
            return await search.call(tool_args)
        elif tool_name == "visit":
            return await visit.call(tool_args, client=client, lock=lock, tokenizer=tokenizer, sem=sem)
        elif tool_name == "click":
            return await click.call(tool_args, client=client, lock=lock, tokenizer=tokenizer, sem=sem)
        elif tool_name == "fill":
            return await fill.call(tool_args, client=client, lock=lock)
        else:
            await asyncio.sleep(1)
            return f'Tool {tool_name} does not exist.'


def _task_prefix(task_id, question):
    """用于进度日志的短前缀"""
    short_q = (question[:45] + "…") if len(question) > 45 else question
    short_q = short_q.replace("\n", " ")
    return f"[Task-{task_id}]"


async def agentic_loop(sem, data, messages, task_id=0):
    global tokenizer
    question = data['question']
    answer = data['answer']
    prefix = _task_prefix(task_id, question)

    record = copy.deepcopy(messages)
    summary_record = []

    termination = 'max_turn_exceeded'
    prediction = '[No Prediction]'

    print(f"{prefix} 等待 session 槽位…", flush=True)
    async with sem['session']:
        print(f"{prefix} 正在连接 MCP…", flush=True)
        try:
            async with mcp_client(server_url=BROWSER_SERVER_URL) as (client, lock):
                print(f"{prefix} MCP 已连接，开始推理。", flush=True)
                for turn in range(MAX_AGENT_TURN):
                    if count_tokens(record, tokenizer) > MAX_AGENT_LEN:
                        termination = 'max_length_exceeded'
                        break

                    print(f"{prefix} [Turn {turn+1}] 调用 LLM…", flush=True)
                    response = await call_llm(sem, record, int(os.getenv("MAX_SINGLE_GEN_TOKENS")), os.getenv("MODEL_NAME"))
                    print(f"{prefix} [Turn {turn+1}] LLM 返回 ({len(response) if response else 0} 字符)", flush=True)

                    if not response:
                        return {'question': question, 'answer': answer, 'prediction': prediction, 'messages': record, 'summary_record': summary_record, 'termination': 'llm_response_error'}

                    record.append({"role": "assistant", "content": response})

                    if "<tool_call>" in response and "</tool_call>" in response:
                        cur_summary_record = None
                        tool_call = response.split('<tool_call>')[-1].split('</tool_call>')[0].strip()

                        try:
                            tool_call = json.loads(tool_call)
                            tool_name = tool_call['name']
                            tool_args = tool_call['arguments']
                            if isinstance(tool_args, str):
                                tool_args = json.loads(tool_args)

                            print(f"{prefix} [Turn {turn+1}] 调用工具: {tool_name}", flush=True)
                            result = await call_tool(sem, tool_name, tool_args, client, lock)
                            print(f"{prefix} [Turn {turn+1}] 工具 {tool_name} 返回完成", flush=True)

                            if isinstance(result, tuple):
                                observation, cur_summary_record = result
                            elif isinstance(result, str):
                                observation = result
                            else:
                                raise Exception(f"Invalid tool result format: {result}")

                            if cur_summary_record:
                                summary_record.extend(cur_summary_record)
                        except Exception as e:
                            observation = f'Error: {e}'
                            print(f"{prefix} Tool error: {e}")

                        tool_response = f"<tool_response>\n{observation}\n</tool_response>"
                        if "server-side error" in observation:
                            return {'question': question, 'answer': answer, 'prediction': prediction, 'messages': record, 'summary_record': summary_record, 'termination': 'server_side_error'}
                        record.append({"role": "user", "content": tool_response, "tool_name": tool_name, "tool_args": tool_args, "function_result": observation})
                    else:
                        if "<answer>" in response and "</answer>" in response:
                            prediction = response.split('<answer>')[-1].split('</answer>')[0].strip()
                            termination = 'answer'
                            print(f"{prefix} 已给出答案。", flush=True)
                        else:
                            termination = 'llm_response_error'
                            print(f"{prefix} 未检测到 <answer>。", flush=True)
                        break
        except Exception as e:
            print(f"{prefix} MCP 连接失败: {e}")
            return {'question': question, 'answer': answer, 'prediction': prediction, 'messages': record, 'summary_record': summary_record, 'termination': 'mcp_error'}

    print(f"{prefix} 任务结束 (termination={termination})", flush=True)
    return {'question': question, 'answer': answer, 'prediction': prediction, 'messages': record, 'summary_record': summary_record, 'termination': termination}
    

async def main(sem, rollout_count, input_path, output_path):
    global tokenizer
    dataset = read_jsonl(input_path)
    
    visited_counter = Counter()
    if os.path.exists(output_path):
        existing_rollouts = read_jsonl(output_path)
        for visited_data in existing_rollouts:
            question = visited_data['question']
            visited_counter[question] += 1

    # submit task
    tasks = []
    pending_counter = Counter()
    task_id = 0
    for data in dataset:
        question = data.get('question')
        total_count = visited_counter[question] + pending_counter[question]
        need_to_submit = rollout_count - total_count if rollout_count - total_count > 0 else 0
        for _ in range(need_to_submit):
            task_id += 1
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT_OURS},
                {"role": "user", "content": question}
            ]
            tasks.append(agentic_loop(sem, data, messages, task_id=task_id))
            pending_counter[question] += 1

    print(f"Total number of tasks: {len(tasks)}")
    print("进度说明: 等待 session → 连接 MCP → LLM 调用 → 工具调用；卡住时看最后一行输出。", flush=True)

    # process task
    done_count = [0]
    with open(output_path, "a", encoding="utf-8") as f:
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Rollout"):
            try:
                result = await future
                done_count[0] += 1
                f.write(json.dumps(result, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
                print(f"[完成] {done_count[0]}/{len(tasks)} 条已写入 {output_path}", flush=True)
            except Exception as e:
                exception_type = type(e).__name__
                exception_message = str(e)
                traceback_info = ''.join(traceback.format_tb(e.__traceback__))
                error_message = f'{exception_type}: {exception_message}\n' \
                                f'Traceback:\n{traceback_info}'
                print(f"[ERROR]: {error_message}")


if __name__ == '__main__':
    BROWSER_SERVER_URL = "http://localhost:8080/sse"
    
    AGENT_LLM_BASE_URL = "http://49.51.37.239:3006/v1"  # locally hosted nestbrowse model
    AGENT_LLM_API_KEY = "sk-eEHuvDfMPJf3mKQOmdDVHDq30RsA9RXKd4LhUtGxNgiXYtPq"

    # Tokenizer 用于上下文长度统计与内循环分片；可与 Agent 模型不同，但一致时更准
    tokenizer_path = os.getenv("TOKENIZER_PATH", "Qwen/Qwen2-0.5B-Instruct")
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)

    # ========================================
    rollout_count = 1
    MAX_AGENT_TURN = 100
    MAX_AGENT_LEN = 128 * 1024
    MAX_SINGLE_GEN_TOKENS = 16 * 1024  # API 限制 16384
    MAX_SUMMARY_SHARD_LEN = 64 * 1024
    benchmark_name = "hard"  # 需要 visit/click 的任务
    MODEL_NAME = "gpt-4o"
    MAX_WORKERS = 1
    sem = {
        'session': asyncio.Semaphore(MAX_WORKERS),
        'llm': asyncio.Semaphore(MAX_WORKERS),
        'tool': asyncio.Semaphore(MAX_WORKERS),
    }
    # ========================================


    os.environ["AGENT_LLM_BASE_URL"] = AGENT_LLM_BASE_URL
    os.environ["AGENT_LLM_API_KEY"] = AGENT_LLM_API_KEY
    os.environ["MAX_SINGLE_GEN_TOKENS"] = str(MAX_SINGLE_GEN_TOKENS)
    os.environ["MAX_SUMMARY_SHARD_LEN"] = str(MAX_SUMMARY_SHARD_LEN)
    os.environ["MODEL_NAME"] = MODEL_NAME


    input_path = f"./data/{benchmark_name}.jsonl"
    output_path = f"./results/{MODEL_NAME}_results_{benchmark_name}.jsonl"

    search = Search()
    visit = Visit()
    click = Click()
    fill = Fill()
    
    TOOLS_SCHEMA = [search.tool_schema, visit.tool_schema, click.tool_schema, fill.tool_schema]

    # 启动前检查 MCP 浏览器服务是否可达，避免卡在连接
    try:
        parsed = urlparse(BROWSER_SERVER_URL)
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((host, port))
        sock.close()
    except (socket.error, OSError, ValueError) as e:
        print(f"[启动检查] MCP 浏览器服务不可达: {BROWSER_SERVER_URL}")
        print(f"  错误: {e}")
        print("  请先在另一终端启动: python mcp_browser_server.py --port 8080")
        sys.exit(1)

    asyncio.run(main(sem, rollout_count, input_path, output_path))