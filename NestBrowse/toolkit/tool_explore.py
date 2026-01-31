import os
import json5
from utils import count_tokens, call_llm
from prompts import *


def _build_user_content_multimodal(text: str, screenshot_base64: str) -> list:
    """ViNest: 构建多模态 user content：先文本，再图片（OpenAI 格式）。"""
    parts = [{"type": "text", "text": text}]
    if screenshot_base64:
        url = f"data:image/png;base64,{screenshot_base64}"
        parts.append({"type": "image_url", "image_url": {"url": url}})
    return parts


async def process_response(raw_response, goal, summary_model, tokenizer, sem, screenshot=None):
    limit = int(os.getenv("MAX_SUMMARY_SHARD_LEN"))
    record = []
    raw_response_shard = []

    if count_tokens(raw_response, tokenizer) > limit:
        tokens = tokenizer.encode(raw_response)
        for i in range(0, len(tokens), limit):
            chunk_tokens = tokens[i:i+limit]
            chunk_text = tokenizer.decode(chunk_tokens)
            raw_response_shard.append(chunk_text)
    else:
        raw_response_shard.append(raw_response)

    # ViNest: 仅第一个 shard 附带截图，避免重复发送大图
    use_screenshot = screenshot if raw_response_shard else None

    for i, raw_resp in enumerate(raw_response_shard):
        if i == 0:
            user_text = SUMMARY_PROMPT.format(raw_response=raw_resp, goal=goal)
        else:
            user_text = SUMMARY_PROMPT_INCREMENTAL.format(
                raw_response=raw_resp, goal=goal,
                existing_evidence=evidence, existing_summary=summary
            )

        if use_screenshot and i == 0:
            user_content = _build_user_content_multimodal(user_text, use_screenshot)
        else:
            user_content = user_text

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT_SUMMARY_OURS},
            {"role": "user", "content": user_content}
        ]

        response = await call_llm(sem, messages, int(os.getenv("MAX_SINGLE_GEN_TOKENS")), summary_model, mode="summary")
        messages.append({"role": "assistant", "content": response})

        record.append({"messages": messages})

        processed_response_json = response.split("</think>")[-1].split('<useful_info>')[-1].split('</useful_info>')[0].strip()
        processed_response_json = json5.loads(processed_response_json)

        evidence = processed_response_json["evidence"]
        summary = processed_response_json["summary"]

    processed_response = "Evidence in page: \n" + str(evidence) + "\n\n" + "Summary: \n" + str(summary)
    processed_response = processed_response.strip()

    # ViNest Step 4: 自适应目标修正 — 若有 refinement 则附加显眼建议
    refinement = processed_response_json.get("refinement")
    if refinement and isinstance(refinement, str) and refinement.strip():
        processed_response = processed_response + "\n\n[URGENT ADVICE]: " + refinement.strip()

    return processed_response, record