
import asyncio
import httpx
from typing import List, Dict
from nonebot import get_driver, logger
from .config import Config

# 加载配置
plugin_config = Config.parse_obj(get_driver().config.dict())
GEMINI_KEY = plugin_config.gemini_key
OPENAI_BASE_URL = plugin_config.openai_base_url
OPENAI_API_KEY = plugin_config.openai_api_key
SUMMARY_MODEL = plugin_config.summary_model
PROXY = plugin_config.proxy
TIME_OUT = plugin_config.time_out

async def request_gemini(messages: List[Dict[str, str]]) -> str:
    """请求 Gemini API"""
    if not GEMINI_KEY:
        return "Gemini API Key 未配置"

    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{SUMMARY_MODEL}:generateContent?key={GEMINI_KEY}" 
    
    headers = {"Content-Type": "application/json"}
    data = {"contents": messages}

    try:
        async with httpx.AsyncClient(proxy=PROXY) as client:
            resp = await client.post(api_url, json=data, headers=headers, timeout=TIME_OUT)
            resp.raise_for_status()
            result = resp.json()
            return result["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        logger.error(f"请求 Gemini API 失败: {e}")
        return f"请求 Gemini API 失败: {e}"

async def request_openai(messages: List[Dict[str, str]]) -> str:
    """请求 OpenAI API"""
    if not OPENAI_API_KEY or not OPENAI_BASE_URL:
        return "OpenAI API Key 或 Base URL 未配置"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}",
    }
    data = {"model": SUMMARY_MODEL, "messages": messages}

    try:
        async with httpx.AsyncClient(proxy=PROXY) as client:
            resp = await client.post(OPENAI_BASE_URL, json=data, headers=headers, timeout=TIME_OUT)
            resp.raise_for_status()
            result = resp.json()
            return result["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"请求 OpenAI API 失败: {e}")
        return f"请求 OpenAI API 失败: {e}"

async def get_ai_summary(danmakus: List[str], comments: List[str]) -> str:
    """
    获取弹幕和评论的 AI 总结
    """
    if not danmakus and not comments:
        return "没有足够的弹幕和评论来进行AI总结。"

    # 将弹幕和评论合并为一个文本块
    # 为了节省 token，我们不需要把所有内容都发过去，抽样一部分即可
    max_items = 500 # 最多处理500条
    content_list = danmakus[:max_items//2] + comments[:max_items//2]
    
    if len(content_list) < 10:
         return "弹幕和评论数量太少，不足以进行AI总结。"

    content_text = "\n".join(content_list)

    prompt = (
        "你是一个B站视频观众，你需要根据以下弹幕和评论内容，用中文对视频的核心看点、槽点和整体风评进行总结。"
        "请以客观、简洁的语言进行描述，不要出现“我”这种第一人称代词。总结内容需要分点、条理清晰。"
        "内容应该包括：\n1. 视频的核心内容和亮点是什么？\n2. 观众的主要讨论焦点或槽点是什么？\n3. 综合来看，观众对这个视频的整体评价是怎样的？"
        "\n--- 弹幕和评论如下 ---\n"
        f"{content_text}"
    )

    messages = [{"role": "user", "parts": [{"text": prompt}]}] if "gemini" in SUMMARY_MODEL else [{"role": "user", "content": prompt}]

    # 根据模型名称选择API
    if "gemini" in SUMMARY_MODEL:
        summary = await request_gemini(messages)
    else:
        summary = await request_openai(messages)
        
    return summary


async def generate_ai_analysis(summary: str, danmakus: List[str], comments: List[str]) -> str:
    """
    根据初步总结、弹幕和评论，进行更深层次的AI分析
    """
    if not summary or "失败" in summary:
        return "AI初步总结不存在或生成失败，无法进行深度分析。"

    max_items = 300
    content_list = danmakus[:max_items//2] + comments[:max_items//2]
    content_text = "\n".join(content_list)

    prompt = (
        "你是一位专业的B站视频评论分析师。你的任务是基于一份已有的AI初步总结，并结合视频的弹幕和评论，进行更深入、更全面的分析。"
        "请遵循以下步骤和要求：\n"
        "1. **情感倾向分析**：综合判断观众的情感是正面的、负面的，还是混合的？并简要说明理由。\n"
        "2. **核心观点提炼**：找出观众讨论最激烈或最普遍的几个核心观点是什么？这些观点是支持视频内容，还是在吐槽？\n"
        "3. **争议点或潜在问题**：视频是否存在争议点？或者，观众是否指出了视频中可能存在的问题或不足？\n"
        "4. **粉丝画像侧写**：根据弹幕和评论的风格，简单描述该视频观众群体的可能特征（例如：是硬核粉丝、路人观众，还是专业人士？）。\n"
        "请用中文、分点、条理清晰地输出你的分析报告，语言风格应客观、专业、简洁。"
        "\n--- AI初步总结如下 ---"
        f"{summary}"
        "\n--- 弹幕和评论参考如下 ---"
        f"{content_text}"
    )

    messages = [{"role": "user", "parts": [{"text": prompt}]}] if "gemini" in SUMMARY_MODEL else [{"role": "user", "content": prompt}]

    # 根据模型名称选择API
    if "gemini" in SUMMARY_MODEL:
        analysis_result = await request_gemini(messages)
    else:
        analysis_result = await request_openai(messages)
        
    return analysis_result
