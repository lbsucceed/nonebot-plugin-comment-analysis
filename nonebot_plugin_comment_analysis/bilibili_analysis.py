import asyncio
import concurrent.futures
import contextlib
import os
import platform
import re
import subprocess
from functools import partial
from io import BytesIO
from typing import List, Optional, Tuple, Dict

import aiofiles
import httpx
import jieba.analyse
import numpy as np
import shutil
from PIL import Image
from bilibili_api import video, Credential, comment
from emoji import replace_emoji
from nonebot import logger
from wordcloud import WordCloud
from xml.etree import ElementTree as ET


# ==================== 词云生成 ====================

# 词云相关配置
FONT_PATH = str(os.path.join(os.path.dirname(__file__), "SourceHanSans.otf"))
WORDCLOUD_WIDTH = 1000
WORDCLOUD_HEIGHT = 800
WORDCLOUD_BACKGROUND_COLOR = "white"
WORDCLOUD_COLORMAP = "viridis"


def _preprocess_text(text: str) -> str:
    """文本预处理，去除URL、emoji等"""
    url_regex = re.compile(
        r"(https?://(www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?://(www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})"
    )
    text = url_regex.sub("", text)
    text = re.sub(r"\u200b", "", text)
    text = replace_emoji(text)
    return text


def _generate_wordcloud_image(text_list: List[str]) -> Optional[bytes]:
    """同步函数：生成词云图的核心实现"""
    if not text_list:
        return None

    full_text = " ".join(text_list)
    processed_text = _preprocess_text(full_text)
    
    # 使用jieba进行分词和词频统计
    frequency = dict(jieba.analyse.extract_tags(processed_text, topK=0, withWeight=True))

    if not frequency:
        return None

    try:
        wordcloud = WordCloud(
            font_path=FONT_PATH,
            width=WORDCLOUD_WIDTH,
            height=WORDCLOUD_HEIGHT,
            background_color=WORDCLOUD_BACKGROUND_COLOR,
            colormap=WORDCLOUD_COLORMAP,
        )
        image = wordcloud.generate_from_frequencies(frequency).to_image()
        image_bytes = BytesIO()
        image.save(image_bytes, format="PNG")
        return image_bytes.getvalue()
    except Exception as e:
        logger.error(f"生成词云失败: {e}")
        return None


async def generate_wordcloud_from_list(text_list: List[str]) -> Optional[bytes]:
    """异步接口：接收文本列表并生成词云图"""
    loop = asyncio.get_running_loop()
    pfunc = partial(_generate_wordcloud_image, text_list)
    with concurrent.futures.ThreadPoolExecutor() as pool:
        return await loop.run_in_executor(pool, pfunc)


# ==================== B站数据获取 ====================

"""
哔哩哔哩的头请求
"""
BILIBILI_HEADER = {
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 '
        'Safari/537.36',
    'referer': 'https://www.bilibili.com',
}


async def get_danmaku_and_comments_async(
    bvid: str, aid: int, credential: Credential, max_comments=2000
) -> Tuple[List[str], List[str], Optional[str]]:
    """
    异步获取弹幕、评论和热评
    :return: (弹幕列表, 评论列表, 热评字符串)
    """

    async def _get_danmaku() -> List[str]:
        danmaku_list = []
        try:
            async with httpx.AsyncClient() as client:
                url = f"https://api.bilibili.com/x/player/pagelist?bvid={bvid}&jsonp=jsonp"
                resp = await client.get(url, headers=BILIBILI_HEADER)
                cid = resp.json()["data"][0]["cid"]

                xml_url = f"https://api.bilibili.com/x/v1/dm/list.so?oid={cid}"
                resp = await client.get(xml_url, headers=BILIBILI_HEADER)
                resp.encoding = "utf-8"
                root = ET.fromstring(resp.text)
                danmaku_list.extend(d.text for d in root.findall("d") if d.text)
        except Exception as e:
            logger.error(f"获取弹幕失败: {e}")
        return danmaku_list

    async def _get_comments() -> Tuple[List[str], Optional[Dict]]:
        comments_list = []
        top_comment_obj = None
        try:
            page = 1
            count = 0
            while count < max_comments:
                res = await comment.get_comments(
                    oid=aid,
                    type_=comment.CommentResourceType.VIDEO,
                    page_index=page,
                    credential=credential
                )
                replies = res.get("replies", [])
                if not replies:
                    break
                
                # 寻找热评
                for r in replies:
                    if top_comment_obj is None or r['like'] > top_comment_obj['like']:
                        top_comment_obj = r
                    
                    # 添加评论到列表
                    comments_list.append(r['content']['message'])
                    count += 1
                    for reply in r.get("replies", []):
                        comments_list.append(reply['content']['message'])
                        count += 1

                if res["page"]["num"] * res["page"]["size"] >= res["page"]["count"]:
                    break
                page += 1
                await asyncio.sleep(0.3)
        except Exception as e:
            logger.error(f"获取评论失败: {e}")
        return comments_list, top_comment_obj

    # 并发执行
    danmakus, (comments, top_comment_obj) = await asyncio.gather(
        _get_danmaku(),
        _get_comments()
    )

    top_comment_str = None
    if top_comment_obj:
        top_comment_str = (
            f"⭐热评 (点赞: {top_comment_obj['like']})：\n"
            f"UP: {top_comment_obj['member']['uname']}\n"
            f"💬: {top_comment_obj['content']['message']}"
        )

    return danmakus, comments, top_comment_str


async def is_ffmpeg_installed():
    """检查ffmpeg是否安装"""

    # 检查ffmpeg是否在环境变量中
    ffmpeg_path = shutil.which('ffmpeg')
    if ffmpeg_path:
        return True
    
    # 如果仍然未找到，尝试异步调用ffmpeg命令
    try:
        # 根据操作系统选择合适的命令
        if platform.system() == "Windows":
            process = await asyncio.create_subprocess_exec(
                'ffmpeg', '-version',
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
        else:
            process = await asyncio.create_subprocess_exec(
                'ffmpeg', '-version',
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
        await process.wait()
        if process.returncode == 0:
            return True
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"检查ffmpeg时发生错误: {e}")

    return False

async def download_b_file(url, full_file_name, progress_callback):
    """
        下载视频文件和音频文件
    :param url:
    :param full_file_name:
    :param progress_callback:
    :return:
    """
    async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(local_address="0.0.0.0")) as client:
        async with client.stream("GET", url, headers=BILIBILI_HEADER) as resp:
            current_len = 0
            total_len = int(resp.headers.get('content-length', 0))
            print(total_len)
            async with aiofiles.open(full_file_name, "wb") as f:
                async for chunk in resp.aiter_bytes():
                    current_len += len(chunk)
                    await f.write(chunk)
                    progress_callback(f'下载进度：{round(current_len / total_len, 3)}')


async def merge_file_to_mp4(v_full_file_name: str, a_full_file_name: str, output_file_name: str, log_output: bool = False):
    """
    合并视频文件和音频文件
    :param v_full_file_name: 视频文件路径
    :param a_full_file_name: 音频文件路径
    :param output_file_name: 输出文件路径
    :param log_output: 是否显示 ffmpeg 输出日志，默认忽略
    :return:
    """
    logger.info(f'正在合并：{output_file_name}')

    # 检查 ffmpeg 是否安装
    if not await is_ffmpeg_installed():
        logger.error('ffmpeg 未安装，请先安装 ffmpeg 并配置环境变量。可参考插件主页说明。')
        return

    # 构建 ffmpeg 命令
    command = f'ffmpeg -y -i "{v_full_file_name}" -i "{a_full_file_name}" -c copy "{output_file_name}"'
    stdout = None if log_output else subprocess.DEVNULL
    stderr = None if log_output else subprocess.DEVNULL

    if platform.system() == "Windows":
        # Windows 下使用 run_in_executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: subprocess.call(command, shell=True, stdout=stdout, stderr=stderr)
        )
    else:
        # 其他平台使用 create_subprocess_shell
        process = await asyncio.create_subprocess_shell(
            command,
            shell=True,
            stdout=stdout,
            stderr=stderr
        )
        await process.communicate()


def extra_bili_info(video_info):
    """
        格式化视频信息
    """
    video_state = video_info['stat']
    video_like, video_coin, video_favorite, video_share, video_view, video_danmaku, video_reply = video_state['like'], \
        video_state['coin'], video_state['favorite'], video_state['share'], video_state['view'], video_state['danmaku'], \
        video_state['reply']

    video_data_map = {
        "点赞": video_like,
        "硬币": video_coin,
        "收藏": video_favorite,
        "分享": video_share,
        "总播放量": video_view,
        "弹幕数量": video_danmaku,
        "评论": video_reply
    }

    video_info_result = ""
    for key, value in video_data_map.items():
        if int(value) > 10000:
            formatted_value = f"{value / 10000:.1f}万"
        else:
            formatted_value = value
        video_info_result += f"{key}: {formatted_value} | "

    return video_info_result


async def get_bili_video_info(bvid: str):
    """
        获取视频信息
    :param bvid:
    :return:
    """

    async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(local_address="0.0.0.0")) as client:
        resp = await client.get(
            f'https://api.bilibili.com/x/web-interface/view?bvid={bvid}',
            headers=BILIBILI_HEADER
        )
        return resp.json()


async def get_bili_video_dl_url(bvid: str, cid: str):
    """
        获取视频下载地址
    :param bvid:
    :param cid:
    :return:
    """

    async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(local_address="0.0.0.0")) as client:
        resp = await client.get(
            f'https://api.bilibili.com/x/player/playurl?bvid={bvid}&cid={cid}&fnval=4048',
            headers=BILIBILI_HEADER
        )
        return resp.json()
