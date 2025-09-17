import asyncio
import os
import re
import time
from typing import cast, List, Union, Iterable
from urllib.parse import urlparse, parse_qs

import aiofiles
import httpx
from bilibili_api import video, Credential, live, article
from bilibili_api.favorite_list import get_video_favorite_list_content
from bilibili_api.opus import Opus
from bilibili_api.video import VideoDownloadURLDataDetecter
from nonebot import on_regex
from nonebot.adapters.onebot.v11 import Message, Event, Bot, MessageSegment
from nonebot.adapters.onebot.v11.event import GroupMessageEvent, PrivateMessageEvent
from nonebot.matcher import current_bot
from nonebot.plugin import PluginMetadata, get_plugin_config

from .bilibili_analysis import (
    download_b_file,
    merge_file_to_mp4,
    extra_bili_info,
    get_danmaku_and_comments_async,
    generate_wordcloud_from_list,
)
from .ai_summary import get_ai_summary, generate_ai_analysis
from .config import Config

__plugin_meta__ = PluginMetadata(
    name="Bilibili 评论分析插件",
    description="一个专门用于解析Bilibili链接并分析评论和生成词云的插件",
    type="application",
    homepage="https://github.com/lbsucceed/nonebot-plugin-comment-analysis",
    usage="发送Bilibili链接即可触发",
    supported_adapters={"~onebot.v11"},
    config=Config,
)

try:
    plugin_config = get_plugin_config(Config)
except Exception:
    plugin_config = Config()

# 从配置加载
GLOBAL_NICKNAME: str = str(plugin_config.r_global_nickname or "Bot")
BILI_SESSDATA: str = str(plugin_config.bili_sessdata or "")
VIDEO_DURATION_MAXIMUM: int = int(plugin_config.video_duration_maximum or 480)
VIDEO_MAX_MB: int = 100  # 假设一个默认值

# 构建哔哩哔哩的Credential
credential = Credential(sessdata=BILI_SESSDATA)

BILIBILI_HEADER = {
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 '
        'Safari/537.36',
    'referer': 'https://www.bilibili.com',
}


# ==================== 辅助函数 ====================

def delete_boring_characters(text: str) -> str:
    return re.sub(r'[\n\t\r]', '', text)


def get_file_size_mb(file_path):
    size_in_bytes = os.path.getsize(file_path)
    size_in_mb = size_in_bytes / (1024 * 1024)
    return round(size_in_mb, 2)


async def download_video(url: str, ext_headers: dict = None) -> str:
    file_name = str(time.time()) + ".mp4"
    headers = {
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/100.0.4896.127 Safari/537.36',
              } | (ext_headers or {})
    async with httpx.AsyncClient() as client:
        async with client.stream("GET", url, headers=headers, timeout=60) as resp:
            async with aiofiles.open(file_name, "wb") as f:
                async for chunk in resp.aiter_bytes():
                    await f.write(chunk)
    return os.path.join(os.getcwd(), file_name)


def make_node_segment(user_id, segments: Union[MessageSegment, List]) -> Union[
    MessageSegment, Iterable[MessageSegment]]:
    if isinstance(segments, list):
        return [MessageSegment.node_custom(user_id=user_id, nickname=GLOBAL_NICKNAME,
                                           content=Message(segment)) for segment in segments]
    return MessageSegment.node_custom(user_id=user_id, nickname=GLOBAL_NICKNAME,
                                      content=Message(segments))


async def send_forward_both(bot: Bot, event: Event, segments: Union[MessageSegment, List]) -> None:
    if isinstance(event, GroupMessageEvent):
        await bot.send_group_forward_msg(group_id=event.group_id, messages=segments)
    else:
        await bot.send_private_forward_msg(user_id=event.user_id, messages=segments)


async def send_both(bot: Bot, event: Event, segments: MessageSegment) -> None:
    if isinstance(event, GroupMessageEvent):
        await bot.send_group_msg(group_id=event.group_id, message=Message(segments))
    elif isinstance(event, PrivateMessageEvent):
        await bot.send_private_msg(user_id=event.user_id, message=Message(segments))


async def upload_both(bot: Bot, event: Event, file_path: str, name: str) -> None:
    if isinstance(event, GroupMessageEvent):
        await bot.upload_group_file(group_id=event.group_id, file=file_path, name=name)
    elif isinstance(event, PrivateMessageEvent):
        await bot.upload_private_file(user_id=event.user_id, file=file_path, name=name)


async def auto_video_send(event: Event, data_path: str):
    try:
        bot: Bot = cast(Bot, current_bot.get())
        if data_path is not None and data_path.startswith("http"):
            data_path = await download_video(data_path)

        file_size_in_mb = get_file_size_mb(data_path)
        if file_size_in_mb > VIDEO_MAX_MB:
            await bot.send(event, Message(
                f"当前解析文件 {file_size_in_mb} MB 大于 {VIDEO_MAX_MB} MB，尝试改用文件方式发送，请稍等..."
            ))
            await upload_both(bot, event, data_path, os.path.basename(data_path))
            return
        await send_both(bot, event, MessageSegment.video(f'file://{data_path}'))
    except Exception as e:
        print(f"解析发送出现错误，具体为\n{e}")
    finally:
        for path in [data_path, f"{data_path}.jpg"]:
            if path and os.path.exists(path):
                os.unlink(path)


# ==================== Bilibili 解析器核心 ====================

bili_matcher = on_regex(
    r"(bilibili.com|b23.tv|bili2233.cn|^BV[0-9a-zA-Z]{10}$)", priority=1, block=True
)


@bili_matcher.handle()
async def handle_bilibili(bot: Bot, event: Event) -> None:
    url: str = str(event.message).strip()
    url_reg = r"(http:|https:)\/\/(space|www|live).bilibili.com\/[A-Za-z\d._?%&+\-=\/#]*"
    b_short_rex = r"(https?://(?:b23\.tv|bili2233\.cn)/[A-Za-z\d._?%&+\-=\/#]+)"

    if re.match(r'^BV[1-9a-zA-Z]{10}$', url):
        url = 'https://www.bilibili.com/video/' + url

    if "b23.tv" in url or "bili2233.cn" in url or "QQ小程序" in url:
        b_short_url = re.search(b_short_rex, url.replace("\\", ""))[0]
        resp = httpx.get(b_short_url, headers=BILIBILI_HEADER, follow_redirects=True)
        url: str = str(resp.url)
    else:
        match = re.search(url_reg, url)
        if match:
            url = match.group(0)

    if ('t.bilibili.com' in url or '/opus' in url) and BILI_SESSDATA:
        if '?' in url:
            url = url[:url.index('?')]
        dynamic_id = int(re.search(r'[^/]+(?!.*/)', url)[0])
        dynamic_info = await Opus(dynamic_id, credential).get_info()
        if dynamic_info:
            title = dynamic_info['item']['basic']['title']
            desc = ""
            if paragraphs := [m.get('module_content', {}).get('paragraphs', []) for m in
                             dynamic_info.get('item', {}).get('modules', [])]:
                desc = paragraphs[0][0].get('text', {}).get('nodes', [{}])[0].get('word', {}).get('words', "")
                pics = paragraphs[0][1].get('pic', {}).get('pics', [])
                await bili_matcher.send(Message(f"{GLOBAL_NICKNAME}识别：B站动态，{title}\n{desc}"))
                send_pics = [make_node_segment(bot.self_id, MessageSegment.image(pic['url'])) for pic in pics]
                await send_forward_both(bot, event, send_pics)
        return

    if 'live' in url:
        room_id = re.search(r'\/(\d+)', url.split('?')[0]).group(1)
        room = live.LiveRoom(room_display_id=int(room_id))
        room_info = (await room.get_room_info())['room_info']
        title, cover, keyframe = room_info['title'], room_info['cover'], room_info['keyframe']
        await bili_matcher.send(Message([MessageSegment.image(cover), MessageSegment.image(keyframe),
                                         MessageSegment.text(f"{GLOBAL_NICKNAME}识别：哔哩哔哩直播，{title}")]))
        return

    if 'read' in url:
        read_id = re.search(r'read\/cv(\d+)', url).group(1)
        ar = article.Article(read_id)
        if ar.is_note():
            ar = ar.turn_to_note()
        await ar.fetch_content()
        markdown_path = os.path.join(os.getcwd(), 'article.md')
        async with aiofiles.open(markdown_path, 'w', encoding='utf8') as f:
            await f.write(ar.markdown())
        await bili_matcher.send(Message(f"{GLOBAL_NICKNAME}识别：哔哩哔哩专栏"))
        await upload_both(bot, event, markdown_path, "article.md")
        os.remove(markdown_path)
        return

    if 'favlist' in url and BILI_SESSDATA:
        fav_id = re.search(r'favlist\?fid=(\d+)', url).group(1)
        fav_list = (await get_video_favorite_list_content(fav_id))['medias'][:10]
        favs = [[MessageSegment.image(fav['cover']),
                 MessageSegment.text(f"🧉 标题：{fav['title']}\n📝 简介：{fav['intro']}\n🔗 链接：{fav['link']}")]
                for fav in fav_list]
        await bili_matcher.send(f'{GLOBAL_NICKNAME}识别：哔哩哔哩收藏夹...')
        await send_forward_both(bot, event, make_node_segment(bot.self_id, favs))
        return

    video_id_match = re.search(r"video\/([^\\/ ]+)", url)
    if not video_id_match:
        return
    video_id = video_id_match[1]

    v = video.Video(bvid=video_id, credential=credential)
    video_info = await v.get_info()
    if not video_info:
        await bili_matcher.send(Message(f"{GLOBAL_NICKNAME}识别：B站，出错，无法获取数据！"))
        return

    video_title, video_cover, video_desc, video_duration = video_info['title'], video_info['pic'], video_info[
        'desc'], video_info['duration']

    page_num = 0
    if parsed_url := urlparse(url):
        if query_params := parse_qs(parsed_url.query):
            page_num = int(query_params.get('p', [1])[0]) - 1

    if 'pages' in video_info and page_num < len(video_info['pages']):
        video_duration = video_info['pages'][page_num].get('duration', video_duration)

    video_title_safe = delete_boring_characters(video_title)
    online = await v.get_online()
    online_str = f'🏄‍♂️ 总共 {online["total"]} 人在观看，{online["count"]} 人在网页端观看'

    info_msg = (
        f"\n{GLOBAL_NICKNAME}识别：B站，{video_title_safe}\n{extra_bili_info(video_info)}\n"
        f"📝 简介：{video_desc}\n{online_str}")

    if video_duration > VIDEO_DURATION_MAXIMUM:
        await bili_matcher.send(Message(MessageSegment.image(video_cover)) + Message(
            f"{info_msg}\n--------- \n⚠️ 当前视频时长 {video_duration // 60} 分钟，超过管理员设置的最长时间 {VIDEO_DURATION_MAXIMUM // 60} 分钟！"))
    else:
        await bili_matcher.send(Message(MessageSegment.image(video_cover)) + Message(info_msg))
        download_url_data = await v.get_download_url(page_index=page_num)
        detecter = VideoDownloadURLDataDetecter(download_url_data)
        streams = detecter.detect_best_streams()
        video_url, audio_url = streams[0].url, streams[1].url

        path = os.path.join(os.getcwd(), video_id)
        video_path = f"{path}-video.m4s"
        audio_path = f"{path}-audio.m4s"
        output_path = f"{path}-res.mp4"

        try:
            await asyncio.gather(
                download_b_file(video_url, video_path, print),
                download_b_file(audio_url, audio_path, print)
            )
            await merge_file_to_mp4(video_path, audio_path, output_path)
            await auto_video_send(event, output_path)
        finally:
            for f in [video_path, audio_path]:
                if os.path.exists(f):
                    os.remove(f)

    # --- 词云与热评分析 ---
    try:
        await bili_matcher.send("正在分析弹幕和评论，请稍候...")
        danmakus, comments, top_comment = await get_danmaku_and_comments_async(
            bvid=video_id, aid=video_info['aid'], credential=credential
        )

        # 发送热评
        if top_comment:
            await bili_matcher.send(top_comment)

        # 生成并发送弹幕词云
        if danmakus:
            danmaku_wordcloud_bytes = await generate_wordcloud_from_list(danmakus)
            if danmaku_wordcloud_bytes:
                await bili_matcher.send(
                    Message("☁️ 弹幕词云：") + MessageSegment.image(danmaku_wordcloud_bytes)
                )
            else:
                await bili_matcher.send("弹幕词云生成失败，可能是弹幕数量太少啦。")
        else:
            await bili_matcher.send("该视频暂无弹幕。")

        # 生成并发送评论词云
        if comments:
            comment_wordcloud_bytes = await generate_wordcloud_from_list(comments)
            if comment_wordcloud_bytes:
                await bili_matcher.send(
                    Message("☁️ 评论词云：") + MessageSegment.image(comment_wordcloud_bytes)
                )
            else:
                await bili_matcher.send("评论词云生成失败，可能是评论数量太少啦。")
        else:
            await bili_matcher.send("该视频暂无评论。")

        # --- AI 总结与分析 ---
        if plugin_config.gemini_key or (plugin_config.openai_api_key and plugin_config.openai_base_url):
            await bili_matcher.send("🤖 正在生成 AI 总结与分析，请稍候...")
            
            # 1. 生成初步总结
            summary = await get_ai_summary(danmakus, comments)
            
            # 2. 基于初步总结和原始数据进行二次分析
            analysis = await generate_ai_analysis(summary, danmakus, comments)

            # 3. 将总结和分析合并为转发消息发送
            bot_self_id = bot.self_id
            forward_messages = [
                make_node_segment(bot_self_id, f"📝 AI 初步总结:\n{summary}"),
                make_node_segment(bot_self_id, f"🧠 AI 深度分析:\n{analysis}")
            ]
            await send_forward_both(bot, event, forward_messages)

        else:
            # 如果没有配置AI Key，则尝试使用B站自带的总结
            if BILI_SESSDATA:
                ai_conclusion = await v.get_ai_conclusion(await v.get_cid(0))
                if ai_conclusion.get('model_result', {}).get('summary'):
                    summary_node = make_node_segment(bot.self_id,
                                                     ["bilibili AI总结", ai_conclusion['model_result']['summary']])
                    await send_forward_both(bot, event, summary_node)


    except Exception as e:
        print(f"分析弹幕评论失败: {e}")
        await bili_matcher.send("分析弹幕和评论时出错了。")


