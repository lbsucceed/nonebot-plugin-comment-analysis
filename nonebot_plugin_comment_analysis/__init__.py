import asyncio
import csv
import os
import re
import time
import itertools
from typing import cast, List, Union, Iterable
from urllib.parse import urlparse, parse_qs
from xml.etree import ElementTree as ET

import aiofiles
import httpx
from bilibili_api import video, Credential, live, article, comment
from bilibili_api.favorite_list import get_video_favorite_list_content
from bilibili_api.opus import Opus
from bilibili_api.video import VideoDownloadURLDataDetecter
from nonebot import on_regex, get_driver
from nonebot.adapters.onebot.v11 import Message, Event, Bot, MessageSegment
from nonebot.adapters.onebot.v11.event import GroupMessageEvent, PrivateMessageEvent
from nonebot.matcher import current_bot
from nonebot.plugin import PluginMetadata, get_plugin_config

from .bilibili_analysis import download_b_file, merge_file_to_mp4, extra_bili_info
from .config import Config

__plugin_meta__ = PluginMetadata(
    name="Bilibili 评论分析插件",
    description="一个专门用于解析Bilibili链接并分析评论的插件",
    usage="发送Bilibili链接即可触发",
    config=Config,
)

config = get_plugin_config(Config)

# 从配置加载
plugin_config = Config.parse_obj(get_driver().config.dict())
GLOBAL_NICKNAME: str = str(getattr(plugin_config, "r_global_nickname", "Bot"))
BILI_SESSDATA: str = str(getattr(plugin_config, "bili_sessdata", ""))
VIDEO_DURATION_MAXIMUM: int = int(getattr(plugin_config, "video_duration_maximum", 480))
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


# ==================== 弹幕评论导出功能 ====================

async def get_danmaku_list_async(bvid: str) -> List[str]:
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
            danmaku_list.extend(d.text for d in root.findall("d"))
    except Exception as e:
        print(f"获取弹幕失败: {e}")
    return danmaku_list


async def get_comments_list_async(aid: int, max_comments=2000) -> List[str]:
    comments_list = []
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
            for r in replies:
                comments_list.append(f"{r['member']['uname']}:{r['content']['message']},点赞：{r['like']}")
                count += 1
                for reply in r.get("replies", []):
                    comments_list.append(f"回复@{r['member']['uname']}: {reply['content']['message']}")
                    count += 1
            if res["page"]["num"] * res["page"]["size"] >= res["page"]["count"]:
                break
            page += 1
            await asyncio.sleep(0.3)
    except Exception as e:
        print(f"获取评论失败: {e}")
    return comments_list


async def create_danmaku_comment_csv_async(bvid: str, aid: int) -> str:
    danmakus, comments = await asyncio.gather(
        get_danmaku_list_async(bvid),
        get_comments_list_async(aid)
    )
    file_path = os.path.join(os.getcwd(), f"{bvid}_弹幕评论.csv")

    async with aiofiles.open(file_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        await writer.writerow(["弹幕", "评论"])
        for danmaku, comment_text in itertools.zip_longest(danmakus, comments, fillvalue=""):
            await writer.writerow([danmaku, comment_text])

    print(f"数据已保存至: {file_path}")
    return file_path


# ==================== Bilibili 解析器核心 ====================

bili_matcher = on_regex(
    r"(bilibili.com|b23.tv|bili2233.cn|^BV[0-9a-zA-Z]{10}$)", priority=1, block=True
)

@bili_matcher.handle()
async def handle_bilibili(bot: Bot, event: Event) -> None:
    url: str = str(event.message).strip()
    url_reg = r"(http:|https:)\\/\/(space|www|live).bilibili.com\/[A-Za-z\d._?%&+\-=\/#]*"
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
            if paragraphs := [m.get('module_content', {}).get('paragraphs', []) for m in dynamic_info.get('item', {}).get('modules', [])]:
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

    video_title, video_cover, video_desc, video_duration = video_info['title'], video_info['pic'], video_info['desc'], video_info['duration']
    
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
            f"{info_msg}\n---------\n⚠️ 当前视频时长 {video_duration // 60} 分钟，超过管理员设置的最长时间 {VIDEO_DURATION_MAXIMUM // 60} 分钟！"))
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

    try:
        await bili_matcher.send("正在导出弹幕和评论，请稍候...")
        csv_file_path = await create_danmaku_comment_csv_async(bvid=video_id, aid=video_info['aid'])
        await upload_both(bot, event, csv_file_path, os.path.basename(csv_file_path))
        os.remove(csv_file_path)
    except Exception as e:
        print(f"导出弹幕评论CSV失败: {e}")
        await bili_matcher.send("导出弹幕评论失败了。")

    if BILI_SESSDATA:
        ai_conclusion = await v.get_ai_conclusion(await v.get_cid(0))
        if ai_conclusion.get('model_result', {}).get('summary'):
            summary_node = make_node_segment(bot.self_id, ["bilibili AI总结", ai_conclusion['model_result']['summary']])
            await send_forward_both(bot, event, summary_node)


