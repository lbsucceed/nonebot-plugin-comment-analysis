from typing import Optional

from pydantic import BaseModel, Extra


class Config(BaseModel, extra=Extra.ignore):
    xhs_ck: Optional[str] = ""
    douyin_ck: Optional[str] = ""
    is_oversea: Optional[bool] = False
    bili_sessdata: Optional[str] = ""
    r_global_nickname: Optional[str] = ""
    video_duration_maximum: Optional[int] = 480
    global_resolve_controller: Optional[str] = ""

    # AI Summary configs
    gemini_key: Optional[str] = None  # Gemini 接口密钥
    openai_base_url: Optional[str] = None  # OpenAI 接口地址
    openai_api_key: Optional[str] = None  # OpenAI 接口密钥
    summary_model: str = "gemini-1.5-flash"  # 默认模型名称
    proxy: Optional[str] = None  # 外部 API 代理
    summary_max_length: int = 1000  # 总结最大长度
    summary_min_length: int = 50  # 总结最小长度
    time_out: int = 120  # 模型 API 请求超时时间（秒）
