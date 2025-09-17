<div align="center">
  <a href="https://v2.nonebot.dev/store">
    <img src="https://raw.githubusercontent.com/fllesser/nonebot-plugin-template/refs/heads/resource/.docs/NoneBotPlugin.svg" width="260" alt="NoneBotPluginLogo">
  </a>

  <h1>nonebot-plugin-comment-analysis</h1>
  <p><strong>NoneBot2 插件：解析 B 站链接，返回视频信息、热评摘要与 AI 洞察</strong></p>

  <a href="./LICENSE">
    <img src="https://img.shields.io/github/license/lbsucceed/nonebot-plugin-comment-analysis.svg" alt="license">
  </a>
  <img src="https://img.shields.io/badge/python-3.9%2B-blue.svg" alt="python">
  <img src="https://img.shields.io/badge/NoneBot-2.x-green.svg" alt="nonebot">
</div>

> 丢一个链接，取回完整的评论分析：封面、在线人数、热评、词云和 AI 报告一次到位。

## 📖 介绍

鉴于群友发的视频太抽象了，有必要调查一下成分

`nonebot-plugin-comment-analysis` 专注于解读 Bilibili 视频：
- 自动识别群聊或私聊中的 `bilibili.com`、`b23.tv`、`BV` 链接
- 拉取视频基础信息、封面和实时在线数据
- 在体积允许时回传视频文件，超限则自动改为上传或提示
- 并行抓取弹幕、评论，生成两份词云
- 调用 Gemini / OpenAI 兼容接口生成摘要与深度分析

## ✨ 功能亮点
- **零指令触发**：直接贴链接即可响应
- **双词云视角**：弹幕、评论分开统计，更快找到争议点
- **热评速览**：附带最高赞评论，便于掌握舆论
- **可选 AI 汇总**：两阶段总结 + 分析，支持主流大模型
- **缓存友好**：默认使用本地 `ffmpeg` 合并音视频

## 💿 安装

> 依赖 `ffmpeg`、`wordcloud`、`jieba` 等基础组件，请先在系统中安装。

```bash
# 使用 nb-cli
nb plugin install nonebot-plugin-comment-analysis --upgrade

# 或使用 pip
pip install --upgrade nonebot-plugin-comment-analysis
```

安装后在 `pyproject.toml` 或 `.env` 中声明插件：

```toml
[tool.nonebot]
plugins = ["nonebot_plugin_comment_analysis"]
```

## ⚙️ 配置

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `bili_sessdata` | `str` | `""` | B 站登录 Cookie，缺省时仅能访问公开数据，建议使用单独账号防止失效 |
| `r_global_nickname` | `str` | `"Bot"` | 合并转发消息中展示的机器人昵称 |
| `video_duration_maximum` | `int` | `480` | 自动发送的视频最长秒数，超过后仅返回信息并改为文件上传 |
| `gemini_key` | `str` \| `null` | Google Gemini API Key，配置后启用 AI 总结 |
| `openai_base_url` | `str` | `null` | OpenAI 兼容接口地址（例如自建反代） |
| `openai_api_key` | `str` | `null` | OpenAI API Key，与 `openai_base_url` 配套使用 |
| `summary_model` | `str` | `"gemini-1.5-flash"` | 摘要与分析使用的模型名称 |
| `proxy` | `str` | `null` | 外部 API 请求代理，部署在国内时建议设置；无需代理可留空 |
| `summary_max_length` | `int` | `1000` | 发送给大模型的上下文最大字符数 |
| `summary_min_length` | `int` | `50` | 生成文本的最小长度约束 |
| `time_out` | `int` | `120` | 调用外部 API 的超时时间（秒） |

其余字段（如 `xhs_ck`、`douyin_ck`、`global_resolve_controller`）为旧版本兼容保留，目前不会影响 B 站解析逻辑，可忽略。

在 NoneBot 的 `.env`/`.env.*` 中可以直接写小写键：

```
bili_sessdata=SESSDATA=xxx
proxy=http://127.0.0.1:10809
gemini_key=YOUR_GEMINI_KEY
```

## 🚀 使用方式
1. 启动 NoneBot。
2. 在 QQ 群聊或私聊中发送任意 B 站视频链接、短链或 `BV` 号。
3. 插件会依次返回：
   - 视频封面与基础信息
   - 视频文件或提示（视文件大小而定）
   - 热评摘要
   - 弹幕词云与评论词云
   - AI 总结与分析（已配置大模型时）

若需要临时停用评论解析，可通过 NoneBot 的插件管理关闭该插件。

## ❗ 常见问题
- **视频太大发送失败？** 调整 `video_duration_maximum` 或直接使用返回的下载地址，也可以让插件改为上传群文件。
- **AI 总结报错？** 检查密钥与代理配置是否正确，必要时降低 `summary_max_length` 以减少请求体积。
- **词云是空白？** 抽样弹幕/评论不足或全部被过滤时会出现空图，属于正常情况。

## 🙏 致谢
- [NoneBot](https://github.com/nonebot/nonebot2)
- [bilibili-api-python](https://github.com/Nemo2011/bilibili-api)
- [nonebot-plugin-resolver](https://github.com/zhiyu1998/nonebot-plugin-resolver)

## 📄 License

本项目在 [MIT License](./LICENSE) 下发布。
