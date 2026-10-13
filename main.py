import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
import pandas as pd
import requests
import websockets
import yfinance as yf

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")
SYMBOL = "QQQ"
EXTRA_SYMBOLS = ("SPY", "TQQQ")
VALID_STATES = {"LONG", "CASH", "UNKNOWN"}
SCHEMA_VERSION = 1
WECOM_WS_URL = "wss://openws.work.weixin.qq.com"
DEFAULT_WECOM_BOT_ID = "aibXei11XiQW5OVSVGmg5b2zpNckHjYkCpx"


def _clean_env(key, default=""):
    """读取环境变量并自动剥离首尾空白与误粘贴的单双引号"""
    return os.getenv(key, default).strip().strip('"').strip("'")


def _clean_history_df(df):
    """清洗日线 DataFrame，兼容 MultiIndex、去除未收盘实时 K 线与重复索引"""
    if df is None or df.empty:
        raise ValueError("拉取到的数据为空")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.rename(columns=str.strip)

    if "Close" not in df.columns:
        close_cols = [c for c in df.columns if "Close" in str(c)]
        if not close_cols:
            raise KeyError(f"未找到 Close 列，当前列: {list(df.columns)}")
        close_col = close_cols[0]
    else:
        close_col = "Close"

    df = df.dropna(subset=[close_col]).sort_index()
    df = df[~df.index.duplicated(keep="last")]

    # 盘中未收盘 K 线过滤：若在美股盘中（美东时间当日 16:00 收盘前）触发，剔除当日未走完的实时日线
    now_et = pd.Timestamp.now(tz="America/New_York")
    last_idx = df.index[-1]
    last_date_et = (
        last_idx.tz_convert("America/New_York").date()
        if getattr(last_idx, "tzinfo", None) is not None
        else last_idx.date()
    )
    if last_date_et == now_et.date() and now_et.hour < 16:
        print(
            f"[INFO] 检测到美股当前处于盘中或盘前 ({now_et.strftime('%Y-%m-%d %H:%M %Z')})，"
            f"剔除 {last_date_et} 未收盘实时数据，以最新已收盘交易日为准。"
        )
        df = df.iloc[:-1]

    return df, close_col


def get_qqq_data_with_retry(retries=3, delay=5):
    """拉取 QQQ 历史日线数据并计算 200MA，带网络重试、MultiIndex 兼容与复权校验"""
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(SYMBOL)
            df = ticker.history(period="2y", interval="1d", auto_adjust=True)
            df, close_col = _clean_history_df(df)

            if len(df) < 200:
                raise ValueError(f"有效交易日数据不足 200 天，实际获取: {len(df)} 天")

            df["SMA_200"] = df[close_col].rolling(window=200).mean()

            latest = df.iloc[-1]
            if pd.isna(latest["SMA_200"]):
                raise ValueError("最新交易日 SMA_200 计算结果为 NaN")

            close_price = round(float(latest[close_col]), 2)
            sma_200 = round(float(latest["SMA_200"]), 2)
            if close_price <= 0 or sma_200 <= 0:
                raise ValueError(
                    f"价格数据异常: close={close_price}, sma_200={sma_200}"
                )

            latest_idx = df.index[-1]
            if getattr(latest_idx, "tzinfo", None) is not None:
                latest_idx = latest_idx.tz_convert("America/New_York")
            date_str = latest_idx.strftime("%Y-%m-%d")

            return date_str, close_price, sma_200
        except Exception as e:
            print(f"[WARN] 数据拉取第 {attempt + 1}/{retries} 次尝试失败: {e}")
            if attempt < retries - 1:
                time.sleep(delay)

    sys.exit("无法从 Yahoo Finance 获取有效数据，程序异常退出。")


def get_market_snapshot():
    """
    拉取 SPY、QQQ、TQQQ 的客观收盘快照（收盘价、单日涨跌幅、200MA 与偏离度）
    若 SPY/TQQQ 辅助拉取失败不会中断主流程
    """
    snapshot = {}
    for sym in ("SPY", "QQQ", "TQQQ"):
        try:
            df = yf.Ticker(sym).history(period="2y", interval="1d", auto_adjust=True)
            df, close_col = _clean_history_df(df)
            if len(df) < 200:
                continue
            closes = df[close_col]
            sma200 = float(closes.rolling(window=200).mean().iloc[-1])
            close_val = float(closes.iloc[-1])
            prev_val = float(closes.iloc[-2]) if len(closes) >= 2 else close_val
            ret_1d = ((close_val - prev_val) / prev_val) * 100 if prev_val > 0 else 0.0
            dist_200 = ((close_val - sma200) / sma200) * 100 if sma200 > 0 else 0.0
            snapshot[sym] = {
                "close": round(close_val, 2),
                "ret_1d": round(ret_1d, 2),
                "sma200": round(sma200, 2),
                "dist_200": round(dist_200, 2),
            }
        except Exception as e:
            print(f"[WARN] 辅助拉取 {sym} 快照异常 (已忽略): {e}")
    return snapshot


def load_previous_state():
    """加载并校验历史状态缓存"""
    if not os.path.exists(STATE_FILE):
        return {
            "schema_version": SCHEMA_VERSION,
            "last_state": "UNKNOWN",
            "last_date": "",
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        raw_state = str(data.get("last_state", "UNKNOWN")).upper().strip()
        if raw_state not in VALID_STATES:
            print(f"[WARN] state.json 中发现非法状态 '{raw_state}'，重置为 UNKNOWN")
            raw_state = "UNKNOWN"
        data["last_state"] = raw_state
        return data
    except Exception as e:
        print(f"[WARN] 读取 state.json 失败 ({e})，重置为待初始化状态")
        return {
            "schema_version": SCHEMA_VERSION,
            "last_state": "UNKNOWN",
            "last_date": "",
        }


def save_current_state(state_data):
    """持久化当前状态（保留已有扩展字段如 wecom_chat_id）"""
    existing = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8-sig") as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    existing = loaded
        except Exception:
            pass
    existing.update(state_data)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)
        f.write("\n")


async def _wecom_aibot_send_async(bot_id, secret, chat_id, title, content):
    """通过企业微信智能机器人长连接 (wss://openws.work.weixin.qq.com) 主动推送 Markdown 消息"""
    sub_req_id = f"aibot_subscribe_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"
    send_req_id = f"aibot_send_msg_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"

    async with websockets.connect(
        WECOM_WS_URL, open_timeout=10, close_timeout=5, ping_interval=None, compression=None
    ) as ws:
        await ws.send(
            json.dumps(
                {
                    "cmd": "aibot_subscribe",
                    "headers": {"req_id": sub_req_id},
                    "body": {"bot_id": bot_id, "secret": secret},
                }
            )
        )
        raw_auth = await asyncio.wait_for(ws.recv(), timeout=10)
        auth_resp = json.loads(raw_auth)
        if auth_resp.get("errcode") != 0:
            raise RuntimeError(f"长连接鉴权失败: {auth_resp}")

        await ws.send(
            json.dumps(
                {
                    "cmd": "aibot_send_msg",
                    "headers": {"req_id": send_req_id},
                    "body": {
                        "chatid": chat_id,
                        "msgtype": "markdown",
                        "markdown": {"content": f"## {title}\n{content}"},
                    },
                }
            )
        )
        for _ in range(5):
            raw_ack = await asyncio.wait_for(ws.recv(), timeout=10)
            ack = json.loads(raw_ack)
            if ack.get("headers", {}).get("req_id") == send_req_id or "errcode" in ack:
                print(f"[企微智能机器人长连接] 推送响应: {raw_ack}")
                if ack.get("errcode") == 0:
                    return True
                raise RuntimeError(f"主动推送返回错误: {ack}")
    return False


async def bind_wecom_aibot_interactive(bot_id, secret, timeout_sec=300):
    """
    一键绑定向导：建立长连接等待用户在手机端向机器人发任意消息，自动捕获 chatid 写入 state.json 并回复测试卡片
    """
    start_ts = time.time()
    printed_prompt = False
    while time.time() - start_ts < timeout_sec:
        try:
            sub_req_id = f"aibot_subscribe_{int(time.time() * 1000)}"
            async with websockets.connect(
                WECOM_WS_URL, open_timeout=10, close_timeout=5, ping_interval=None, compression=None
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "cmd": "aibot_subscribe",
                            "headers": {"req_id": sub_req_id},
                            "body": {"bot_id": bot_id, "secret": secret},
                        }
                    )
                )
                auth_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                if auth_resp.get("errcode") != 0:
                    sys.exit(f"❌ 鉴权失败，请检查 WECOM_BOT_ID 和 WECOM_BOT_SECRET: {auth_resp}")

                if not printed_prompt:
                    print("✅ 企业微信长连接认证成功！正在实时监听消息...")
                    print("👉 请在手机企业微信向该机器人单聊发一句话（如果在群聊请 @机器人 发一句话）...")
                    printed_prompt = True

                async def _ping_loop():
                    while True:
                        await asyncio.sleep(15)
                        await ws.send(
                            json.dumps(
                                {"cmd": "ping", "headers": {"req_id": f"ping_{int(time.time())}"}}
                            )
                        )

                ping_task = asyncio.create_task(_ping_loop())
                try:
                    async for raw_msg in ws:
                        frame = json.loads(raw_msg)
                        req_id = frame.get("headers", {}).get("req_id", "")
                        if req_id.startswith("ping_"):
                            continue
                        print(f"[收到企微帧] {raw_msg}")
                        cmd = frame.get("cmd", "")
                        body = frame.get("body", {})
                        chat_id = (
                            body.get("chatid")
                            or body.get("from", {}).get("userid")
                            or body.get("user", {}).get("userid")
                        )
                        if chat_id:
                            print(f"🎉 成功捕获会话 ID (chatid): {chat_id}")
                            save_current_state({"wecom_chat_id": chat_id})

                            welcome_md = (
                                "### ✅ TQQQ-LRS 哨兵长连接绑定成功！\n"
                                f"- **绑定会话 ID**：`{chat_id}`\n"
                                "- **监控标的**：`SPY / QQQ / TQQQ`（QQQ 复权收盘价 vs 200MA）\n"
                                "- **运行计划**：北京时间每周二至周六 `10:00` 准时推送早报，穿刺 200MA 立即高亮报警！"
                            )

                            if cmd == "aibot_msg_callback" and req_id:
                                await ws.send(
                                    json.dumps(
                                        {
                                            "cmd": "aibot_respond_msg",
                                            "headers": {"req_id": req_id},
                                            "body": {
                                                "msgtype": "stream",
                                                "stream": {
                                                    "id": f"stream_{int(time.time() * 1000)}",
                                                    "content": welcome_md,
                                                    "finish": True,
                                                },
                                            },
                                        }
                                    )
                                )
                            send_req_id = f"aibot_send_msg_{int(time.time() * 1000)}"
                            await ws.send(
                                json.dumps(
                                    {
                                        "cmd": "aibot_send_msg",
                                        "headers": {"req_id": send_req_id},
                                        "body": {
                                            "chatid": chat_id,
                                            "msgtype": "markdown",
                                            "markdown": {"content": welcome_md},
                                        },
                                    }
                                )
                            )
                            ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                            print(f"测试消息回执: {ack}")
                            print("✨ 绑定完成！chatid 已自动保存至 state.json。")
                            return chat_id
                finally:
                    ping_task.cancel()
        except Exception as e:
            print(f"[INFO] 长连接保持自动重连中 ({e})...")
            await asyncio.sleep(1)

    sys.exit("等待超时，未收到机器人会话消息。")


def _build_webhook_request(url, title, content, secret=""):
    """
    根据 Webhook URL 域名自动识别平台（飞书 / 企业微信 / 钉钉），构造原生富文本卡片与 @所有人 强提醒
    """
    lower_url = url.lower()

    if "feishu.cn" in lower_url or "larksuite.com" in lower_url:
        header_color = "red" if "入场" in title or "买入" in title else ("orange" if "离场" in title else "wathet")
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": title},
                    "template": header_color,
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": f"{content}\n\n<at id=all></at>",
                    }
                ],
            },
        }
        if secret:
            timestamp = str(int(time.time()))
            string_to_sign = f"{timestamp}\n{secret}"
            hmac_code = hmac.new(
                string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
            ).digest()
            payload["timestamp"] = timestamp
            payload["sign"] = base64.b64encode(hmac_code).decode("utf-8")
        return "飞书(Feishu)", url, payload

    if "qyapi.weixin.qq.com" in lower_url:
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": f"## {title}\n{content}\n<@all>",
            },
        }
        return "企业微信(WeCom)", url, payload

    if "dingtalk.com" in lower_url:
        target_url = url
        if secret:
            timestamp = str(round(time.time() * 1000))
            string_to_sign = f"{timestamp}\n{secret}"
            hmac_code = hmac.new(
                secret.encode("utf-8"),
                string_to_sign.encode("utf-8"),
                digestmod=hashlib.sha256,
            ).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
            sep = "&" if "?" in target_url else "?"
            target_url = f"{target_url}{sep}timestamp={timestamp}&sign={sign}"

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"## {title}\n{content}",
            },
            "at": {"isAtAll": True},
        }
        return "钉钉(DingTalk)", target_url, payload

    payload = {"title": title, "content": content, "text": f"{title}\n\n{content}"}
    return "自定义Webhook", url, payload


def _post_with_retry(channel_name, build_req_fn, check_resp_fn, retries=3):
    """通用带重试 HTTP POST 执行器"""
    for attempt in range(retries):
        try:
            url, payload = build_req_fn()
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            resp_json = resp.json()
            print(f"[{channel_name}] 推送响应: {resp.text}")
            if check_resp_fn(resp_json):
                return True
            print(f"[WARN] [{channel_name}] 返回业务状态异常: {resp_json}")
        except Exception as e:
            print(f"[WARN] [{channel_name}] 推送第 {attempt + 1}/{retries} 次异常: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return False


def send_notification(title, content):
    """
    多通道自适应推送网关：
    1. 支持 企业微信智能机器人长连接 (WECOM_BOT_ID + WECOM_BOT_SECRET + WECOM_CHAT_ID)
    2. 支持 企业微信 / 飞书 / 钉钉 群机器人 Webhook (WEBHOOK_URL)
    3. 兼容 PUSHPLUS_TOKEN
    """
    webhook_secret = _clean_env("WEBHOOK_SECRET")
    feishu_secret = _clean_env("FEISHU_SECRET") or webhook_secret
    dingtalk_secret = _clean_env("DINGTALK_SECRET") or webhook_secret

    webhook_entries = []
    for raw_url in _clean_env("WEBHOOK_URL").split(","):
        u = raw_url.strip()
        if u:
            webhook_entries.append((u, webhook_secret))

    if _clean_env("FEISHU_WEBHOOK"):
        webhook_entries.append((_clean_env("FEISHU_WEBHOOK"), feishu_secret))
    if _clean_env("WEWORK_WEBHOOK"):
        webhook_entries.append((_clean_env("WEWORK_WEBHOOK"), ""))
    if _clean_env("DINGTALK_WEBHOOK"):
        webhook_entries.append((_clean_env("DINGTALK_WEBHOOK"), dingtalk_secret))

    seen_urls = set()
    unique_webhooks = []
    for u, sec in webhook_entries:
        if u not in seen_urls:
            seen_urls.add(u)
            unique_webhooks.append((u, sec))

    wecom_bot_id = _clean_env("WECOM_BOT_ID") or DEFAULT_WECOM_BOT_ID
    wecom_bot_secret = _clean_env("WECOM_BOT_SECRET")
    cached_state = load_previous_state()
    wecom_chat_id = _clean_env("WECOM_CHAT_ID") or str(cached_state.get("wecom_chat_id", "")).strip()

    pushplus_token = _clean_env("PUSHPLUS_TOKEN")
    has_aibot = bool(wecom_bot_id and wecom_bot_secret and wecom_chat_id)

    if not unique_webhooks and not has_aibot and not pushplus_token:
        print(
            f"[LOG - 未配置推送凭证，仅控制台输出]\n标题: {title}\n内容:\n{content}"
        )
        return True

    results = []

    if has_aibot:
        aibot_ok = False
        for attempt in range(3):
            try:
                if asyncio.run(
                    _wecom_aibot_send_async(
                        wecom_bot_id, wecom_bot_secret, wecom_chat_id, title, content
                    )
                ):
                    aibot_ok = True
                    break
            except Exception as e:
                print(f"[WARN] [企微智能机器人长连接] 第 {attempt + 1}/3 次推送异常: {e}")
                if attempt < 2:
                    time.sleep(3)
        results.append(aibot_ok)

    for raw_url, secret in unique_webhooks:
        platform_name, _, _ = _build_webhook_request(raw_url, title, content, secret)

        def make_req(u=raw_url, s=secret):
            _, target_url, payload = _build_webhook_request(u, title, content, s)
            return target_url, payload

        def check_im_resp(rj):
            if "code" in rj:
                return rj.get("code") == 0
            if "StatusCode" in rj:
                return rj.get("StatusCode") == 0
            if "errcode" in rj:
                return rj.get("errcode") == 0
            return True

        ok = _post_with_retry(platform_name, make_req, check_im_resp)
        results.append(ok)

    if pushplus_token:
        def make_pp_req():
            return "https://www.pushplus.plus/send", {
                "token": pushplus_token,
                "title": title,
                "content": content,
                "template": "markdown",
            }

        ok = _post_with_retry(
            "PushPlus", make_pp_req, lambda rj: rj.get("code") == 200
        )
        results.append(ok)

    return any(results)


def _format_extra_snapshot_lines(snapshot, qqq_close, qqq_sma200, diff_pct):
    """格式化 SPY / QQQ / TQQQ 极简客观行情行"""
    lines = []
    spy = snapshot.get("SPY")
    qqq = snapshot.get("QQQ")
    tqqq = snapshot.get("TQQQ")

    if spy:
        spy_tag = "🟢 >200MA" if spy["close"] >= spy["sma200"] else "🔴 <200MA"
        lines.append(
            f"- **标普 500 (`SPY`)**：`${spy['close']:.2f}` (`{spy['ret_1d']:+.2f}%`) | "
            f"200MA: `${spy['sma200']:.2f}` (`{spy['dist_200']:+.2f}%` {spy_tag})"
        )
    if qqq:
        lines.append(
            f"- **纳指 100 (`QQQ`)**：`${qqq_close:.2f}` (`{qqq['ret_1d']:+.2f}%`) | "
            f"200MA: `${qqq_sma200:.2f}` (`{diff_pct:+.2f}%`)"
        )
    else:
        lines.append(
            f"- **纳指 100 (`QQQ`)**：`${qqq_close:.2f}` | 200MA: `${qqq_sma200:.2f}` (`{diff_pct:+.2f}%`)"
        )
    if tqqq:
        lines.append(
            f"- **3倍纳指 (`TQQQ`)**：`${tqqq['close']:.2f}` (`{tqqq['ret_1d']:+.2f}%`) | "
            f"200MA: `${tqqq['sma200']:.2f}` (`{tqqq['dist_200']:+.2f}%`)"
        )
    return "\n".join(lines)


def main():
    if "--bind" in sys.argv:
        bot_id = _clean_env("WECOM_BOT_ID") or DEFAULT_WECOM_BOT_ID
        secret = _clean_env("WECOM_BOT_SECRET")
        if not secret:
            sys.exit("请先设置环境变量 WECOM_BOT_SECRET 再运行 --bind")
        asyncio.run(bind_wecom_aibot_interactive(bot_id, secret))
        return

    trade_date, close_price, sma_200 = get_qqq_data_with_retry()
    diff_pct = ((close_price - sma_200) / sma_200) * 100
    cushion_dollar = close_price - sma_200
    cushion_drop_pct = ((sma_200 - close_price) / close_price) * 100

    # 计算最新状态（Close >= SMA200 为 LONG，否则为 CASH）
    current_state = "LONG" if close_price >= sma_200 else "CASH"

    cached = load_previous_state()
    last_state = cached.get("last_state", "UNKNOWN")
    last_date = cached.get("last_date", "")

    print(
        f"交易日: {trade_date} | QQQ收盘价: ${close_price:.2f} | "
        f"200MA: ${sma_200:.2f} | 偏离度: {diff_pct:+.2f}%"
    )
    print(f"历史状态: {last_state} ({last_date or '无'}) -> 当前状态: {current_state}")

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 首次初始化运行
    if last_state == "UNKNOWN":
        print("首次运行，初始化基准状态，不发送通知。")
        save_current_state(
            {
                "schema_version": SCHEMA_VERSION,
                "last_state": current_state,
                "last_date": trade_date,
                "last_close": close_price,
                "last_sma200": sma_200,
                "updated_at": now_utc,
            }
        )
        return

    # 休市/重复运行防抖：如果最新交易日与已记录状态日期相同且状态一致（且未传 --force），直接静默退出
    if trade_date == last_date and current_state == last_state and "--force" not in sys.argv:
        print(f"交易日 {trade_date} 已处理且状态未变（可能为美股休市日或重复执行），保持静默。")
        return

    snapshot = get_market_snapshot()
    snapshot_md = _format_extra_snapshot_lines(snapshot, close_price, sma_200, diff_pct)

    # 核心状态机校验：状态翻转时发特级指令，状态未变但进入新交易日时发每日例行简报
    if current_state != last_state:
        if current_state == "LONG":
            title = "🚨【入场指令】QQQ 突破 200MA，建仓 TQQQ！"
            content = (
                f"### 🚨 TQQQ-LRS 触发买入翻转\n"
                f"- **触发日期**：{trade_date}（昨夜美股收盘）\n"
                f"- **QQQ 复权收盘价**：`${close_price:.2f}`\n"
                f"- **QQQ 200MA**：`${sma_200:.2f}`\n"
                f"- **向上偏离幅度**：`{diff_pct:+.2f}%`\n\n"
                f"### 📊 三大标的收盘实况\n{snapshot_md}\n\n"
                f"- **👉 操作建议**：**买入 / 持有 TQQQ**\n\n"
                f"> 原防守仓位（现金/短债 SGOV/BIL）请于今晚美股开盘全额置换为 TQQQ。\n\n"
                f"> ⚠️ *免责声明：本信号由量化模型自动生成，仅供参考，不构成投资建议，据此操作风险自负。*"
            )
        else:
            title = "⚠️【离场指令】QQQ 跌破 200MA，清仓避险！"
            content = (
                f"### ⚠️ TQQQ-LRS 触发清仓翻转\n"
                f"- **触发日期**：{trade_date}（昨夜美股收盘）\n"
                f"- **QQQ 复权收盘价**：`${close_price:.2f}`\n"
                f"- **QQQ 200MA**：`${sma_200:.2f}`\n"
                f"- **向下偏离幅度**：`{diff_pct:+.2f}%`\n\n"
                f"### 📊 三大标的收盘实况\n{snapshot_md}\n\n"
                f"- **👉 操作建议**：**清仓 TQQQ 换回现金/短债**\n\n"
                f"> 市场已转入高风险防守期，请于今晚美股开盘全额卖出 TQQQ，转存 SGOV/BIL/现金。\n\n"
                f"> ⚠️ *免责声明：本信号由量化模型自动生成，仅供参考，不构成投资建议，据此操作风险自负。*"
            )

        print("检测到状态翻转！正在推送特级指令通知...")
        if send_notification(title, content):
            save_current_state(
                {
                    "schema_version": SCHEMA_VERSION,
                    "last_state": current_state,
                    "last_date": trade_date,
                    "last_close": close_price,
                    "last_sma200": sma_200,
                    "updated_at": now_utc,
                }
            )
            print("状态已成功更新并持久化。")
        else:
            sys.exit("推送通知失败！为防止漏报，本次不更新 state.json，等待下次重试。")
    else:
        # 状态未变：若开启每日简报模式 (DAILY_BRIEF=1，默认在云端开启或传 --daily)，发送每日客观实况早报
        daily_enabled = _clean_env("DAILY_BRIEF", "0") == "1" or "--daily" in sys.argv
        if daily_enabled:
            state_badge = "🟢 LONG（持有 TQQQ）" if current_state == "LONG" else "🛡️ CASH（持有短债/现金）"
            if current_state == "LONG":
                safety_line = (
                    f"QQQ 当前高于 200MA **`+${cushion_dollar:.2f}` (`{diff_pct:+.2f}%`)**，"
                    f"需再跌 **`{cushion_drop_pct:.2f}%`** 至 `${sma_200:.2f}` 才会触发清仓线。"
                )
            else:
                safety_line = (
                    f"QQQ 当前低于 200MA **`-${abs(cushion_dollar):.2f}` (`{diff_pct:+.2f}%`)**，"
                    f"需反弹 **`+{cushion_drop_pct:.2f}%`** 站上 `${sma_200:.2f}` 才会触发建仓线。"
                )

            title = f"📊【美股每日早报】{trade_date} | {state_badge}"
            content = (
                f"### 1️⃣ 标普500 / 纳指100 / TQQQ 收盘实况\n"
                f"{snapshot_md}\n\n"
                f"### 2️⃣ 200MA 趋势安全垫 (LRS 核心法则)\n"
                f"- **当前策略状态**：**{state_badge}**（未发生状态翻转，无需调仓）\n"
                f"- **生死线距离**：{safety_line}"
            )
            print("状态未变，正在推送每日客观早报...")
            if send_notification(title, content):
                save_current_state(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "last_state": current_state,
                        "last_date": trade_date,
                        "last_close": close_price,
                        "last_sma200": sma_200,
                        "updated_at": now_utc,
                    }
                )
                print("每日早报已发送，交易日已更新。")
            else:
                sys.exit("每日早报推送失败！")
        else:
            print("状态未变，策略保持静默，不触发通知。")


if __name__ == "__main__":
    main()
