#!/usr/bin/env python3
"""
刷卡群数据统计员 - 云端版（钉钉 Stream 模式）
================================================
通过钉钉 Stream 模式实时接收群消息，自动解析刷卡/收入数据，
通过机器人 session_webhook 发送当日汇总。

部署方式：
  1. Docker: docker build -t card-swipe-bot . && docker run -d --env-file .env card-swipe-bot
  2. 直接运行: pip install -r requirements.txt && python3 cloud_bot.py
  3. 配置通过环境变量或 config.json
"""

import re
import json
import asyncio
import logging
import os
import time
import threading
from datetime import datetime, date
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import dingtalk_stream
from dingtalk_stream import DingTalkStreamClient, Credential, AckMessage
from dingtalk_stream.chatbot import ChatbotMessage

# ============================================================
# 配置加载（优先环境变量，其次 config.json）
# ============================================================

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

CONFIG_FILE = SCRIPT_DIR / "config.json"


def load_config():
    """加载配置：优先环境变量，其次 config.json"""
    # 尝试从环境变量加载
    app_key = os.environ.get("DINGTALK_APP_KEY", "")
    app_secret = os.environ.get("DINGTALK_APP_SECRET", "")
    webhook_url = os.environ.get("WEBHOOK_URL", "")
    webhook_secret = os.environ.get("WEBHOOK_SECRET", "")
    conversation_id = os.environ.get("CONVERSATION_ID", "")
    conversation_title = os.environ.get("CONVERSATION_TITLE", "刷卡")

    # 如果环境变量不完整，尝试从 config.json 补充
    if not app_key and CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        app_key = cfg.get("app_key", "")
        app_secret = cfg.get("app_secret", "")
        webhook_url = cfg.get("webhook_url", "")
        webhook_secret = cfg.get("webhook_secret", "")
        conversation_id = cfg.get("conversation_id", "")
        conversation_title = cfg.get("conversation_title", "刷卡")

    if not app_key or not app_secret:
        raise ValueError(
            "缺少钉钉凭证！请设置环境变量 DINGTALK_APP_KEY 和 DINGTALK_APP_SECRET，"
            "或在 config.json 中填写 app_key 和 app_secret"
        )

    return {
        "app_key": app_key,
        "app_secret": app_secret,
        "webhook_url": webhook_url,
        "webhook_secret": webhook_secret,
        "conversation_id": conversation_id,
        "conversation_title": conversation_title,
    }


config = load_config()

DINGTALK_APP_KEY = config["app_key"]
DINGTALK_APP_SECRET = config["app_secret"]
WEBHOOK_URL = config["webhook_url"]
WEBHOOK_SECRET = config.get("webhook_secret", "")
TARGET_CONVERSATION_ID = config.get("conversation_id", "")
TARGET_CONVERSATION_TITLE = config.get("conversation_title", "刷卡")

# 健康检查端口（部分云平台需要）
HEALTH_PORT = int(os.environ.get("PORT", "8080"))

# ============================================================
# 日志配置
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("card_swipe_bot")

# ============================================================
# 健康检查 HTTP 服务（部分云平台需要）
# ============================================================

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')

    def log_message(self, format, *args):
        pass  # 不打印访问日志


def start_health_server():
    """启动健康检查 HTTP 服务"""
    try:
        server = HTTPServer(('0.0.0.0', HEALTH_PORT), HealthCheckHandler)
        logger.info(f"健康检查服务启动: http://0.0.0.0:{HEALTH_PORT}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"健康检查服务启动失败: {e}")


# ============================================================
# 金额解析工具
# ============================================================

def parse_amount(text: str) -> int:
    """将金额文本解析为整数（单位：元）"""
    text = text.strip()
    if not text:
        return 0
    has_wan = '万' in text
    num_str = re.sub(r'[^\d.]', '', text)
    if not num_str:
        return 0
    try:
        num = float(num_str)
    except ValueError:
        return 0
    if has_wan:
        return int(num * 10000)
    return int(num)


def format_amount(amount: int) -> str:
    """格式化金额显示：>=1万用"X万"，否则原始数字"""
    if amount >= 10000:
        wan = amount / 10000
        if wan == int(wan):
            return f"{int(wan)}万"
        return f"{wan:.1f}万"
    return str(amount)


# ============================================================
# 消息识别与清洗
# ============================================================

def is_data_message(message: str) -> bool:
    """判断是否为刷卡数据消息"""
    if not re.search(r'\d', message):
        return False
    if '[图片消息]' in message:
        return False
    if '昨日刷卡请入账' in message:
        return False
    if '注意：如需下载' in message:
        return False
    if '刷卡' in message:
        return True
    if re.search(r'\d+(?:\.\d+)?\s*万', message) and '收入' in message:
        return True
    return False


def clean_message(message: str) -> str:
    """去除标题行、@提及、序号前缀"""
    lines = message.strip().split('\n')
    cleaned_parts = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # 去除 @机器人 提及
        line = re.sub(r'@\S+\s*', '', line)
        # 去除"今日刷卡"前缀（无论后面跟什么）
        line = re.sub(r'^今日刷卡[：:\s]*', '', line)
        # 去除"昨日刷卡"行
        if re.match(r'^昨日刷卡', line):
            continue
        # 去除空行（去掉前缀后可能变空）
        if not line:
            continue
        # 去除序号前缀（1. 2. 1、 等）
        line = re.sub(r'^\d+\s*[.、）)]\s*', '', line)
        if line:
            cleaned_parts.append(line)
    return '，'.join(cleaned_parts)


# ============================================================
# 消息解析器
# ============================================================

def parse_message(message: str) -> dict:
    """解析一条群消息，提取刷卡金额和收入金额"""
    result = {
        "card_swipes": [],
        "incomes": [],
        "channels": [],
        "raw": message.strip()
    }

    cleaned = clean_message(message)
    parts = re.split(r'[，,；;、]+', cleaned)

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # 跳过"共XX万..."汇总备注
        if re.match(r'^共\s*\d', part):
            continue

        # 收入模式
        income_match = re.match(r'收入\s*(\d+(?:\.\d+)?)\s*万?\s*[（(]?(.*?)[)）]?$', part)
        if income_match and '收入' in part:
            amount_str = income_match.group(1)
            has_wan = '万' in part
            channel = income_match.group(2).strip() if income_match.group(2) else ''
            channel = re.sub(r'[（）()]', '', channel).strip()
            amount = parse_amount(amount_str + ('万' if has_wan else ''))
            result["incomes"].append({
                "amount": amount,
                "channel": channel,
                "raw": part
            })
            continue

        # 刷卡模式：描述+金额+(渠道)
        swipe_match = re.match(
            r'(.+?)(\d+(?:\.\d+)?)\s*万?\s*[（(](.+?)[)）]',
            part
        )
        if swipe_match:
            note = swipe_match.group(1).strip()
            amount_str = swipe_match.group(2)
            has_wan = '万' in part
            channel = swipe_match.group(3).strip()
            amount = parse_amount(amount_str + ('万' if has_wan else ''))
            result["card_swipes"].append({
                "amount": amount,
                "channel": channel,
                "note": note,
                "raw": part
            })
            continue

        # 纯刷卡金额（无渠道括号）
        amount_only = re.match(r'^(.+?)(\d+(?:\.\d+)?)\s*万?$', part)
        if amount_only:
            note = amount_only.group(1).strip()
            amount_str = amount_only.group(2)
            has_wan = '万' in part
            if note:
                amount = parse_amount(amount_str + ('万' if has_wan else ''))
                result["card_swipes"].append({
                    "amount": amount,
                    "channel": "",
                    "note": note,
                    "raw": part
                })
                continue

        # 纯渠道名
        if not re.search(r'\d', part):
            result["channels"].append(part)

    return result


# ============================================================
# 每日统计管理
# ============================================================

def get_data_file(target_date: date = None) -> Path:
    if target_date is None:
        target_date = date.today()
    return DATA_DIR / f"stats_{target_date.strftime('%Y-%m-%d')}.json"


def load_daily_stats(target_date: date = None) -> dict:
    filepath = get_data_file(target_date)
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        "date": (target_date or date.today()).strftime('%Y-%m-%d'),
        "records": [],
        "total_card_swipe": 0,
        "total_income": 0
    }


def save_daily_stats(stats: dict, target_date: date = None):
    filepath = get_data_file(target_date)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def add_record(message: str, sender: str = "", msg_time: str = "", target_date: date = None) -> dict:
    """处理一条新消息，解析并添加到当日统计"""
    parsed = parse_message(message)
    stats = load_daily_stats(target_date)

    msg_card_swipe = sum(s["amount"] for s in parsed["card_swipes"])
    msg_income = sum(i["amount"] for i in parsed["incomes"])

    # 去重：检查是否已存在相同内容的记录
    for existing in stats["records"]:
        if existing["raw"] == message.strip() and existing.get("sender") == sender:
            logger.info("  跳过重复消息")
            return stats

    record = {
        "time": msg_time or datetime.now().strftime('%H:%M:%S'),
        "sender": sender,
        "raw": message.strip(),
        "card_swipe": msg_card_swipe,
        "income": msg_income,
        "details": parsed
    }
    stats["records"].append(record)
    stats["total_card_swipe"] += msg_card_swipe
    stats["total_income"] += msg_income

    save_daily_stats(stats, target_date)
    return stats


# ============================================================
# 汇总回复生成
# ============================================================

def generate_simple_summary(stats: dict) -> str:
    """生成简洁版汇总（当日累计）"""
    total_card = stats["total_card_swipe"]
    total_income = stats["total_income"]
    record_count = len(stats["records"])
    card_display = format_amount(total_card)
    income_display = format_amount(total_income)
    return f"今日合计刷卡{card_display}（{total_card}元），合计收入{income_display}（{total_income}元），共{record_count}笔"


def generate_detailed_summary(stats: dict) -> str:
    """生成详细版汇总（markdown格式，适合钉钉）"""
    total_card = stats["total_card_swipe"]
    total_income = stats["total_income"]
    records = stats["records"]

    card_display = format_amount(total_card)
    income_display = format_amount(total_income)

    lines = [
        f"### 今日刷卡统计",
        f"",
        f"- **合计刷卡：** {card_display}（{total_card}元）",
        f"- **合计收入：** {income_display}（{total_income}元）",
        f"- **记录数：** {len(records)}条",
    ]

    if records:
        lines.append("")
        lines.append("**明细：**")
        for r in records:
            parts = []
            for s in r["details"]["card_swipes"]:
                if s["channel"]:
                    parts.append(f"{s['note']}{format_amount(s['amount'])}({s['channel']})")
                else:
                    parts.append(f"{s['note']}{format_amount(s['amount'])}")
            for i in r["details"]["incomes"]:
                if i["channel"]:
                    parts.append(f"收入{format_amount(i['amount'])}({i['channel']})")
                else:
                    parts.append(f"收入{format_amount(i['amount'])}")
            for c in r["details"]["channels"]:
                parts.append(c)

            sender_tag = f"[{r['sender']}]" if r["sender"] else ""
            lines.append(f"- {r['time']} {sender_tag} {' | '.join(parts)}")

    return '\n'.join(lines)


# ============================================================
# 消息发送
# ============================================================

import hmac
import hashlib
import base64
import urllib.parse


def send_message(text: str, session_webhook: str = None):
    """
    发送消息到群
    优先使用 session_webhook（企业机器人自带），其次用自定义 Webhook
    """
    headers = {'Content-Type': 'application/json'}

    url = session_webhook if session_webhook else WEBHOOK_URL

    if not url:
        logger.error("  没有可用的发送通道")
        return

    # 如果用自定义 Webhook 且有 secret，计算签名
    if not session_webhook and WEBHOOK_SECRET:
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{WEBHOOK_SECRET}"
        hmac_code = hmac.new(
            WEBHOOK_SECRET.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            digestmod=hashlib.sha256
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        url = f"{url}&timestamp={timestamp}&sign={sign}"

    payload = {
        "msgtype": "text",
        "text": {
            "content": text
        }
    }

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        result = resp.json()
        if result.get("errcode") == 0:
            logger.info("  发送成功")
        else:
            logger.error(f"  发送失败: {result}")
            # session_webhook 失败时尝试自定义 Webhook
            if session_webhook and WEBHOOK_URL:
                logger.info("  尝试用自定义 Webhook 发送...")
                send_message(text, session_webhook=None)
    except Exception as e:
        logger.error(f"  发送异常: {e}")


# ============================================================
# 钉钉 Stream 消息处理
# ============================================================

class CardSwipeHandler(dingtalk_stream.ChatbotHandler):
    """自定义消息处理器：监听刷卡群消息"""

    def __init__(self):
        super().__init__()

    async def process(self, callback):
        """处理收到的消息"""
        try:
            data = callback.data
            msg = ChatbotMessage.from_dict(data)

            # 获取消息基本信息
            conversation_id = msg.conversation_id or ""
            conversation_title = msg.conversation_title or ""
            sender_nick = msg.sender_nick or "未知"
            message_type = msg.message_type or ""

            # 只处理文本消息
            if message_type != 'text':
                return AckMessage.STATUS_OK, 'success'

            # 获取消息内容
            text_content = ""
            if msg.text and hasattr(msg.text, 'content'):
                text_content = msg.text.content or ""

            if not text_content.strip():
                return AckMessage.STATUS_OK, 'success'

            # 过滤：只处理目标群消息
            is_target = False
            if TARGET_CONVERSATION_ID and TARGET_CONVERSATION_ID in conversation_id:
                is_target = True
            elif TARGET_CONVERSATION_TITLE and TARGET_CONVERSATION_TITLE in conversation_title:
                is_target = True
            elif TARGET_CONVERSATION_TITLE and TARGET_CONVERSATION_TITLE in text_content:
                is_target = True

            if not is_target:
                return AckMessage.STATUS_OK, 'success'

            # 过滤：机器人自己发的消息不处理
            if "今日合计刷卡" in text_content or "今日刷卡统计" in text_content:
                return AckMessage.STATUS_OK, 'success'

            logger.info(f"收到消息 [{sender_nick}]: {text_content[:80]}")

            # 判断是否为数据消息
            if not is_data_message(text_content):
                logger.info(f"  非数据消息，跳过")
                return AckMessage.STATUS_OK, 'success'

            # 解析并统计
            msg_time = datetime.now().strftime('%H:%M:%S')
            if msg.create_at:
                try:
                    create_at = int(msg.create_at)
                    msg_time = datetime.fromtimestamp(create_at / 1000).strftime('%H:%M:%S')
                except (ValueError, TypeError):
                    pass

            stats = add_record(text_content, sender_nick, msg_time)
            logger.info(f"  解析完成，当日累计: 刷卡={format_amount(stats['total_card_swipe'])}, 收入={format_amount(stats['total_income'])}")

            # 生成并发送汇总
            summary = generate_simple_summary(stats)
            session_webhook = msg.session_webhook or ""
            send_message(summary, session_webhook=session_webhook)

            return AckMessage.STATUS_OK, 'success'

        except Exception as e:
            logger.error(f"消息处理异常: {e}", exc_info=True)
            return AckMessage.STATUS_OK, 'success'


# ============================================================
# 主程序（带自动重连）
# ============================================================

def run_stream_client():
    """启动 Stream 客户端，断线自动重连"""
    retry_count = 0
    max_retry = 100

    while retry_count < max_retry:
        try:
            credential = Credential(DINGTALK_APP_KEY, DINGTALK_APP_SECRET)
            client = DingTalkStreamClient(credential)

            handler = CardSwipeHandler()
            client.register_callback_handler(ChatbotMessage.TOPIC, handler)

            if retry_count == 0:
                logger.info("连接钉钉 Stream 服务...")
            else:
                logger.info(f"第 {retry_count} 次重连...")

            client.start_forever()

        except KeyboardInterrupt:
            logger.info("手动停止")
            break
        except Exception as e:
            retry_count += 1
            wait = min(5 * retry_count, 60)  # 最多等60秒
            logger.error(f"Stream 连接异常: {e}，{wait}秒后重连...")
            time.sleep(wait)
        else:
            # start_forever 正常退出（不太可能），重置重试计数
            retry_count = 0
            time.sleep(5)

    logger.error("超过最大重连次数，程序退出")


def main():
    logger.info("=" * 50)
    logger.info("刷卡群数据统计员 - 云端版启动")
    logger.info(f"目标群: {TARGET_CONVERSATION_TITLE}")
    logger.info(f"健康检查端口: {HEALTH_PORT}")
    logger.info("=" * 50)

    # 启动健康检查服务（后台线程）
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()

    # 启动 Stream 客户端（阻塞）
    run_stream_client()


if __name__ == "__main__":
    main()
