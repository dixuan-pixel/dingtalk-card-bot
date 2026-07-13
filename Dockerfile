FROM python:3.12-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制脚本
COPY cloud_bot.py .

# 数据目录
RUN mkdir -p /app/data
VOLUME /app/data

# 健康检查端口
EXPOSE 8080

# 自动重连，不需要 restart 策略
CMD ["python3", "cloud_bot.py"]
