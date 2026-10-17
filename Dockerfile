FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt sentry-sdk "psycopg[binary]" temporalio
COPY . .
ENV HOST=0.0.0.0 PORT=10000 OVERTURN_CKPT=/tmp/overturn-ckpt PYTHONUNBUFFERED=1
EXPOSE 10000
CMD ["python", "run.py"]
