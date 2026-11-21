FROM python:3.9-slim

RUN pip install --no-cache-dir flask flask-cors paho-mqtt requests "qrcode[pil]"
WORKDIR /app
COPY . .
EXPOSE 80
CMD ["python", "app.py"]