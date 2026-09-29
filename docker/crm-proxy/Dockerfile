FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/crm-proxy
COPY server.py ./server.py

RUN useradd --system --create-home proxyuser
USER proxyuser

HEALTHCHECK CMD python -c "import socket; s=socket.create_connection(('127.0.0.1',8080),1); s.close()"

ENTRYPOINT ["python", "server.py"]
