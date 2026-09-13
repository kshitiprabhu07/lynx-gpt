FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

COPY lynx/ ./lynx/
COPY admin/ ./admin/
RUN pip install --no-cache-dir -e .

CMD ["uvicorn", "lynx.api:app", "--host", "0.0.0.0", "--port", "8000"]