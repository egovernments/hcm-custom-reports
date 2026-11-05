FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies first for better layer caching.
# Same layer: drop bundled test suites, bytecode caches and pip itself (~75MB) -
# none are needed at runtime, and cleaning in a later layer would not shrink the image.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && find /usr/local/lib/python3.12/site-packages -type d \( -name tests -o -name test \) -prune -exec rm -rf {} + \
 && find /usr/local/lib/python3.12 -type d -name __pycache__ -prune -exec rm -rf {} + \
 && pip uninstall -y pip setuptools wheel

# Application code: main.py orchestrates; REPORTS_GENERATION holds the report scripts + COMMON_UTILS
COPY main.py .
COPY REPORTS_GENERATION/ ./REPORTS_GENERATION/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python3", "main.py"]
