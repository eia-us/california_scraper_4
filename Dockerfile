RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN useradd -m -u 10001 appuser

# Install Python deps first for better caching
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY build_timber_plans.py /app/

USER appuser

# Default command (Render Cron can override)
CMD ["python", "build_timber_plans.py"]