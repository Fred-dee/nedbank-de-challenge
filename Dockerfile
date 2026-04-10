FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ pipeline/
COPY config/ config/

RUN groupadd -g 1000 pipeline && \
    useradd -m -u 1000 -g 1000 -s /bin/bash pipeline

USER pipeline:pipeline

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "-m", "pipeline.run_all"]
# Force ownership of the mount to root before running the pipeline
#CMD ["sh", "-c", "chown -R root:root /data/output && chmod -R 777 /data/output 2?dev/null && python -m pipeline.run_all"]
