FROM python:3.11-slim

# System dependencies: ffmpeg, chromium deps for Playwright, Xvfb + noVNC for debug viewing
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    # Playwright/Chromium dependencies
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libx11-xcb1 \
    fonts-liberation \
    fonts-noto-cjk \
    # noVNC debug viewer (remove after testing)
    xvfb \
    x11vnc \
    novnc \
    websockify \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser
RUN playwright install chromium

# Copy application code
COPY . .

# Create output directories
RUN mkdir -p output/sessions output/clips output/voiceovers output/normalized output/heygen_clips

# Expose the server port + noVNC port
EXPOSE 8080 6080

# Entrypoint: start Xvfb + x11vnc + noVNC, then run server
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
CMD ["./entrypoint.sh"]
