FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application modules and configuration
COPY *.py .
COPY city_config.json .

# Run the city simulator
CMD ["python", "-u", "city_simulator.py"]
