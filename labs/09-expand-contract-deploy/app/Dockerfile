FROM python:3.12-slim
RUN pip install --no-cache-dir PyMySQL==1.1.1
WORKDIR /srv
COPY app.py .
EXPOSE 8000
CMD ["python", "app.py"]
