FROM python:3.11-alpine

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "uvicorn", "app:app", "--host=0.0.0.0", "--port=5000", "--no-server-header", "--no-date-header" ]
