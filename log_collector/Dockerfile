FROM python:3.14-alpine

WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY collector.py .

ENTRYPOINT [ "python3" ]
CMD [ "collector.py" ]