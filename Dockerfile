FROM python:3-alpine

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY config.json ./
COPY table.sql ./
COPY AreaInfo.py ./

CMD [ "python3", "AreaInfo.py" ]