FROM tataucloud/python

RUN apt-get update && apt-get install -y curl unzip nano git

RUN mkdir /app
WORKDIR /app

VOLUME ['/app']

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./tatau_core ./tatau_core
COPY ./examples ./examples
COPY ./*.py ./