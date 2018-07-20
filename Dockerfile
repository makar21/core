ARG BRANCH
ARG REVISION
FROM python:3.6.6-jessie
ARG BRANCH
ARG REVISION
ENV PYTHONUNBUFFERED 1
RUN apt-get update && \
    apt-get install -y --no-install-recommends apt-utils netcat && \
    rm -rf /var/lib/apt/lists/*

ENV APP_VERSION=${BRANCH}:rev-${REVISION}
ENV TATAU_HOME=/tatau
ENV TATAU_CORE=$TATU_HOME/core
RUN mkdir -p $TATAU_CORE
WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY ./minerdog/ /app/minerdog/
COPY ./templates/ /app/templates/
COPY ./manage.py /app/manage.py
RUN echo "APP_VERSION: $APP_VERSION"
# COPY ./bin/wait_for_it /bin/wait_for_it
# RUN python manage.py check
# RUN python manage.py collectstatic --noinput

