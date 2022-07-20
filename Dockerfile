ARG PYTHON_VERSION=3.7
ARG DEBIAN_VERSION=bullseye

FROM python:${PYTHON_VERSION}-${DEBIAN_VERSION} as build
ENV PYTHONUNBUFFERED 1
ARG PIP_INDEX_URL
ENV PIP_INDEX_URL ${PIP_INDEX_URL}
RUN pip --no-cache install --upgrade pip
COPY setup.py .
COPY psqlextra/_version.py psqlextra/_version.py
COPY README.md .
RUN pip install .[test] .[analysis] --no-cache-dir --no-cache --prefix /python-packages --no-warn-script-location

FROM python:${PYTHON_VERSION}-${DEBIAN_VERSION}
ENV PROJECT_DIR /project
WORKDIR $PROJECT_DIR
ENV PYTHONUNBUFFERED 1
COPY --from=build /python-packages /usr/local
COPY . .