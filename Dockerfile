FROM alpine:latest as build-env

COPY . /

RUN apk update                                              && \
    apk add gcc python python-dev musl-dev file yaml py-pip && \
    python -m pip install --upgrade pip setuptools wheel    && \
    python setup.py bdist_wheel --universal

FROM scratch
COPY --from=build-env /dist/furryninja* /furry-ninja-cassandra/
