FROM jupyter/minimal-notebook

WORKDIR /home/$NB_USER/work

COPY pyproject.toml poetry.lock ./
COPY dwh.cfg ./

ENV PYTHONPATH=${PYTHONPATH}:${PWD}

RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev