FROM python:3.9

ENV PYTHONUNBUFFERED=1

RUN mkdir /src
COPY . /src
WORKDIR /src
RUN pip install -r requirements.txt

CMD ["python", "main.py"]