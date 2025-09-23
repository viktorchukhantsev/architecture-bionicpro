FROM python:3.13-slim

WORKDIR /reports

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

COPY . .

EXPOSE 8000

RUN \
    groupadd -g 1001 runner_reports && \
    useradd -r -u 1001 -g runner_reports runner_reports && \
    chown -R runner_reports:runner_reports /reports

USER runner_reports

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
