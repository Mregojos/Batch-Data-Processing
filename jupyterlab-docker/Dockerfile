# Jupyterlab Dockerfile
FROM python:slim
WORKDIR /app
COPY . .
VOLUME /app
RUN pip install -U -r requirements.txt -q
EXPOSE 8888 4200
CMD ["jupyter", "lab", "--allow-root", "--ip=0.0.0.0", "--no-browser"]
