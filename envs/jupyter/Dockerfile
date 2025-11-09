FROM jupyter/base-notebook:latest

ENV GRANT_SUDO=yes
ENV CHOWN_HOME=yes
ENV CHOWN_HOME_OPTS='-R'

USER root

RUN apt-get update && apt-get install -y openjdk-17-jdk
RUN python3 -m pip install pyspark==3.5.1

# WORKDIR /home/jovyan/lib
# RUN pip install -e .

# WORKDIR /home/jovyan
CMD ["start-notebook.py", "--user=root"]