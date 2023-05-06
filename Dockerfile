FROM ruby:3.2.2-bullseye

RUN apt-get update -y && \
    apt-get install -y python python-setuptools curl ca-certificates gnupg gettext-base && \
    curl -o /tmp/get-pip.py https://bootstrap.pypa.io/get-pip.py && \
    python3 /tmp/get-pip.py && \
    pip install s3cmd && \
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc > ./ACCC4CF8.asc && \
    cat ./ACCC4CF8.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ bullseye-pgdg main" > /etc/apt/sources.list.d/postgresql.list' && \
    apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y postgresql-14 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./entrypoint.rb ./entrypoint.rb
COPY ./postgres_maintenance_service.rb ./postgres_maintenance_service.rb
COPY ./s3cfg.template ./s3cfg.template

RUN chmod +x ./entrypoint.rb

#RUN mkdir /pgdata && chown postgres:postgres /pgdata
#VOLUME /pgdata

ENTRYPOINT ["./entrypoint.rb"]
