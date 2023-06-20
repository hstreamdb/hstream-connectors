FROM timbru31/java-node:jdk-18-hydrogen
RUN npm install -g json-schema-faker-cli
RUN mkdir /json
COPY ./app/build/install/app /usr/src/app
COPY ./scripts/app.sh /usr/src/app/app.sh
WORKDIR /usr/src/app
ENTRYPOINT ["./app.sh"]
