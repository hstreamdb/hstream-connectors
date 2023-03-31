FROM openjdk:18
RUN microdnf install findutils
COPY ./app/build/install/app /usr/src/app
COPY ./scripts/app.sh /usr/src/app/app.sh
WORKDIR /usr/src/app
ENTRYPOINT ["./app.sh"]
