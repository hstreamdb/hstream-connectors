FROM amazoncorretto:17

COPY ./app/build/install/app /usr/src/app
COPY ./scripts/app.sh /usr/src/app/app.sh

WORKDIR /usr/src/app

ENTRYPOINT ["./app.sh"]
