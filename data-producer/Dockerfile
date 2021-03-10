FROM hseeberger/scala-sbt:11.0.6_1.3.9_2.13.1 AS build
COPY . .
RUN sbt clean assembly

FROM openjdk:8-jre-alpine3.9
WORKDIR /app
COPY --from=build /root/target/scala-2.13/*.jar BootcampDataProducer.jar
CMD ["java", "-jar", "/app/BootcampDataProducer.jar", "com.trendyol.dataeng.bootcamp.DataProducer"]
