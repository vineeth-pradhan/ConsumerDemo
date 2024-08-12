plugins {
    java
    application
    id("java")
}

group = "com.tollfreeroad.kafkawalk"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.8.0")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.slf4j:slf4j-simple:2.0.16")
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass = "com.tollfreeroad.kafkawalk.ConsumerDemo"
}