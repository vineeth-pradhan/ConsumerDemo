plugins {
    id("java")
    application
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
    // https://mvnrepository.com/artifact/org.opensearch.client/opensearch-rest-high-level-client
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.17.1")
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass = "com.tollfreeroad.kafkawalk.ConsumerDemo"
}