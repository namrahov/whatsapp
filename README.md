# WhatsApp — System Design, Built for Real

Learning project for topic 3/30 (WhatsApp system design): 1-to-1 and group chat, durable-first delivery,
per-conversation ordering, idempotent retries, ACKs, offline sync and presence. The design reference is
[docs/reference/03_WhatsApp_System_Design_Detailed.pdf](docs/reference/03_WhatsApp_System_Design_Detailed.pdf).

Stack: Java 26, Gradle, Spring Boot 4.1, JPA, PostgreSQL, Liquibase (same as `../twitter`).

## Run locally

- JDK 26: `C:\Users\AZINTELECOM\.jdks\temurin-26.0.2.1` (Project SDK and Gradle JVM in IntelliJ).
- Create the database: `CREATE DATABASE whatsapp;` on `localhost:5433`.
- Set `DB_PASSWORD` (and optionally `DB_URL`, `DB_USERNAME`) in the terminal or IntelliJ run configuration —
  see `.env.example`. No password is stored in the project.
- The app listens on port `8081` (`SERVER_PORT`), so it can run next to twitter on `8080`.

```
./gradlew bootRun
./gradlew test
```

## Authentication (development)

Same as twitter: the caller's id comes from the `X-User-Id` header (the API-gateway pattern, where the gateway
validates the token and forwards the user id). A missing, non-numeric or non-positive value returns `401`.
Controllers get the id with `@CurrentUser long userId`; business code depends only on `CurrentUserProvider`, so
the header implementation can later be swapped for Spring Security / JWT.
