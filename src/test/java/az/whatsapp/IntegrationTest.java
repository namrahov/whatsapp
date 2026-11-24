package az.whatsapp;

import az.whatsapp.dto.request.CreateUserRequest;
import az.whatsapp.service.ConversationService;
import az.whatsapp.service.UserService;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Base for integration tests: the whole application against real PostgreSQL (schema from Liquibase), Kafka and
 * Redis in Docker containers, started once and shared by all *IT classes (one Spring context). Tests do not clean
 * up; each creates its own users and conversations, so they do not see each other's data.
 */
// A real embedded server (not the mock environment): the WebSocket container needs one.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class IntegrationTest {

    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @ServiceConnection(name = "redis")
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4.2").withExposedPorts(6379);

    static {
        Startables.deepStart(POSTGRES, KAFKA, REDIS).join();
    }

    private static final AtomicInteger USER_COUNTER = new AtomicInteger();

    @Autowired
    protected UserService users;

    @Autowired
    protected ConversationService conversations;

    @Autowired
    protected JdbcClient jdbc;

    protected long newUser() {
        String name = "u" + USER_COUNTER.incrementAndGet() + "_" + UUID.randomUUID().toString().substring(0, 8);
        return users.create(new CreateUserRequest(name, name)).getId();
    }

    protected long newDirect(long a, long b) {
        return conversations.getOrCreateDirect(a, b).getId();
    }

    protected long newGroup(long creator, Long... members) {
        return conversations.createGroup(creator, "group", Set.of(members)).getId();
    }

    protected long outboxEvents(String eventType, long conversationId) {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE event_type = :t AND kafka_key = :k")
                .param("t", eventType)
                .param("k", String.valueOf(conversationId))
                .query(Long.class)
                .single();
    }

    protected long lastSequence(long conversationId) {
        return jdbc.sql("SELECT last_sequence FROM conversations WHERE id = :id")
                .param("id", conversationId)
                .query(Long.class)
                .single();
    }

    protected static byte[] payload(String text) {
        return text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
