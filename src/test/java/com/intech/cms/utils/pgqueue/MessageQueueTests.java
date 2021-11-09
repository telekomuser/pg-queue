package com.intech.cms.utils.pgqueue;

import com.impossibl.postgres.jdbc.PGDriver;
import com.intech.cms.utils.pgqueue.model.Message;
import com.intech.cms.utils.pgqueue.model.MessageState;
import com.intech.cms.utils.pgqueue.repository.IMessageQueue;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@Testcontainers
public class MessageQueueTests {
    private static final String SCHEMA_NAME = "queues";
    private static final String QUEUE_NAME = "test";
    private static final String CONSUMER_ID = "1:1";

    @Container
    private static final PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:13")
            .withDatabaseName("testdb")
            .withUsername("postgres")
            .withPassword("postgres");

    private static DataSource dataSource;
    private static IMessageQueue queue;

    @BeforeAll
    public static void beforeAll() {
        dataSource = createDataSource();
        queue = IMessageQueue.builder()
                .jdbcTemplate(new NamedParameterJdbcTemplate(dataSource))
                .transactionManager(new JdbcTransactionManager(dataSource))
                .schema(SCHEMA_NAME)
                .queueName(QUEUE_NAME)
                .build();
    }

    @BeforeEach
    public void setUp() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s RESTART IDENTITY", SCHEMA_NAME, QUEUE_NAME));
        }
    }

    @Test
    public void testPut() {
        String payload = "Test message";
        Message sentMessage = queue.put(Message.builder().payload(payload).build());
        assertNotNull(sentMessage);
        assertAll("offset",
                () -> assertNotNull(sentMessage.getOffset()),
                () -> assertTrue(sentMessage.getOffset() > 0)
        );
        assertEquals(MessageState.ACCEPTED, sentMessage.getState());
        assertNotNull(sentMessage.getDateAdded());
        assertNull(sentMessage.getConsumerId());
        assertEquals(payload, sentMessage.getPayload());
    }

    @Test
    public void testFindNextAsync() throws Exception {
        CompletableFuture<List<Message>> future = CompletableFuture.supplyAsync(() -> {
            try {
                return queue.findNext(CONSUMER_ID, 10);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
        });

        assertFalse(future.isDone());
        Thread.sleep(1000);

        String payload = "Test message";
        assertNotNull(queue.put(Message.builder().payload(payload).build()));

        List<Message> messages = assertTimeoutPreemptively(ofSeconds(1800),
                (ThrowingSupplier<List<Message>>) future::get);

        assertAll("exactly one message",
                () -> assertNotNull(messages),
                () -> assertEquals(1, messages.size()));

        Message actualMessage = messages.get(0);

        assertEquals(MessageState.SENT, actualMessage.getState());
        assertEquals(CONSUMER_ID, actualMessage.getConsumerId());
        assertEquals(payload, actualMessage.getPayload());
    }

    @Test
    public void testFindNextSync() {
        List<Message> expectedEmpty = queue.findNextOrEmpty(CONSUMER_ID, 10);

        assertAll("empty messages",
                () -> assertNotNull(expectedEmpty),
                () -> assertTrue(expectedEmpty.isEmpty()));

        String payload = "Test message";
        assertNotNull(queue.put(Message.builder().payload(payload).build()));

        List<Message> messages = queue.findNextOrEmpty(CONSUMER_ID, 10);

        assertAll("exactly one message",
                () -> assertNotNull(messages),
                () -> assertEquals(1, messages.size()));

        Message actualMessage = messages.get(0);

        assertEquals(MessageState.SENT, actualMessage.getState());
        assertEquals(CONSUMER_ID, actualMessage.getConsumerId());
        assertEquals(payload, actualMessage.getPayload());
    }

    @Test
    public void testResetUncommitted() {
        String payload = "Test message";
        assertNotNull(queue.put(Message.builder().payload(payload).build()));
        List<Message> messages = queue.findNextOrEmpty(CONSUMER_ID, 10);

        assertAll("exactly one message",
                () -> assertNotNull(messages),
                () -> assertEquals(1, messages.size()));

        queue.resetAllUncommittedMessages(CONSUMER_ID);
        List<Message> expectedMessages = queue.findNextOrEmpty(CONSUMER_ID, 10);

        assertAll("exactly one message",
                () -> assertNotNull(expectedMessages),
                () -> assertEquals(1, expectedMessages.size()));
    }

    @Test
    public void testCommitAllConsumed() {
        String payload = "Test message";
        assertNotNull(queue.put(Message.builder().payload(payload).build()));
        List<Message> messages = queue.findNextOrEmpty(CONSUMER_ID, 10);

        assertAll("exactly one message",
                () -> assertNotNull(messages),
                () -> assertEquals(1, messages.size()));

        queue.commitAllConsumedMessages(CONSUMER_ID);
        List<Message> expectedMessages = queue.findNextOrEmpty(CONSUMER_ID, 10);

        assertAll("empty messages",
                () -> assertNotNull(expectedMessages),
                () -> assertTrue(expectedMessages.isEmpty()));
    }

    private static DataSource createDataSource() {
        SimpleDriverDataSource ds = new SimpleDriverDataSource();

        ds.setDriverClass(PGDriver.class);
        ds.setUrl(postgreSQLContainer.getJdbcUrl().replace("postgresql", "pgsql").split("\\?")[0]);
        ds.setUsername(postgreSQLContainer.getUsername());
        ds.setPassword(postgreSQLContainer.getPassword());

        return ds;
    }
}
