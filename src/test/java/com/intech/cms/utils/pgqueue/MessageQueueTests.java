package com.intech.cms.utils.pgqueue;

import com.intech.cms.utils.pgqueue.model.Message;
import com.intech.cms.utils.pgqueue.model.MessageState;
import com.intech.cms.utils.pgqueue.repository.MessageQueue;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlGroup;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@ExtendWith(SpringExtension.class)
@Testcontainers
@ContextConfiguration(classes = MessageQueueTests.Configuration.class)
@SqlGroup({
        @Sql(scripts = "classpath:/sql/init-data.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD),
        @Sql(scripts = "classpath:/sql/truncate.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
})
public class MessageQueueTests {
    private static final String SCHEMA_NAME = "queues";
    private static final String QUEUE_NAME = "test";
    private static final String CONSUMER_ID = "consumerId";

    @Container
    private static final PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:13")
            .withDatabaseName("testdb")
            .withUsername("postgres")
            .withPassword("postgres");

    @Autowired
    private MessageQueue queue;

    @Test
    public void testGetName() {
        assertEquals(QUEUE_NAME, queue.getName());
    }

    @Test
    public void testExistsNewMessages() {
        assertTrue(queue.isMessagesExistsForState(MessageState.ACCEPTED));
    }

    @Test
    public void testNPEForNullState() {
        assertThrows(NullPointerException.class, () -> queue.isMessagesExistsForState(null));
    }

    @Test
    public void testPut() {
        String payload = "Test put new message";
        Message sentMessage = queue.put(payload);
        assertNotNull(sentMessage);
        assertAll("offset",
                () -> assertNotNull(sentMessage.getOffset()),
                () -> assertTrue(sentMessage.getOffset() > 0)
        );
        assertNotNull(sentMessage.getDateAdded());
        assertEquals(MessageState.ACCEPTED, sentMessage.getState());
        assertNull(sentMessage.getConsumerId());
        assertNotNull(sentMessage.getStateUpdatedAt());
        assertEquals(payload, sentMessage.getPayload());
    }

    @Test
    public void testNPEWhenPutNullPayload() {
        assertThrows(NullPointerException.class, () -> queue.put(null));
    }

    @Test
    public void testSelectNewMessages() {
        List<Message> messages = queue.selectByStateAndUpdate(MessageState.ACCEPTED, 3, MessageState.SENT, CONSUMER_ID);

        assertFalse(messages.isEmpty());
        assertEquals(1, messages.size());

        Message actualMessage = messages.get(0);

        assertEquals(1, actualMessage.getOffset());
        assertNotNull(actualMessage.getDateAdded());
        assertEquals(MessageState.SENT, actualMessage.getState());
        assertNotNull(actualMessage.getStateUpdatedAt());
        assertEquals(CONSUMER_ID, actualMessage.getConsumerId());
        assertEquals("Test new message", actualMessage.getPayload());
    }

    @Test
    public void testInvalidArgsForSelectNewMessages() {
        assertThrows(NullPointerException.class, () -> queue.selectByStateAndUpdate(null, 3, MessageState.SENT, CONSUMER_ID));
        assertThrows(NullPointerException.class, () -> queue.selectByStateAndUpdate(MessageState.ACCEPTED, 3, null, CONSUMER_ID));
        assertThrows(NullPointerException.class, () -> queue.selectByStateAndUpdate(MessageState.ACCEPTED, 3, MessageState.SENT, null));
        assertThrows(IllegalArgumentException.class, () -> queue.selectByStateAndUpdate(MessageState.ACCEPTED, 0, MessageState.SENT, CONSUMER_ID));
        assertThrows(IllegalArgumentException.class, () -> queue.selectByStateAndUpdate(MessageState.ACCEPTED, 3, MessageState.ACCEPTED, CONSUMER_ID));
    }

    @Test
    public void testUpdateStateByConsumerId() {
        assertEquals(2, queue.updateStateByConsumerId(CONSUMER_ID, MessageState.SENT, MessageState.DELIVERED));
    }

    @Test
    public void testInvalidArgsForUpdateStateByConsumerId() {
        assertThrows(NullPointerException.class, () -> queue.updateStateByConsumerId(null, MessageState.SENT, MessageState.DELIVERED));
        assertThrows(NullPointerException.class, () -> queue.updateStateByConsumerId(CONSUMER_ID, null, MessageState.DELIVERED));
        assertThrows(NullPointerException.class, () -> queue.updateStateByConsumerId(CONSUMER_ID, MessageState.SENT, null));
    }

    @Test
    void testUpdateStateOlderThan() {
        assertEquals(1, queue.updateStateOlderThan(MessageState.SENT, MessageState.ACCEPTED, 1, LocalDateTime.now()));
    }

    @Test
    void testInvalidArgsForUpdateStateOlderThan() {
        assertThrows(NullPointerException.class, () -> queue.updateStateOlderThan(null, MessageState.ACCEPTED, 1, LocalDateTime.now()));
        assertThrows(NullPointerException.class, () -> queue.updateStateOlderThan(MessageState.SENT, null, 1, LocalDateTime.now()));
        assertThrows(NullPointerException.class, () -> queue.updateStateOlderThan(MessageState.SENT, MessageState.ACCEPTED, 1, null));
        assertThrows(IllegalArgumentException.class, () -> queue.updateStateOlderThan(MessageState.SENT, MessageState.ACCEPTED, 0, LocalDateTime.now()));
    }

    public static class Configuration {

        @Bean
        public DataSource dataSource() {
            SimpleDriverDataSource ds = new SimpleDriverDataSource();

            ds.setDriverClass(Driver.class);
            ds.setUrl(postgreSQLContainer.getJdbcUrl());
            ds.setUsername(postgreSQLContainer.getUsername());
            ds.setPassword(postgreSQLContainer.getPassword());

            return ds;
        }

        @Bean
        public MessageQueue messageQueue(DataSource dataSource) {
            return MessageQueue.builder()
                    .jdbcTemplate(new NamedParameterJdbcTemplate(dataSource))
                    .schema(SCHEMA_NAME)
                    .queueName(QUEUE_NAME)
                    .enableCleaner(10, 10, 7)
                    .build();
        }
    }
}
