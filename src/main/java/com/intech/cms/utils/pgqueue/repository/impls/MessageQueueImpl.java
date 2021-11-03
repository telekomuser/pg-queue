package com.intech.cms.utils.pgqueue.repository.impls;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import com.intech.cms.utils.pgqueue.model.Message;
import com.intech.cms.utils.pgqueue.model.MessageState;
import com.intech.cms.utils.pgqueue.repository.IMessageQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
//TODO move all sql to templates
public class MessageQueueImpl implements IMessageQueue, PGNotificationListener {
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    private final String schema;
    private final String queueName;
    private final String tableName;

    private final Lock lock;
    private final Condition newMessageEvent;

    private final RowMapper<Message> messageMapper = (rs, rowNum) -> Message.builder()
            .offset(rs.getLong("id"))
            .dateAdded(rs.getTimestamp("date_added").toLocalDateTime())
            .state(MessageState.getById(rs.getInt("state")))
            .consumerId(rs.getString("consumer_id"))
            .payload(rs.getString("payload"))
            .build();

    protected MessageQueueImpl(NamedParameterJdbcTemplate jdbcTemplate,
                               PlatformTransactionManager transactionManager,
                               String schema,
                               String queueName) throws SQLException {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.schema = schema;
        this.queueName = queueName;
        this.tableName = String.format("%s.%s", schema, queueName);
        this.lock = new ReentrantLock();
        this.newMessageEvent = this.lock.newCondition();
        addListener(this);
    }

    protected void init() {
        createSchemaIfNotExists();
        createTableIfNotExists();
    }

    @Override
    public long put(Message message) {
        String sqlTemplate = "INSERT INTO %s (date_added, state, payload) VALUES (:date_added, :state, :payload)";
        String sql = String.format(sqlTemplate, tableName);

        SqlParameterSource params = new MapSqlParameterSource()
                .addValue("date_added", message.getDateAdded())
                .addValue("state", message.getState().getId())
                .addValue("payload", message.getPayload());
        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcTemplate.update(sql, params, keyHolder, new String[]{"id"});
        return Objects.requireNonNull(keyHolder.getKeyAs(Long.class));
    }

    @Override
    public List<Message> findNext(String consumerId, int limit) throws InterruptedException {
        List<Message> messages;

        while ((messages = selectAndUpdateState(consumerId, limit)).isEmpty()) {
            try {
                lock.lock();
                log.debug("awaiting for a new messages...");
                newMessageEvent.await();
            } finally {
                lock.unlock();
            }
        }

        return messages;
    }

    @Override
    public List<Message> findNextOrEmpty(String consumerId, int limit) {
        return selectAndUpdateState(consumerId, limit);
    }

    private List<Message> selectAndUpdateState(String consumerId, int limit) {
        String sqlTemplate = "SELECT * FROM %s WHERE state = 1 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT %d";
        String sql = String.format(sqlTemplate, tableName, limit);

        return transactionTemplate.execute(status ->
                jdbcTemplate.query(sql, Collections.emptyMap(), messageMapper).stream()
                        .map(message -> markAsSentTo(message, consumerId))
                        .collect(Collectors.toList())
        );
    }

    @Override
    public void commitAllConsumedMessages(String consumerId) {
        String sql = String.format("UPDATE %s SET state = 3 WHERE consumer_id = :id AND state = 2", tableName);
        jdbcTemplate.update(sql, Collections.singletonMap("id", consumerId));
        log.debug("Commit messages for consumer with id = {}", consumerId);
    }

    @Override
    public void resetAllUncommittedMessages(String consumerId) {
        String sql = String.format("UPDATE %s SET state = 1 WHERE consumer_id = :id AND state = 2", tableName);
        jdbcTemplate.update(sql, Collections.singletonMap("id", consumerId));
        log.debug("Reset uncommitted messages for consumer with id = {}", consumerId);
    }

    @Override
    public void notification(int processId, String channelName, String payload) {
        log.debug("got new notification from PID = {}, channel = {} with payload = {}", processId, channelName, payload);
        try {
            this.lock.lock();
            this.newMessageEvent.signal();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void closed() {
        log.debug("closed in listener");
        //TODO
    }

    private void createSchemaIfNotExists() {
        //TODO
    }

    private void createTableIfNotExists() {
        //TODO
    }

    private Message markAsSentTo(Message message, String consumerId) {
        String sqlTemplate = "UPDATE %s SET state = :state, consumer_id = :consumer_id WHERE id = :id";
        String sql = String.format(sqlTemplate, tableName);

        SqlParameterSource params = new MapSqlParameterSource()
                .addValue("state", MessageState.SENT.getId())
                .addValue("consumer_id", consumerId)
                .addValue("id", message.getOffset());

        jdbcTemplate.update(sql, params);
        return Message.builder()
                .offset(message.getOffset())
                .dateAdded(message.getDateAdded())
                .state(MessageState.SENT)
                .consumerId(consumerId)
                .payload(message.getPayload())
                .build();
    }

    private void addListener(PGNotificationListener listener) throws SQLException {
        PGConnection connection = getDataSource().getConnection().unwrap(PGConnection.class);
        connection.addNotificationListener(this.queueName, listener);
        Statement statement = connection.createStatement();
        statement.execute(String.format("LISTEN %s", this.queueName));
        statement.close();
        log.debug("subscribed to channel: {}", this.queueName);
    }

    private DataSource getDataSource() {
        return Objects.requireNonNull(jdbcTemplate.getJdbcTemplate().getDataSource());
    }
}
