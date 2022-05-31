package com.intech.cms.utils.pgqueue.repository.impls;

import com.intech.cms.utils.pgqueue.model.Message;
import com.intech.cms.utils.pgqueue.model.MessageState;
import com.intech.cms.utils.pgqueue.repository.MessageQueue;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class MessageQueueImpl implements MessageQueue {
    private static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS %s";

    private static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS %s ( " +
            "    id                bigserial PRIMARY KEY, " +
            "    date_added        timestamp NOT NULL, " +
            "    state             smallint  NOT NULL, " +
            "    state_updated_at  timestamp NOT NULL, " +
            "    consumer_id       text, " +
            "    payload           text      NOT NULL " +
            ")";

    private static final RowMapper<Message> MESSAGE_MAPPER = (rs, rowNum) -> Message.builder()
            .offset(rs.getLong("id"))
            .dateAdded(rs.getTimestamp("date_added").toLocalDateTime())
            .state(MessageState.getById(rs.getInt("state")))
            .stateUpdatedAt(rs.getTimestamp("state_updated_at").toLocalDateTime())
            .consumerId(rs.getString("consumer_id"))
            .payload(rs.getString("payload"))
            .build();

    private final String queueName;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Map<QueryType, String> sqlCache;
    private final Optional<Cleaner> cleaner;

    protected MessageQueueImpl(NamedParameterJdbcTemplate jdbcTemplate,
                               String schema,
                               String queueName,
                               int cleanInterval,
                               int cleanBatchSize,
                               int messageAge) {
        this.jdbcTemplate = jdbcTemplate;
        this.queueName = queueName;

        String tableName = String.format("%s.%s", schema, queueName);
        createSchemaIfNotExists(schema);
        createTableIfNotExists(tableName);

        this.sqlCache = prepareSqlCache(tableName);

        this.cleaner = Optional.of(cleanInterval)
                .filter(interval -> interval > 0)
                .map(interval ->
                        new Cleaner(
                                String.format("%s-message-cleaner", queueName.replaceAll("_", "-")),
                                interval,
                                cleanBatchSize,
                                messageAge
                        )
                );
        this.cleaner.ifPresent(Cleaner::start);
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public boolean isMessagesExistsForState(MessageState state) {
        Objects.requireNonNull(state);
        String sql = getSqlFromCache(QueryType.IS_EXISTS);
        Map<String, Integer> params = Collections.singletonMap("state", state.getId());

        return Optional.ofNullable(jdbcTemplate.queryForObject(sql, params, Boolean.class))
                .orElse(false);
    }

    @Override
    public Message put(String payload) {
        Objects.requireNonNull(payload);
        String sql = getSqlFromCache(QueryType.SAVE);

        SqlParameterSource params = new MapSqlParameterSource()
                .addValue("date_added", LocalDateTime.now())
                .addValue("state", MessageState.ACCEPTED.getId())
                .addValue("state_updated_at", LocalDateTime.now())
                .addValue("payload", payload);

        return jdbcTemplate.queryForObject(sql, params, MESSAGE_MAPPER);
    }

    @Override
    public List<Message> selectByStateAndUpdate(MessageState currentState, int limit, MessageState newState, String consumerId) {
        Objects.requireNonNull(currentState);
        Objects.requireNonNull(newState);
        Objects.requireNonNull(consumerId);

        if (limit <= 0) {
            throw new IllegalArgumentException("limit must have positive value");
        }

        if (currentState == newState) {
            throw new IllegalArgumentException("states should be different");
        }

        String sql = getSqlFromCache(QueryType.SELECT_AND_UPDATE);
        SqlParameterSource params = new MapSqlParameterSource()
                .addValue("current_state", currentState.getId())
                .addValue("limit", limit)
                .addValue("new_state", newState.getId())
                .addValue("consumer_id", consumerId);

        return jdbcTemplate.query(sql, params, MESSAGE_MAPPER);
    }

    @Override
    public int updateStateByConsumerId(String consumerId, MessageState currentState, MessageState newState) {
        Objects.requireNonNull(consumerId);
        Objects.requireNonNull(currentState);
        Objects.requireNonNull(newState);

        String sql = getSqlFromCache(QueryType.UPDATE_STATE);
        SqlParameterSource params = new MapSqlParameterSource()
                .addValue("consumer_id", consumerId)
                .addValue("current_state", currentState.getId())
                .addValue("new_state", newState.getId());

        return jdbcTemplate.update(sql, params);
    }

    @Override
    public int updateStateOlderThan(MessageState currentState, MessageState newState, int limit, LocalDateTime dateTime) {
        Objects.requireNonNull(currentState);
        Objects.requireNonNull(newState);
        Objects.requireNonNull(dateTime);

        if (limit <= 0) {
            throw new IllegalArgumentException("limit must have positive value");
        }

        String sql = getSqlFromCache(QueryType.UPDATE_STATE_OLDER);
        SqlParameterSource params = new MapSqlParameterSource()
                .addValue("current_state", currentState.getId())
                .addValue("new_state", newState.getId())
                .addValue("limit",limit)
                .addValue("date_time", dateTime);

        return jdbcTemplate.update(sql, params);

    }

    @Override
    public void close() {
        cleaner.ifPresent(Cleaner::interrupt);
    }

    private int deleteDeliveredMessagesOlderThan(int limit, LocalDateTime dateTime) {
        String sql = getSqlFromCache(QueryType.DELETE_OLD);
        SqlParameterSource params = new MapSqlParameterSource()
                .addValue("state", MessageState.DELIVERED.getId())
                .addValue("limit", limit)
                .addValue("date_time", dateTime);

        return jdbcTemplate.update(sql, params);
    }

    private void createSchemaIfNotExists(String schema) {
        jdbcTemplate.getJdbcTemplate().execute(String.format(CREATE_SCHEMA_SQL, schema));
    }

    private void createTableIfNotExists(String tableName) {
        jdbcTemplate.getJdbcTemplate().execute(String.format(CREATE_TABLE_SQL, tableName));
    }

    private Map<QueryType, String> prepareSqlCache(String tableName) {
        return Stream.of(QueryType.values())
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                queryType -> String.format(queryType.getTemplate(), tableName)
                        )
                );
    }

    private String getSqlFromCache(QueryType queryType) {
        return Optional.ofNullable(sqlCache.get(queryType))
                .orElseThrow(IllegalStateException::new);
    }

    @Getter
    @RequiredArgsConstructor
    private enum QueryType {
        IS_EXISTS("SELECT exists(SELECT 1 FROM %s WHERE state = :state)"),

        SAVE("INSERT INTO %s (date_added, state, state_updated_at, payload) VALUES (:date_added, :state, :state_updated_at, :payload) RETURNING *"),

        SELECT_AND_UPDATE("UPDATE %1$s SET state = :new_state, consumer_id = :consumer_id, state_updated_at = now() " +
                "WHERE id IN (SELECT id FROM %1$s WHERE state = :current_state ORDER BY id FOR UPDATE SKIP LOCKED LIMIT :limit)" +
                "RETURNING *"),

        UPDATE_STATE("UPDATE %s SET state = :new_state, state_updated_at = now() WHERE consumer_id = :consumer_id AND state = :current_state"),

        DELETE_OLD("DELETE FROM %1$s " +
                "WHERE id IN (SELECT id FROM %1$s WHERE state = :state AND state_updated_at <= :date_time ORDER BY state_updated_at LIMIT :limit)"),

        UPDATE_STATE_OLDER("UPDATE %1$s SET state = :new_state, state_updated_at = now() " +
                " WHERE id IN (SELECT id FROM %1$s WHERE state = :current_state AND state_updated_at <= :date_time ORDER BY id FOR UPDATE SKIP LOCKED LIMIT :limit)");

        private final String template;
    }

    private class Cleaner extends Thread {

        private final int cleanInterval;
        private final int cleanBatchSize;
        private final int messageAge;

        public Cleaner(String name, int cleanInterval, int cleanBatchSize, int messageAge) {
            super(name);
            this.cleanInterval = cleanInterval;
            this.cleanBatchSize = cleanBatchSize;
            this.messageAge = messageAge;
            setDaemon(true);
        }

        @Override
        public void run() {
            log.info("{} started...", getName());

            while (!isInterrupted()) {
                try {
                    Thread.sleep(cleanInterval * 1000L);
                    int deleted = deleteDeliveredMessagesOlderThan(cleanBatchSize, LocalDateTime.now().minusDays(messageAge));
                    log.info("deleted {} messages", deleted);
                } catch (InterruptedException e) {
                    log.info("{} was interrupted, stopping...", getName());
                    break;
                } catch (Exception e) {
                    log.error("", e);
                }
            }

            log.info("...{} stopped", getName());
        }
    }

}
