package com.intech.cms.utils.pgqueue.repository;

import com.intech.cms.utils.pgqueue.model.Message;
import com.intech.cms.utils.pgqueue.model.MessageState;
import com.intech.cms.utils.pgqueue.repository.impls.DefaultMessageQueueBuilder;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.LocalDateTime;
import java.util.List;

public interface MessageQueue extends AutoCloseable {

    String getName();

    /**
     * Проверить наличие сообщений с указанным статусом
     *
     * @param state статус искомых сообщений
     * @return true, если сообщение имеются, иначе false
     */
    boolean isMessagesExistsForState(MessageState state);

    /**
     * Разместить новое сообщение с указанным телом в очереди
     *
     * @param payload тело сообщения
     * @return Message
     */
    Message put(String payload);

    /**
     * Получить список сообщений (не более limit) с заданным статусом.
     *
     * @param currentState текущий статус сообщений
     * @param limit        максимальное количество для получения
     * @param newState     статус, который будет у сообщений после получения
     * @param consumerId   идентификатор получателя
     * @return список Message (возможно пустой)
     */
    List<Message> selectByStateAndUpdate(MessageState currentState, int limit, MessageState newState, String consumerId);

    /**
     * Обновить статус сообщений, выданных ранее указанному получателю, имеющих указанный статус
     *
     * @param consumerId   идентификатор получателя
     * @param currentState текущий статус сообщений
     * @param newState     новый статус сообщений
     * @return количество сообщений, статус которых был обновлен
     */
    int updateStateByConsumerId(String consumerId, MessageState currentState, MessageState newState);

    int updateStateOlderThan(MessageState currentState, MessageState newState, int limit, LocalDateTime dateTime);

    static MessageQueue.Builder builder() {
        return new DefaultMessageQueueBuilder();
    }

    interface Builder {
        Builder jdbcTemplate(NamedParameterJdbcTemplate jdbcTemplate);

        Builder schema(String schema);

        Builder queueName(String queueName);

        Builder enableCleaner(int interval, int batchSize, int age);

        MessageQueue build();
    }
}
