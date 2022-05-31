INSERT INTO queues.test (date_added, state, state_updated_at, consumer_id, payload) VALUES (now(), 1, now(), NULL, 'Test new message');
INSERT INTO queues.test (date_added, state, state_updated_at, consumer_id, payload) VALUES (now(), 2, now(), 'consumerId', 'Test sent message 1');
INSERT INTO queues.test (date_added, state, state_updated_at, consumer_id, payload) VALUES (now(), 2, now(), 'consumerId', 'Test sent message 2');
INSERT INTO queues.test (date_added, state, state_updated_at, consumer_id, payload) VALUES ('2021-12-31 23:50:01', 2, '2022-01-01 00:00:01', 'aaa', 'Test not delivered message');
INSERT INTO queues.test (date_added, state, state_updated_at, consumer_id, payload) VALUES (now(), 3, '2022-01-01 00:00:01', 'aaa', 'Test message for delete');