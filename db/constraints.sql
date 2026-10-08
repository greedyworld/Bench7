-- Secondary indexes, unique constraints and foreign keys. Applied by seed.sh
-- after the bulk load (faster than maintaining them row by row). They are all
-- present during benchmark runs, so writes pay the real FK/index cost.
SET maintenance_work_mem = '512MB';
SET max_parallel_maintenance_workers = 2;

CREATE UNIQUE INDEX IF NOT EXISTS users_username ON users (username);
CREATE UNIQUE INDEX IF NOT EXISTS users_email ON users (email);

-- public feed: newest public, not deleted
CREATE INDEX IF NOT EXISTS posts_public ON posts (id) WHERE visibility = 0 AND deleted_at IS NULL;
-- profile / private feed
CREATE INDEX IF NOT EXISTS posts_user ON posts (user_id, id);
CREATE INDEX IF NOT EXISTS posts_reply_to ON posts (reply_to_id) WHERE reply_to_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS conversations_pair ON conversations (user_a_id, user_b_id);
CREATE INDEX IF NOT EXISTS conversations_b ON conversations (user_b_id);

-- inbox
CREATE INDEX IF NOT EXISTS messages_recipient ON messages (recipient_id, id);
CREATE INDEX IF NOT EXISTS messages_conversation ON messages (conversation_id, id);

CREATE INDEX IF NOT EXISTS likes_post ON likes (post_id);

ALTER TABLE posts ADD CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES users (id);
ALTER TABLE posts ADD CONSTRAINT posts_reply_to_fk FOREIGN KEY (reply_to_id) REFERENCES posts (id);
ALTER TABLE conversations ADD CONSTRAINT conversations_a_fk FOREIGN KEY (user_a_id) REFERENCES users (id);
ALTER TABLE conversations ADD CONSTRAINT conversations_b_fk FOREIGN KEY (user_b_id) REFERENCES users (id);
ALTER TABLE messages ADD CONSTRAINT messages_conversation_fk FOREIGN KEY (conversation_id) REFERENCES conversations (id);
ALTER TABLE messages ADD CONSTRAINT messages_sender_fk FOREIGN KEY (sender_id) REFERENCES users (id);
ALTER TABLE messages ADD CONSTRAINT messages_recipient_fk FOREIGN KEY (recipient_id) REFERENCES users (id);
ALTER TABLE likes ADD CONSTRAINT likes_user_fk FOREIGN KEY (user_id) REFERENCES users (id);
ALTER TABLE likes ADD CONSTRAINT likes_post_fk FOREIGN KEY (post_id) REFERENCES posts (id);
