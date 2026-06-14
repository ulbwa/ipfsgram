-- migrate:up
CREATE TABLE config (
    key   text PRIMARY KEY,
    value text NOT NULL
);
INSERT INTO config (key, value) VALUES
    ('car_max_size', '15728640'),
    ('bot_api_url', 'https://api.telegram.org'),
    ('channel_warn_threshold', '0.9'),
    ('mtproto_enabled', 'false');

CREATE TABLE mtproto_credentials (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    api_id     integer NOT NULL,
    api_hash   text NOT NULL,
    active     boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX mtproto_credentials_one_active ON mtproto_credentials ((true)) WHERE active;

CREATE TABLE bots (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tg_id             bigint NOT NULL UNIQUE,
    username          text NOT NULL,
    token             text NOT NULL UNIQUE,
    active            boolean NOT NULL DEFAULT true,
    unavailable_until timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE channels (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tg_id         bigint NOT NULL UNIQUE,
    title         text NOT NULL DEFAULT '',
    message_count bigint NOT NULL DEFAULT 0,
    message_limit bigint NOT NULL DEFAULT 1000000,
    active        boolean NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE bot_channels (
    bot_id      bigint NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    channel_id  bigint NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    can_post    boolean NOT NULL DEFAULT false,
    can_read    boolean NOT NULL DEFAULT false,
    can_delete  boolean NOT NULL DEFAULT false,
    member      boolean NOT NULL DEFAULT true,
    verified_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (bot_id, channel_id)
);

CREATE TYPE car_status AS ENUM ('pending','published','no_bot_access','too_large');

CREATE TABLE cars (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channel_id  bigint NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    message_id  bigint,
    size        bigint NOT NULL,
    block_count integer NOT NULL,
    status      car_status NOT NULL DEFAULT 'pending',
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX cars_channel_idx ON cars (channel_id);
CREATE INDEX cars_status_idx ON cars (status) WHERE status = 'pending';

CREATE TABLE car_file_ids (
    car_id     bigint NOT NULL REFERENCES cars(id) ON DELETE CASCADE,
    bot_id     bigint NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    file_id    text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (car_id, bot_id)
);

CREATE TABLE blocks (
    cid     bytea PRIMARY KEY,
    car_id  bigint NOT NULL REFERENCES cars(id) ON DELETE CASCADE,
    "offset" bigint NOT NULL,
    length  integer NOT NULL
);
CREATE INDEX blocks_car_idx ON blocks (car_id);

CREATE TABLE pins (
    root_cid   bytea PRIMARY KEY,
    name       text NOT NULL DEFAULT '',
    size       bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pin_blocks (
    root_cid bytea NOT NULL REFERENCES pins(root_cid) ON DELETE CASCADE,
    cid      bytea NOT NULL,
    PRIMARY KEY (root_cid, cid)
);
CREATE INDEX pin_blocks_cid_idx ON pin_blocks (cid);

-- migrate:down
DROP TABLE pin_blocks;
DROP TABLE pins;
DROP TABLE blocks;
DROP TABLE car_file_ids;
DROP TABLE cars;
DROP TYPE car_status;
DROP TABLE bot_channels;
DROP TABLE channels;
DROP TABLE bots;
DROP TABLE mtproto_credentials;
DROP TABLE config;
