create schema if not exists hello_world;

--　サンプルテーブル作成
create table if not exists hello_world.greetings (
    id serial primary key,
    who text not null,
    message text not null,
    created_at timestamptz not null default now()
);

-- サンプルデータを作成
insert into hello_world.greetings (who, message) values
    ('alice', 'hello, world!'),
    ('bob', 'こんにちは、世界！'),
    ('carol', 'hola mundo');
