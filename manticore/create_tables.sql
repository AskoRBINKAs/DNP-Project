CREATE TABLE logs (
    id bigint,
    `timestamp` timestamp,
    source_ip string,
    source_tag string,
    data json
);