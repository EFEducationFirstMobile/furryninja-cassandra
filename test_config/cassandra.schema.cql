create keyspace test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use test_keyspace;

create table book (
  key varchar,
  kind varchar,
  revision varchar,
  blob varchar,
  create_date timestamp,
  last_update timestamp,
  primary key ((kind, key),  revision)
);

create table tag (
  key varchar,
  revision varchar,
  blob varchar,
  create_date timestamp,
  last_update timestamp,
  primary key (key, revision)
);

create table imageasset (
  key varchar,
  revision varchar,
  blob varchar,
  update_token varchar,
  primary key (key, revision)
);

create table videoasset (
  key varchar,
  title varchar,
  num int,
  primary key (key)
);


create table edge (
  key varchar,
  label varchar,
  indoc varchar,
  outdoc varchar,
  create_date timestamp,
  last_update timestamp,
  primary key(indoc, outdoc, label)
);
