create table if not exists sample_table (
    id                  bigserial not null primary key,
    name                varchar(100),
    is_good             boolean,
    birth_date          date,
    first_encounter     timestamp without time zone,
    dollars             numeric(10,2)
);

insert into sample_table (name, is_good, birth_date, first_encounter, dollars) values
('alice', true, '2000-01-02', '2001-01-03 12:43:33', 12.48);