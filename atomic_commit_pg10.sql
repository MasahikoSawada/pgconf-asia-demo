select * from hotel_bookings where user_id = 1892;
\! read

-- Enable fault injection on shared1
\c postgres masahiko localhost 3331
alter system set pg_simula.enabled to 'on';
select pg_reload_conf();

-- Back to coordinator
\c postgres masahiko localhost 3330

-- =# select * from hotel_bookings where user_id = 1892;
--   id  | user_id |      booked_at      | city_name | continent | flight_id
-- ------+---------+---------------------+-----------+-----------+-----------
--  7318 |    1892 | 2017-06-07 05:02:41 | Mumbai    | Asia      |     35730
--   (1 row)
-- The above data is in shard1(Asia), and will be moved to shard2(Europe).
--
-- In PG10, the data is deleted from shard2 and committed, and then transaction fails on shard3
BEGIN;
WITH old as (
DELETE FROM hotel_bookings1 where continent = 'Asia'  and user_id = 1892 returning *) -- delete data from shard1
INSERT INTO hotel_bookings2 SELECT id, user_id, booked_at, 'Europe', 'Moscow', flight_id FROM old; -- insert it to shard2
COMMIT;

\c postgres masahiko localhost 3331
alter system set pg_simula.enabled to 'off';
select pg_reload_conf();

-- Back to coordinator
\c postgres masahiko localhost 3330
\! read
select * from hotel_bookings where user_id = 1892; -- check!!

-- sample new data for INSERT
-- BEGIN;
-- insert into hotel_bookings values (100, 100, current_timestamp, 'Asia', 'Tokyo', 100); -- shard2
-- insert into hotel_bookings values (100, 100, current_timestamp, 'Europe', 'London', 100); --shard3
-- COMMIT;
