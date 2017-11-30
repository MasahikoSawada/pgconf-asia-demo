BEGIN;
WITH old as (
DELETE FROM hotel_bookings1 where continent = 'Asia'  and user_id = 1892 returning *) -- delete data from shard1
INSERT INTO hotel_bookings2 SELECT id, user_id, booked_at, 'Europe', 'Moscow', flight_id FROM old; -- insert it to shard2
COMMIT;
