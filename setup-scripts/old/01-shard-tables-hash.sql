-- Coordinator
drop table if exists "flight_bookings";
drop table if exists "hotel_bookings";
drop table if exists "users";
drop table if exists "cities";
create table "hotel_bookings" (id serial, user_id int, booked_at timestamp, city_name text, continent text, flight_id int) partition by hash (user_id, city_name);
create table "flight_bookings" (id serial, user_id int, booked_at timestamp, from_city text, from_continent text, to_city text, to_continent text) partition by hash (user_id, to_city);
create table "users" (id serial, name text, age int);

-- Shard 1
\c postgres masahiko localhost 4441
drop table if exists "flight_bookings1";
drop table if exists "hotel_bookings1";
create table "hotel_bookings1" (id int, user_id int, booked_at timestamp, city_name text, continent text, flight_id int);
create table "flight_bookings1" (id int, user_id int, booked_at timestamp, from_city text, from_continent text, to_city text, to_continent text);

-- Shard 2
\c postgres masahiko localhost 4442
drop table if exists "flight_bookings2";
drop table if exists "hotel_bookings2";
create table "hotel_bookings2" (id int, user_id int, booked_at timestamp, city_name text, continent text, flight_id int);
create table "flight_bookings2" (id int, user_id int, booked_at timestamp, from_city text, from_continent text, to_city text, to_continent text);

-- Shard 3
\c postgres masahiko localhost 4443
drop table if exists "flight_bookings3";
drop table if exists "hotel_bookings3";
create table "hotel_bookings3" (id int, user_id int, booked_at timestamp, city_name text, continent text, flight_id int);
create table "flight_bookings3" (id int, user_id int, booked_at timestamp, from_city text, from_continent text, to_city text, to_continent text);

-- Shard 4
\c postgres masahiko localhost 4444
drop table if exists "flight_bookings4";
drop table if exists "hotel_bookings4";
create table "hotel_bookings4" (id int, user_id int, booked_at timestamp, city_name text, continent text, flight_id int);
create table "flight_bookings4" (id int, user_id int, booked_at timestamp, from_city text, from_continent text, to_city text, to_continent text);

-- Back to coordinator
\c postgres masahiko localhost 4440
create foreign table "flight_bookings1" partition of flight_bookings for values with (modulus 4, remainder 0) server "shard1";
create foreign table "hotel_bookings1" partition of hotel_bookings for values with (modulus 4, remainder 0) server "shard1";

create foreign table "flight_bookings2" partition of flight_bookings for values with (modulus 4, remainder 1) server "shard2";
create foreign table "hotel_bookings2" partition of hotel_bookings for values with (modulus 4, remainder 1) server "shard2";

create foreign table "flight_bookings3" partition of flight_bookings for values with (modulus 4, remainder 2) server "shard3";
create foreign table "hotel_bookings3" partition of hotel_bookings for values with (modulus 4, remainder 2) server "shard3";

create foreign table "flight_bookings4" partition of flight_bookings for values with (modulus 4, remainder 3) server "shard4";
create foreign table "hotel_bookings4" partition of hotel_bookings for values with (modulus 4, remainder 3) server "shard4";
