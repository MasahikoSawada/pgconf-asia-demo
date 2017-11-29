select split_part(version(), ' ', 2) = '11devel' as pg11; \gset

copy users from '/home/masahiko/pgsql/demo/testdata/users.csv' (format csv);
--copy cities from '/home/masahiko/pgsql/demo/testdata/cities.csv' (format csv);

-- flight booking data
create table if not exists flight_bookings_u (like flight_bookings);
truncate flight_bookings_u;
copy flight_bookings_u from '/home/masahiko/pgsql/demo/testdata/flight_bookings.csv' (format csv);

\if :pg11
insert into flight_bookings select * from flight_bookings_u;
\else
-- PG10 dones't support the tuple routing for foreign paritions, load each table directly.
insert into flight_bookings1 select * from flight_bookings_u where to_continent in ('Asia', 'Oceania');
insert into flight_bookings2 select * from flight_bookings_u where to_continent in ('Europe', 'Africa');
insert into flight_bookings3 select * from flight_bookings_u where to_continent in ('North America');
insert into flight_bookings4 select * from flight_bookings_u where to_continent in ('South and Central America');
\endif

-- Check
select count(*) from flight_bookings;

-- hotel booking data (1/3 of flights have associated hotel booking)
create table if not exists hotel_bookings_u (like hotel_bookings);
truncate hotel_bookings_u;
copy hotel_bookings_u from '/home/masahiko/pgsql/demo/testdata/hotel_bookings.csv' (format csv);

\if :pg11
insert into hotel_bookings select * from hotel_bookings_u;
\else
-- PG10 dones't support the tuple routing for foreign paritions, load each table directly.
insert into hotel_bookings1 select * from hotel_bookings_u where continent in ('Asia', 'Oceania');
insert into hotel_bookings2 select * from hotel_bookings_u where continent in ('Europe', 'Africa');
insert into hotel_bookings3 select * from hotel_bookings_u where continent in ('North America');
insert into hotel_bookings4 select * from hotel_bookings_u where continent in ('South and Central America');
\endif

-- Check
select count(*) from hotel_bookings;

analyze;
