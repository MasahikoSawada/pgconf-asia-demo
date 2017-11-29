--
-- Warming up
--

explain analyze select * from flight_bookings;
explain analyze select * from hotel_bookings;
explain analyze select * from flight_bookings where user_id = 100 and to_city = 'Tokyo';
explain analyze select * from flight_bookings where user_id = 100;
explain analyze select * from flight_bookings where to_city = 'Tokyo';

--
-- JOIN
--

-- disable partition-wise planning and push-down to remote servers
\! echo "Next: disable use_remote_estimate"
\! read
\! clear
reset enable_partition_wise_join;
alter server "shard1" options (add use_remote_estimate 'off');
alter server "shard2" options (add use_remote_estimate 'off');
alter server "shard3" options (add use_remote_estimate 'off');
alter server "shard4" options (add use_remote_estimate 'off');
alter server "shard1" options (set use_remote_estimate 'off');
alter server "shard2" options (set use_remote_estimate 'off');
alter server "shard3" options (set use_remote_estimate 'off');
alter server "shard4" options (set use_remote_estimate 'off');

-- no join pushdown
\! echo "Next: no join pushdown"
\! read
\! clear
explain analyze select * from flight_bookings f left join hotel_bookings h on (f.user_id = h.user_id and f.to_city = h.city_name);

-- first enable partition-wise join
\! echo "Next: first enable paritition-wise join"
\! read
\! clear
set enable_partition_wise_join to on;

-- partition wise join, but join still happens locally
\! echo "Next: partition wise join, but join still happens locally"
\! read
\! clear
explain analyze select * from flight_bookings f left join hotel_bookings h on (f.user_id = h.user_id and f.to_city = h.city_name);

-- enable remote join
\! echo "Next: enable remote join"
\! read
\! clear
alter server "shard1" options (set use_remote_estimate 'on');
alter server "shard2" options (set use_remote_estimate 'on');
alter server "shard3" options (set use_remote_estimate 'on');
alter server "shard4" options (set use_remote_estimate 'on');

-- partition-wise join with join pushed down to remote side
\! echo "Next: partition-wise join with join pushed down"
\! read
\! clear
explain analyze select * from flight_bookings f left join hotel_bookings h on (f.user_id = h.user_id and f.to_city = h.city_name);

--
-- AGGREGATE and GROUPING
--

-- disable partition-wise planning and push-down to remote servers
\! echo "Next: diable parition-wise planning"
\! read
\! clear
reset enable_partition_wise_agg;

-- no aggregate/grouping push-down without partition-wise planning
\! echo "Next: no aggreate/grouping pushdown"
\! read
\! clear
explain analyze select count(*) from flight_bookings;
explain analyze select user_id, to_city, count(*) from flight_bookings group by user_id, to_city;

-- enable partition-wise aggregation; push-down should occur too
\! echo "Next: partition-wise aggreation with push down"
\! read
\! clear


\! echo "Next: partition-wise aggregation"
\! read
\! clear
set enable_partition_wise_agg to on;
explain analyze select count(*) from flight_bookings;
explain analyze select user_id, to_city, count(*) from flight_bookings group by user_id, to_city;

--
-- JOIN, GROUPING, AGGREGATION
--
\! echo "Next: "
\! read
\! clear
explain analyze select count(*) from flight_bookings f left join hotel_bookings h on (f.user_id = h.user_id and f.to_city = h.city_name);
explain analyze select f.user_id, to_city, count(*) from flight_bookings f left join hotel_bookings h on (f.user_id = h.user_id and f.to_city = h.city_name) group by f.user_id, to_city;
