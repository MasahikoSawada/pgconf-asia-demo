analyze;
set enable_partition_wise_join to on;
alter server "shard1" options (add use_remote_estimate 'on');
alter server "shard2" options (add use_remote_estimate 'on');
alter server "shard3" options (add use_remote_estimate 'on');
alter server "shard4" options (add use_remote_estimate 'on');

--alter server "shard1" options (set fdw_tuple_cost '0.00000001');
--alter server "shard2" options (set fdw_tuple_cost '0.00000001');
--alter server "shard3" options (set fdw_tuple_cost '0.00000001');
--alter server "shard4" options (set fdw_tuple_cost '0.00000001');

--alter server "shard1" options (set fdw_startup_cost '100');
--alter server "shard2" options (set fdw_startup_cost '100');
--alter server "shard3" options (set fdw_startup_cost '100');
--alter server "shard4" options (set fdw_startup_cost '100');

--set enable_mergejoin to off;
--set enable_hashjoin to off;
--set enable_nestloop to off;


explain analyze verbose select to_continent, count(*) from flight_bookings f left join hotel_bookings h on (f.to_continent = h.continent) group by to_continent;
