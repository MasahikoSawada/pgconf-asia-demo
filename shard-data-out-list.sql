select split_part(version(), ' ', 2) = '11devel' as pg11; \gset

analyze;
\if :pg11
set enable_partition_wise_join to on;
set enable_partition_wise_agg to on;
\endif

alter server "shard1" options (add use_remote_estimate 'on');
alter server "shard2" options (add use_remote_estimate 'on');
alter server "shard3" options (add use_remote_estimate 'on');
alter server "shard4" options (add use_remote_estimate 'on');

\if :pg11
set enable_mergejoin to off;
set enable_hashjoin to off;
set enable_nestloop to off;
\endif

explain analyze verbose select to_continent, count(*) from flight_bookings f left join hotel_bookings h on (f.to_continent = h.continent) where h.booked_at > '2017-10-01' group by to_continent;
