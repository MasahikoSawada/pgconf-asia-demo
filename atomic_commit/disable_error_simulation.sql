select split_part(version(), ' ', 2) = '11devel' as pg11; \gset

\if :pg11
\set coord_port 4440
\set shd1_port 4441
\else
\set coord_port 3330
\set shd1_port 3331
\endif

\c postgres masahiko localhost :shd1_port
alter system set pg_simula.enabled to 'off';
select pg_reload_conf();
\c postgres masahiko localhost :coord_port

