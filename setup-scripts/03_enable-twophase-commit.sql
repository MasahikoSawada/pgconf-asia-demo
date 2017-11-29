select split_part(version(), ' ', 2) = '11devel' as pg11; \gset

\if :pg11
alter server shard1 options ( add two_phase_commit 'on');
alter server shard2 options ( add two_phase_commit 'on');
alter server shard3 options ( add two_phase_commit 'on');
alter server shard4 options ( add two_phase_commit 'on');
\endif
