CREATE TABLE index (vid int, rlist int[]);
INSERT INTO index VALUES (1, ARRAY[1,2]), (2, ARRAY[1,2,3]), (3, ARRAY[1,4,5]), (4, ARRAY[1,4,5,6]);

create table datatable(rid serial primary key, employee_id int, age int, salary float);
insert into datatable(employee_id, age, salary) values (66000001,25,6500), (66000002,30,7500), (66000003,25,7000),(66000004,30,8000), (66000002,30,10000),  (66000005, 35,10000 );

create table version(vid int primary key, num_records int, parent integer[], children integer[], create_time timestamp, commit_time timestamp, commit_msg text);
insert into version values (1, 3, '{-1}', '{2,3}', '2016-03-15 00:00:49.387281-05', '2016-03-15 00:00:49.387281-05', 'init version'),
 (2, 4, '{1}', '{4}', '2016-03-15 00:00:49.387281-05', '2016-03-15 00:00:49.387281-05', 'checkout from v1'), (3, 4, '{1}', '{4}', '2016-03-15 00:00:49.387281-05',  '2016-03-15 00:00:49.387281-05', 'checkout from v1'), (4, 4, '{2,3}', '{}', '2016-03-15 00:00:49.387281-05',  '2016-03-15 00:00:49.387281-05', 'cehckout from v2 and v3');
