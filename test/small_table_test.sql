create table datatable(rid serial primary key, employee_id int, age int, salary float);
insert into datatable(employee_id, age, salary) values (66000001,25,6500), (66000002,30,7500), (66000001,25,7000), (66000003, 40, 9000), (66000004, 35,6500 ),(66000005,28,7500 ),(66000004, 35,9000 );

create table indexTbl (vlist integer[], rlist integer[]);
insert into indexTbl values ('{1,3,5}', '{1}'), ('{1,2,3,4,5}', '{2}'), ('{2,4}', '{3,4}'), ('{3,4}', '{5}'), ('{3,4,5}','{6}');

create table version(vid int primary key, num_records int, parent integer[], children integer[], create_time timestamp, commit_time timestamp, commit_msg text);
insert into version values (1, 3, '{-1}', '{2,3,4}', '2016-03-15 00:00:49.387281-05', '2016-03-15 00:00:49.387281-05', 'test1'),
 (2, 4, '{1}', '{}', '2016-03-15 00:00:49.387281-05', '2016-03-15 00:00:49.387281-05', 'test2'), (3, 4, '{1}', '{4}', '2016-03-15 00:00:49.387281-05',  '2016-03-15 00:00:49.387281-05', 'test3'), (4, 4, '{3}', '{}', '2016-03-15 00:00:49.387281-05',  '2016-03-15 00:00:49.387281-05', 'test4');

create table mapTbl(rid serial primary key, employee_id int);
create table demo(rid serial primary key, employee_id int);





