CREATE TABLE maps(
ID int,
KEY varchar(50) NOT NULL,
VALUE varchar(50) NOT NULL
);

IceFireDB-SQLite
select * from maps;

select * from maps order by id desc;

insert into `maps`(`id`,`key`,`value`) values(1, 'a', 'aa'), (2,'b','bb'),(3,'c','ccccc');

update `maps` set value='bbbbbbb' where id=2;

delete from `maps` where id=3;