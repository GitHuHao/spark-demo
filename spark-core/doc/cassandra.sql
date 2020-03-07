-- 登录
cqlsh --color hadoop01

-- 查看键空间
desc keyspaces;

-- 创建键空间
---- SimpleStrategy 副本存放普通拓扑策略
---- replication_factor 副本数
create
keyspace test WITH replication={'class':'SimpleStrategy','replication_factor':2};

alter
keyspace test with replication = {'class':'SimpleStrategy','replication_factor':3};

drop
keyspace test;

-- 使使用指定空间
use test;


-- 常规数据类型
-- 数据类型	常量	描述
-- ascii	strings	表示ASCII字符串
-- bigint	bigint	表示64位有符号长
-- blob	blobs	表示任意字节
-- Boolean	booleans	表示true或false
-- counter	integers	表示计数器列
-- decimal	integers, floats	表示变量精度十进制
-- double	integers	表示64位IEEE-754浮点
-- float	integers, floats	表示32位IEEE-754浮点
-- inet	strings	表示一个IP地址，IPv4或IPv6
-- int	integers	表示32位有符号整数
-- text	strings	表示UTF8编码的字符串
-- timestamp	integers, strings	表示时间戳
-- timeuuid	uuids	表示类型1 UUID
-- uuid	uuids	表示类型1或类型4 UUID
-- varchar	strings	表示uTF8编码的字符串
-- varint	integers	表示任意精度整数

-- 集合类型
-- 集合	描述
-- list	列表是一个或多个有序元素的集合。
-- map	地图是键值对的集合。
-- set	集合是一个或多个元素的集合。

-- 创建表
create table emp
(
    emp_id   int primary key,
    emp_city text,
    emp_name text,
    emp_phone varint, -- 任意精度整数
    emp_sal varint
);

-- 表添加列
alter table emp
    add emp_email text;

-- 表删除列
alter table emp
    drop emp_email;

-- 删除表
drop table emp;

-- 添加索引
create index emp_emp_sal_idx on emp (emp_sal);

-- 删除索引
drop index emp_emp_sal_idx;

-- 查看建表语句
desc table emp;

insert into emp(emp_id, emp_city, emp_name, emp_phone, emp_sal)
values (1, 'Hyderabad', 'ram', 9848022338, 50000);
insert into emp(emp_id, emp_city, emp_name, emp_phone, emp_sal)
values (2, 'Delhi', 'robin', 9848022339, 50000);
insert into emp(emp_id, emp_city, emp_name, emp_phone, emp_sal)
values (3, 'Pune', 'rajeev', 9848022331, 30000);
insert into emp(emp_id, emp_city, emp_name, emp_phone, emp_sal)
values (4, 'Chennai', 'rahman', 9848022330, 50000);

-- 更新（不支持 set emp_sal = emp_sal+ 10000）
update emp
set emp_sal = 10000
where emp_id = 1;

delete
from emp
where emp_id = 1;

-- 创建列表类型的表
create table data
(
    name text primary key,
    email list < text >
);

-- 列表字段插入数据
insert into data(name, email)
values ('ramu', ['abc@gmail.com', 'cba@yahoo.com']);

-- 列表字段添加元素、删除元素
update data
set email = email + ['xyz@tutorialspoint,com']
where name ='ramu';
update data
set email = email - ['xyz@tutorialspoint,com']
where name ='ramu';
-- 替换指定位置元素
update data
set email[1] = 'cba@yahoo.com'
where name = 'ramu';

-- 床架集合类型字段表
create table data2
(
    name  text primary key,
    phone set<varint>
);
insert into data2(name, phone)
values ('rahman', {9000920, 9000921});
-- 集合添加元素(自动去重)
update data2
set phone = phone + {9000922, 9000923}
where name ='rahman';
update data2
set phone = phone - {9000922}
where name ='rahman';

-- map类型字段表
create table data3
(
    name text primary key,
    address map< text,
    text>
);

-- 插入
insert into data3(name, address)
values ('robin', {'home':'hyderabad','office':'Delhi'});

update data3
set address = address + {'office':'mumbaiu'}
where name = 'robin';

-- 清空表
truncate emp;

-- capture 捕捉指定目录采集数据到文件(表格式)
capture '/opt/softwares/cassandra-3.11.5/temp/emp.csv';
select *
from emp;
capture off;

-- 查看一致性级别
constency

-- copy 拷贝表指定字段到文件(csv 格式)
copy emp(emp_city,emp_name) to '/opt/softwares/cassandra-3.11.5/temp/emp.copy'

-- 查看集群信息
desc cluster

-- 查看键空间
    desc keyspaces;

-- 查看键空间的所有表
desc tables;

-- 自定义类型
create
type card_details(
  num int,
  pin int,
  name text,
  cvv int,
  phone set<int>,
  mail text
);

-- 查看自定义类型
desc type card_details;

-- 列举所有自定义类型
desc types;

-- 扩展输出(行转列)
expand on;
expand off;

-- 查看当前连接节点主机名，查看版本
show host;
show version;

-- 执行 sql 脚本，输出到命令行
vim emp.sql
--------------------
use test;
expand on;
select *
from emp;
expand off;
--------------------
source '/opt/softwares/cassandra-3.11.5/temp/emp.sql';

-- 批处理 (只能包含 ddl语句，且开头必须存在缩进)
create
keyspace simple with replication = {'class':'SimpleStrategy','replication_factor':3};
use simple;
create table emp
(
    emp_id   int primary key,
    emp_city text,
    emp_name text,
    emp_phone varint,
    emp_sal varint
);

begin batch
insert into emp(emp_id, emp_city, emp_name, emp_phone, emp_sal)
values (1, 'Hyderabad', 'ram', 9848022338, 50000);
insert into emp(emp_id, emp_city, emp_name, emp_phone, emp_sal)
values (2, 'Delhi', 'robin', 9848022339, 50000);
insert into emp(emp_id, emp_city, emp_name, emp_phone, emp_sal)
values (3, 'Pune', 'rajeev', 9848022331, 30000);
insert into emp(emp_id, emp_city, emp_name, emp_phone, emp_sal)
values (4, 'Chennai', 'rahman', 9848022330, 50000);
apply batch;



select *
from (select time_bucket,
             SUM(if(event = 'income_order' and ((event_dot_income_order_dot_recharge_status in (0))),
                    event_dot_income_order_dot_recharge_money, null)) as A
      from (select from_unixtime(cast(time / 1000 + 8 * 3600 as bigint), 'yyyy-MM-dd') as time_bucket,
                   distinct_id                                                         as event_dot_Anything_dot_distinct_id,
                   recharge_status                                                     as event_dot_income_order_dot_recharge_status,
                   event,
                   recharge_money                                                      as event_dot_income_order_dot_recharge_money
            from liveme_dwd_event
            where dt >= '2020-02-02'
              and dt <= '2020-02-06'
              and ((event = 'income_order' and ((recharge_status in (0)))))) driven
               left outer join (select distinct_id, count(distinct consume_type) as agg0
                                from liveme_dwd_event
                                where event = 'consume_log'
                                  and dt >= '2020-02-05'
                                  and dt <= '2020-02-06'
                                  and ((consume_type in (0)))
                                group by distinct_id
                                having (agg0 > 100)) event0
                               on event0.distinct_id = driven.event_dot_Anything_dot_distinct_id
      where (event0.distinct_id is not null)
      group by time_bucket) temp;





