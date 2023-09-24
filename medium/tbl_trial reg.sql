create table tbl_trialRegister(

transaction_id int PRIMARY KEY,
account_id int,
subscription_id int,
security_id int,
amount int,
first_name varchar(100),
last_name varchar(100),
subscribe_newsletter int,
subscription_type varchar(100),
register_date date


);
