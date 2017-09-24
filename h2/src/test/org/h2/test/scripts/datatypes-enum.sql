----------------
--- ENUM support
----------------

--- ENUM basic operations

create table card (rank int, suit enum('hearts', 'clubs', 'spades'));
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts');
> update count: 2

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

select * from card;
> RANK SUIT
> ---- ------
> 0    clubs
> 3    hearts

select * from card order by suit;
> RANK SUIT
> ---- ------
> 3    hearts
> 0    clubs

insert into card (rank, suit) values (8, 'diamonds'), (10, 'clubs'), (7, 'hearts');
> update count: 3

select suit, count(rank) from card group by suit order by suit, count(rank);
> SUIT     COUNT(RANK)
> -------- -----------
> hearts   2
> clubs    2
> diamonds 1

select rank from card where suit = 'diamonds';
> RANK
> ----
> 8

--- ENUM integer-based operations

select rank from card where suit = 1;
> RANK
> ----
> 0
> 10

insert into card (rank, suit) values(5, 2);
> update count: 1

select * from card where rank = 5;
> RANK SUIT
> ---- ------
> 5    spades

--- ENUM edge cases

insert into card (rank, suit) values(6, ' ');
> exception

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', 'clubs');
> exception

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', '');
> exception

drop table card;
> ok

--- ENUM as custom user data type

create type CARD_SUIT as enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

create table card (rank int, suit CARD_SUIT);
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts');
> update count: 2

select * from card;
> RANK SUIT
> ---- ------
> 0    clubs
> 3    hearts

drop table card;
> ok

drop type CARD_SUIT;
> ok

--- ENUM in primary key with another column
create type CARD_SUIT as enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

create table card (rank int, suit CARD_SUIT, primary key(rank, suit));
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts'), (1, 'clubs');
> update count: 3

insert into card (rank, suit) values (0, 'clubs');
> exception

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1

drop table card;
> ok

drop type CARD_SUIT;
> ok

--- ENUM with index
create type CARD_SUIT as enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

create table card (rank int, suit CARD_SUIT, primary key(rank, suit));
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts'), (1, 'clubs');
> update count: 3

create index idx_card_suite on card(`suit`);

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1

select rank from card where suit in ('clubs');
> RANK
> ----
> 0
> 1

drop table card;
> ok

drop type CARD_SUIT;
> ok

