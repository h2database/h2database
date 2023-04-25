-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

call hash('SHA256', 'Hello', 0);
> exception INVALID_VALUE_2

call hash('SHA256', 'Hello');
>> X'185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969'

call hash('SHA256', 'Hello', 1);
>> X'185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969'

call hash('SHA256', stringtoutf8('Hello'), 1);
>> X'185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969'

CALL HASH('SHA256', 'Password', 1000);
>> X'c644a176ce920bde361ac336089b06cc2f1514dfa95ba5aabfe33f9a22d577f0'

CALL HASH('SHA256', STRINGTOUTF8('Password'), 1000);
>> X'c644a176ce920bde361ac336089b06cc2f1514dfa95ba5aabfe33f9a22d577f0'

call hash('unknown', 'Hello', 1);
> exception INVALID_VALUE_2

CALL HASH('MD5', '****** Message digest test ******', 1);
>> X'ccd7ee53b52575b5b04fcadf1637fd30'

CALL HASH('MD5', '****** Message digest test ******', 10);
>> X'b9e4b74ee3c41f646ee0ba42335efe20'

CALL HASH('SHA-1', '****** Message digest test ******', 1);
>> X'b9f28134b8c9aef59e1257eca89e3e5101234694'

CALL HASH('SHA-1', '****** Message digest test ******', 10);
>> X'e69a31beb996b59700aed3e6fbf9c29791efbc15'

CALL HASH('SHA-224', '****** Message digest test ******', 1);
>> X'7bd9bf319961cfdb7fc9351debbcc8a80143d5d0909e8cbccd8b5f0f'

CALL HASH('SHA-224', '****** Message digest test ******', 10);
>> X'6685a394158763e754332f0adec3ed43866dd0ba8f47624d0521fd1e'

CALL HASH('SHA-256', '****** Message digest test ******', 1);
>> X'4e732bc9788b0958022403dbe42b4b79bfa270f05fbe914b4ecca074635f3f5c'

CALL HASH('SHA-256', '****** Message digest test ******', 10);
>> X'93731025337904f6bc117ca5d3adc960ee2070c7a9666a5499af28546520da85'

CALL HASH('SHA-384', '****** Message digest test ******', 1);
>> X'a37baa07c0cd5bc8dbb510b3fc3fa6f5ca539c847d8ee382d1d045b405a3d43dc4a898fcc31930cf7a80e2a79af82d4e'

CALL HASH('SHA-384', '****** Message digest test ******', 10);
>> X'03cc3a769871ab13a64c387c44853efafe016180ab6ea70565924ccabe62c8884b2f2e1a53c1a79db184c112c9082bc2'

CALL HASH('SHA-512', '****** Message digest test ******', 1);
>> X'88eb2488557eaf7e4da394b6f4ba08d4c781b9f2b9c9d150195ac7f7fbee7819923476b5139abc98f252b07649ade2471be46e2625b8003d0af5a8a50ca2915f'

CALL HASH('SHA-512', '****** Message digest test ******', 10);
>> X'ab3bb7d9447f87a07379e9219c79da2e05122ff87bf25a5e553a7e44af7ac724ed91fb1fe5730d4bb584c367fc2232680f5c45b3863c6550fcf27b4473d05695'

CALL HASH('SHA3-224', '****** Message digest test ******', 1);
>> X'cb91fec022d97ed63622d382e36e336b65a806888416a549fb4db390'

CALL HASH('SHA3-224', '****** Message digest test ******', 10);
>> X'0d4dd581ed9b188341ec413988cb7c6bf15d178b151b543c91031ae6'

CALL HASH('SHA3-256', '****** Message digest test ******', 1);
>> X'91db71f65f3c5b19370e0d9fd947da52695b28c9b440a1324d11e8076643c21f'

CALL HASH('SHA3-256', '****** Message digest test ******', 10);
>> X'ed62484d8ac54550292241698dd5480de061fc23ab12e3e941a96ec7d3afd70f'

CALL HASH('SHA3-384', '****** Message digest test ******', 1);
>> X'c2d5e516ea10a82a3d3a8c5fe8838ca77d402490f33ef813be9af168fd2cdf8f6daa7e9cf79565f3987f897d4087ce26'

CALL HASH('SHA3-384', '****** Message digest test ******', 10);
>> X'9f5ac0eae232746826ea59196b455267e3aaa492047d5a2616c4a8aa325216f706dc7203fcbe71ee7e3357e0f3d93ee3'

CALL HASH('SHA3-512', '****** Message digest test ******', 1);
>> X'08811cf7409957b59bb5ba090edbef9a35c3b7a4db5d5760f15f2b14453f9cacba30b9744d4248c742aa47f3d9943cf99e7d78d1700d4ccf5bc88b394bc00603'

CALL HASH('SHA3-512', '****** Message digest test ******', 10);
>> X'37f2a9dbc6cd7a5122cc84383843566dd7195ed8d868b1c10aca2b706667c7bb0b4f00eab81d9e87b6f355e3afe0bccd57ba04aa121d0ef0c0bdea2ff8f95513'
