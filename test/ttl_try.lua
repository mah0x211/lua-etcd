local Etcd = require('etcd.luasocket');

local cli = ifNil( Etcd.new() );
local ttl = 2;
local res;

-- dir
res = ifNil( cli:rmdir( '/path', true ) );
res = ifNil( cli:mkdir( '/path/to/dir' ) );
ifNotEqual( res.status, 201 );

res = ifNil( cli:set( '/path/to/dir/key', 'hello world' ) );
ifNotEqual( res.status, 201 );

res = ifNil( cli:setTTL( '/path', ttl ) );
ifNotEqual( res.status, 200 );
res = ifNil( cli:get( '/path/to/dir/key' ) );
ifNotEqual( res.status, 200 );

sleep( ttl + 1 );
res = ifNil( cli:readdir( '/path', true ) );
ifNotEqual( res.status, 404 );

-- key
res = ifNil( cli:set( '/path/to/key', 'hello world' ) );
ifNotEqual( res.status, 201 );
res = ifNil( cli:setTTL( '/path/to/key', ttl ) );
ifNotEqual( res.status, 200 );
sleep( ttl + 1 );
res = ifNil( cli:get( '/path/to/key' ) );
ifNotEqual( res.status, 404 );
