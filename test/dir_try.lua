local Etcd = require('etcd.luasocket');

local cli = ifNil( Etcd.new() );
local ttl = 2;
local res;

res = ifNil( cli:rmdir( '/path', true ) );
res = ifNil( cli:mkdir( '/path/to/dir' ) );
ifNotEqual( res.status, 201 );

res = ifNil( cli:readdir( '/', true ) );
ifNotEqual( res.status, 200 );

res = ifNil( cli:rmdir( '/path', true ) );
ifNotEqual( res.status, 200 );

res = ifNil( cli:readdir( '/path/to/dir', true ) );
ifNotEqual( res.status, 404 );

res = ifNil( cli:mkdir( '/path', ttl ) );
ifNotEqual( res.status, 201 );
sleep( ttl + 1 );
res = ifNil( cli:readdir( '/path', true ) );
ifNotEqual( res.status, 404 );

-- mkdir if not exists
res = ifNil( cli:mkdirnx( '/path/to/dir' ) );
ifNotEqual( res.status, 201 );
res = ifNil( cli:mkdirnx( '/path/to/dir' ) );
ifNotEqual( res.status, 412 );

-- cleanup
ifNil( cli:rmdir( '/path', true ) );
