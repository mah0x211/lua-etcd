local Etcd = require('etcd.luasocket');

local cli = ifNil( Etcd.new() );
local res;

res = ifNil( cli:rmdir( '/path/to/dir', true ) );
res = ifNil( cli:mkdir( '/path/to/dir' ) );
ifNotEqual( res.status, 201 );

res = ifNil( cli:readdir( '/', true ) );
ifNotEqual( res.status, 200 );

res = ifNil( cli:rmdir( '/path/to/dir', true ) );
ifNotEqual( res.status, 200 );

