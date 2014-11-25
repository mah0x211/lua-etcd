local Etcd = require('etcd.luasocket');

local cli = ifNil( Etcd.new() );
local recursive = true;
local res;

-- cleanup
ifNil( cli:rmdir( '/path', recursive ) );

res = ifNil( cli:mkdir( '/path/to/dir' ) );
ifNotEqual( res.status, 201 );

res = ifNil( cli:readdir( '/', recursive ) );
ifNotEqual( res.status, 200 );

res = ifNil( cli:rmdir( '/path', recursive ) );
ifNotEqual( res.status, 200 );

res = ifNil( cli:readdir( '/path/to/dir' ) );
ifNotEqual( res.status, 404 );

-- mkdir if not exists
res = ifNil( cli:mkdirnx( '/path/to/dir' ) );
ifNotEqual( res.status, 201 );
res = ifNil( cli:mkdirnx( '/path/to/dir' ) );
ifNotEqual( res.status, 412 );

-- cleanup
ifNil( cli:rmdir( '/path', recursive ) );
