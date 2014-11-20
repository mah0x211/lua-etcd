local Etcd = require('etcd.luasocket');

local cli = ifNil( Etcd.new() );
local res = ifNil( cli:version() );

ifNotEqual( res.status, 200 );
