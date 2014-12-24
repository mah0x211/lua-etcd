ifNil( execChild('etcd') );
sleep(1);
local Etcd = require('etcd.luasocket');
local cli = ifNil( Etcd.new() );
local res = ifNil( cli:version() );

ifNotEqual( res.status, 200 );
