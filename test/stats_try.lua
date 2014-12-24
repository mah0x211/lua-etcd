ifNil( execChild('etcd') );
sleep(1);
local Etcd = require('etcd.luasocket');
local cli = ifNil( Etcd.new() );
local res;

res = ifNil( cli:statsLeader() );
ifNotEqual( res.status, 200 );

res = ifNil( cli:statsSelf() );
ifNotEqual( res.status, 200 );

res = ifNil( cli:statsStore() );
ifNotEqual( res.status, 200 );
