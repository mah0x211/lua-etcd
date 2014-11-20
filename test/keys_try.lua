local Etcd = require('etcd.luasocket');

local cli = ifNil( Etcd.new() );
local key = '/test_key'
local val = {
    structured = {
        value = {
            of = 'key'
        }
    }
};
local ttl = 2;
local res;

-- cleanup
ifNil( cli:delete( key ) )

res = ifNil( cli:get( key ) );
ifNotEqual( res.status, 404 );

res = ifNil( cli:set( key, val ) );
ifNotEqual( res.status, 201 );

res = ifNil( cli:get( key ) );
ifNotEqual( inspect( val ), inspect( res.body.node.value ) );

res = ifNil( cli:delete( key ) );
ifNotEqual( res.status, 200 );


res = ifNil( cli:set( key, val, ttl ) );
ifNotEqual( res.status, 201 );
sleep( ttl + 1 );
res = ifNil( cli:get( key ) );
ifNotEqual( res.status, 404 );

