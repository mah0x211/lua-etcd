local ETCD = {};
do
    local NETCD = 5;
    local ADDR = '127.0.0.1:400';
    local PEER = '127.0.0.1:700';
    local label = 'machine';
    local args, name, peers;
    
    for idx = 1, NETCD do
        name = label .. idx;
        args = {
            '-addr', ADDR .. idx,
            '-peer-addr', PEER .. idx,
            '-name', name,
            '-data-dir', './' .. name,
        };
        
        -- set peers
        peers = {};
        for j = 1, NETCD do
            if j ~= idx then
                peers[#peers+1] = PEER .. j;
            end
        end
        args[#args+1] = '-peers';
        args[#args+1] = table.concat( peers, ',' );
        --print( 'exec: etcd ' .. table.concat( args, ' ' ) );
        ETCD[name] = ifNil( execChild( 'etcd', args ) );
        sleep(1);
    end
    sleep(1);
end

-- default etcd endpoint
--  host: http://127.0.0.1:4001
--  peer: http://127.0.0.1:7001
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
local consistent = true;
local res;

local function stopLeader()
    local res = ifNil( cli:statsLeader() );
    local name = res.body.leader;
    local etcd = ETCD[name];
    
    if etcd then
        --print( 'stop machine:', name );
        etcd:kill();
        sleep(1);
        ETCD[name] = nil;
    end
end


ifNotTrue( cli:initFailoverURIs() );

-- cleanup
ifNil( cli:delete( key ) )

res = ifNil( cli:get( key ) );
ifNotEqual( res.status, 404 );

res = ifNil( cli:setx( key, val ) );
ifNotEqual( res.status, 404 );

-- stop current leader
stopLeader();

res = ifNil( cli:set( key, val ) );
ifNotEqual( res.status, 201 );

-- stop current leader
stopLeader();

res = ifNil( cli:setnx( key, val ) );
ifNotEqual( res.status, 412 );

res = ifNil( cli:get( key ) );
ifNotEqual( inspect( val ), inspect( res.body.node.value ) );

res = ifNil( cli:get( key, consistent ) );
ifNotEqual( inspect( val ), inspect( res.body.node.value ) );

res = ifNil( cli:delete( key ) );
ifNotEqual( res.status, 200 );

