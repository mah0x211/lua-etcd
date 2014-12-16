local Etcd = require('etcd.luasocket');

-- opt must be table or nil
ifNotNil( Etcd.new( '' ) );
ifNil( Etcd.new({}) );
ifNil( Etcd.new() );

-- timeout must be uint
ifNotNil( Etcd.new({
    timeout = ''
}));
ifNil( Etcd.new({
    timeout = 10
}));

-- host must be string
ifNotNil( Etcd.new({
    host = 0
}));
ifNil( Etcd.new({
    host = 'http://127.0.0.1:4001'
}));

-- peer must be string
ifNotNil( Etcd.new({
    peer = 0
}));
ifNil( Etcd.new({
    peer = 'http://127.0.0.1:7001'
}));


-- prefix must be string
ifNotNil( Etcd.new({
    prefix = 0
}));
ifNil( Etcd.new({
    prefix = '/app/cache'
}));

-- ttl must be int
ifNotNil( Etcd.new({
    ttl = true
}));
ifNil( Etcd.new({
    ttl = 60
}));

