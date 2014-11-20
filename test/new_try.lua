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
    host = '127.0.0.1'
}));

-- clientPort must be uint
ifNotNil( Etcd.new({
    clientPort = ''
}));
ifNotNil( Etcd.new({
    clientPort = -1
}));
ifNil( Etcd.new({
    clientPort = 8000
}));

-- adminPort must be uint
ifNotNil( Etcd.new({
    adminPort = ''
}));
ifNotNil( Etcd.new({
    adminPort = -1
}));
ifNil( Etcd.new({
    adminPort = 8000
}));

-- https must be boolean
ifNotNil( Etcd.new({
    https = ''
}));
ifNil( Etcd.new({
    https = false
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

