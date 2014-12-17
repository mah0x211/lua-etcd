--[[
  
  Copyright (C) 2014 Masatoshi Teruya

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:
 
  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.
 
  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.

 
  etcd.lua
  lua-etcd
  
  Created by Masatoshi Teruya on 14/11/17.
  
--]]

-- module
local normalize = require('path').normalize;
local typeof = require('util.typeof');
local encodeJSON = require('cjson.safe').encode;
local decodeJSON = require('cjson.safe').decode;
-- constants
local DEFAULT_OPTS = {
    host = {
        typ = 'string',
        def = 'http://127.0.0.1:4001'
    },
    peer = {
        typ = 'string',
        def = 'http://127.0.0.1:7001'
    },
    prefix = {
        typ = 'string'
    },
    ttl = {
        typ = 'int',
        def = -1
    }
};
local CLIENT_ENDPOINTS = {
    version         = '/version',
    statsLeader     = '/v2/stats/leader',
    statsSelf       = '/v2/stats/self',
    statsStore      = '/v2/stats/store',
    keys            = '/v2/keys'
};
local ADMIN_ENDPOINTS = {
    adminConfig     = '/v2/admin/config',
    adminMachines   = '/v2/admin/machines'
};
-- errors
local EOPTS = 'opts.%s must be %s';
local EINVAL = '%s must be %s';
local EKEYPATH = 'key should not be a slash';
local EENCODE = 'encoding error: %q';


-- private
local function initClientEndpoints( endpoints, uri, prefix )
    local opt;
    
    -- construct client api endpoints
    for k, v in pairs( CLIENT_ENDPOINTS ) do
        -- append prefix
        if k == 'keys' and prefix then
            opt = prefix;
            -- append prefix if it is not a slash.
            if opt ~= '/' then
                endpoints[k] = uri .. v .. opt;
            else
                endpoints[k] = uri .. v;
            end
        else
            endpoints[k] = uri .. v;
        end
    end
end


local function initAdminEndpoints( endpoints, uri )
    for k, v in pairs( ADMIN_ENDPOINTS ) do
        endpoints[k] = uri .. v;
    end
end


local function createEndpoints( host, peer, prefix )
    local endpoints = {};
    
    initClientEndpoints( endpoints, host, prefix );
    initAdminEndpoints( endpoints, peer );
    
    return endpoints;
end


local function request( own, method, uri, opts, timeout )
    local entity, err = own.cli[method]( own.cli, uri, opts, timeout );
    
    if err then
        return nil, err;
    end
    
    return entity;
end


local function set( own, key, val, attr )
    local opts = {
        query = {
            prevExist = attr.prevExist,
            prevIndex = attr.prevIndex
        },
        body = {}
    };
    local uri, err;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    -- CAS idx
    elseif attr.prevIndex ~= nil and not typeof.uint( attr.prevIndex ) then
        return nil, EINVAL:format( 'modifiedIndex', 'unsigned integer' );
    elseif attr.ttl == nil then
        attr.ttl = own.ttl;
    elseif not typeof.int( attr.ttl ) then
        return nil, EINVAL:format( 'ttl', 'integer' );
    end
    
    -- verify key
    key = normalize( key );
    if key == '/' then
        return nil, EKEYPATH;
    end
    uri = own.endpoints.keys .. key;
    
    -- set ttl
    opts.body.ttl = attr.ttl >= 0 and attr.ttl or '';
    
    if attr.dir then
        opts.body.dir = true;
    -- encode value
    elseif val ~= nil then
        opts.body.value, err = encodeJSON( val );
        if err then
            return nil, EENCODE:format( err );
        end
    end
    
    return request( own, attr.inOrder and 'post' or 'put', uri, opts );
end


local function get( own, key, attr )
    local opts = {
        query = {
            wait = attr.wait or nil,
            waitIndex = attr.waitIndex or nil,
            recursive = attr.recursive or nil,
            consistent = attr.consistent or nil
        }
    };
    local uri, entity, err;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    elseif attr.timeout ~= nil and not typeof.uint( attr.timeout ) then
        return nil, EINVAL:format( 'timeout', 'unsigned integer' );
    elseif attr.wait ~= nil and not typeof.boolean( attr.wait ) then
        return nil, EINVAL:format( 'wait', 'boolean' );
    elseif attr.waitIndex ~= nil and not typeof.uint( attr.waitIndex ) then
        return nil, EINVAL:format( 'waitIndex', 'unsigned integer' );
    elseif attr.recursive ~= nil and not typeof.boolean( attr.recursive ) then
        return nil, EINVAL:format( 'recursive', 'boolean' );
    elseif attr.consistent ~= nil and not typeof.boolean( attr.consistent ) then
        return nil, EINVAL:format( 'consistent', 'boolean' );
    end
    uri = own.endpoints.keys .. normalize( key );
    
    entity, err = request( own, 'get', uri, opts, attr.timeout );
    if err then
        return nil, err;
    -- readdir
    elseif attr.dir then
        -- set 404 not found if result node is not directory
        if entity.status == 200 and entity.body.node and 
               not entity.body.node.dir then
            entity.status = 404;
            entity.body.node.dir = false;
        end
    -- get
    elseif entity.status == 200 and entity.body.node and 
           not entity.body.node.dir then
        entity.body.node.value, err = decodeJSON( entity.body.node.value );
        if err then
            return nil, err;
        end
    end
    
    return entity;
end


local function delete( own, key, attr )
    local opts = {
        query = {
            dir = attr.dir,
            prevIndex = attr.prevIndex
        }
    };
    local uri, recursive;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    -- CAS index
    elseif attr.prevIndex ~= nil and not typeof.uint( attr.prevIndex ) then
        return nil, EINVAL:format( 'modifiedIndex', 'unsigned integer' );
    end
    uri = own.endpoints.keys .. normalize( key );
    
    -- check attributes
    if attr.dir then
        if attr.recursive ~= nil and not typeof.boolean( attr.recursive ) then
            return nil, EINVAL:format( 'recursive', 'boolean' );
        end
        opts.query.recursive = attr.recursive or nil;
    -- use prevValue
    elseif attr.prevValue ~= nil then
        opts.query.prevValue, err = encodeJSON( attr.prevValue );
        if err then
            return nil, EENCODE:format( err );
        end
    end
    
    return request( own, 'delete', uri, opts );
end


-- class
local Etcd = require('halo').class.Etcd;


function Etcd:init( cli, opts )
    local own = protected( self );
    local opt;
    
    -- set http-client instance
    own.cli = cli;
    
    -- check opts
    if not opts then
        opts = {};
    elseif not typeof.table( opts ) then
        return nil, EINVAL:format( 'opts', 'table' );
    end
    
    for k, v in pairs( DEFAULT_OPTS ) do
        opt = opts[k];
        if opt == nil then
            own[k] = v.def;
        elseif not typeof[v.typ]( opt ) then
            return nil, EOPTS:format( k, v.typ );
        else
            own[k] = opt;
        end
    end
    
    -- normalize prefix
    if own.prefix then
        own.prefix = normalize( own.prefix );
    end
    own.endpoints = createEndpoints( own.host, own.peer, own.prefix );
    
    return self;
end


-- /version
function Etcd:version()
    local own = protected( self );
    return request( own, 'get', own.endpoints.version );
end


-- /stats
function Etcd:statsLeader()
    local own = protected( self );
    return request( own, 'get', own.endpoints.statsLeader );
end

function Etcd:statsSelf()
    local own = protected( self );
    return request( own, 'get', own.endpoints.statsSelf )
end

function Etcd:statsStore()
    local own = protected( self );
    return request( own, 'get', own.endpoints.statsStore );
end


-- /admin/machines
function Etcd:adminMachines( name )
    local own = protected( self );
    local uri = own.endpoints.adminMachines;
    
    if name ~= nil then
        if not typeof.string( name ) then
            return nil, EINVAL:format( 'name', 'string' );
        end
        name = normalize( name );
        if name ~= '/' then
            uri = uri .. own.endpoints.adminMachines .. name;
        end
    end
    
    return request( own, 'get', uri );
end


function Etcd:removeAdminMachines( name )
    local own = protected( self );
    local uri = own.endpoints.adminMachines;
    
    if name ~= nil then
        if not typeof.string( name ) then
            return nil, EINVAL:format( 'name', 'string' );
        end
        name = normalize( name );
        if name ~= '/' then
            uri = uri .. own.endpoints.adminMachines .. name;
        end
    end
    
    return request( own, 'delete', uri );
end


-- /admin/config
function Etcd:adminConfig()
    local own = protected( self );
    return request( own, 'get', own.endpoints.adminConfig );
end


function Etcd:setAdminConfig( opts )
    local own = protected( self );
    local cfg = {};
    local opt;
    
    if not typeof.table( cfg ) then
        return nil, EINVAL:format( 'cfg', 'table' );
    end
    
    for k, t in pairs({
        activeSize = 'uint',
        removeDelay = 'uint',
        syncInterval = 'uint'
    }) do
        opt = opts[k]
        if opt ~= nil then
            if not typeof[t]( opt ) then
                return nil, EOPTS:format( k, t );
            end
            cfg[k] = opt;
        end
    end
    
    return request( own, 'put', own.endpoints.adminConfig, {
        body = cfg,
        enctype = 'application/json'
    });
end


-- /keys
function Etcd:set( key, val, ttl )
    return set( protected( self ), key, val, { 
        ttl = ttl
    });
end


-- set key-val and ttl if key does not exists (atomic create)
function Etcd:setnx( key, val, ttl )
    return set( protected( self ), key, val, { 
        ttl = ttl,
        prevExist = false
    });
end


-- set key-val and ttl if key is exists (update)
function Etcd:setx( key, val, ttl, modifiedIndex )
    return set( protected( self ), key, val, { 
        ttl = ttl,
        prevExist = true,
        prevIndex = modifiedIndex
    });
end


-- in-order keys
function Etcd:push( key, val, ttl )
    return set( protected( self ), key, val, { 
        ttl = ttl,
        inOrder = true
    });
end


function Etcd:get( key, consistent )
    return get( protected( self ), key, {
        consistent = consistent
    });
end


-- delete key 
-- atomic delete if val or modifiedIndex are not nil.
function Etcd:delete( key, val, modifiedIndex )
    return delete( protected( self ), key, {
        prevValue = val,
        prevIndex = modifiedIndex
    });
end


-- wait
function Etcd:wait( key, modifiedIndex, timeout )
    local own = protected( self );
    
    return get( protected( self ), key, {
        wait = true,
        waitIndex = modifiedIndex,
        timeout = timeout
    });
end


-- dir
function Etcd:mkdir( key, ttl )
    return set( protected( self ), key, nil, { 
        ttl = ttl,
        dir = true
    });
end


-- mkdir if not exists
function Etcd:mkdirnx( key, ttl )
    return set( protected( self ), key, nil, { 
        ttl = ttl,
        dir = true,
        prevExist = false
    });
end


function Etcd:readdir( key, recursive, consistent )
    return get( protected( self ), key, {
        dir = true,
        recursive = recursive,
        consistent = consistent
    });
end


function Etcd:rmdir( key, recursive )
    return delete( protected( self ), key, {
        dir = true,
        recursive = recursive
    });
end


-- wait with recursive
function Etcd:waitdir( key, modifiedIndex, timeout )
    local own = protected( self );
    
    return get( protected( self ), key, {
        wait = true,
        recursive = true,
        waitIndex = modifiedIndex,
        timeout = timeout
    });
end


-- set ttl for key
function Etcd:setTTL( key, ttl )
    local own = protected( self );
    local uri, entity, err;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    elseif ttl == nil then
        ttl = own.ttl;
    elseif not typeof.int( ttl ) then
        return nil, EINVAL:format( 'ttl', 'integer' );
    end
    uri = own.endpoints.keys .. normalize( key );
    
    -- get prev-value
    entity, err = request( own, 'get', uri );
    if err then
        return nil, err;
    elseif entity.status ~= 200 then
        return entity;
    end
    
    -- update with prev-value
    return request( own, 'put', uri, {
        query = {
            prevValue = entity.body.node.value,
            prevIndex = not entity.body.node.dir and entity.body.node.modifiedIndex or nil,
            prevExist = true
        },
        body = {
            ttl = ttl >= 0 and ttl or '',
            dir = entity.body.node.dir,
            value = entity.body.node.value
        }
    });
end


return Etcd.exports;
