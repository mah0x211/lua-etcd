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
        def = '127.0.0.1'
    },
    clientPort = {
        typ = 'uint',
        def = 4001
    },
    adminPort = {
        typ = 'uint',
        def = 7001
    },
    https = {
        typ = 'boolean',
        def = false
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
local function request( cli, method, uri, opts )
    local entity, err = cli[method]( cli, uri, opts );
    
    if err then
        return nil, err;
    end
    
    return entity;
end


local function set( own, key, val, attr )
    local opts = {
        query = {
            prevExist = attr.prevExist
        },
        body = {}
    };
    local uri, err;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
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
    
    return request( own.cli, attr.inOrder and 'post' or 'put', uri, opts );
end


local function delete( own, key, attr )
    local uri, recursive;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    end
    uri = own.endpoints.keys .. normalize( key );
    
    -- check attributes
    if attr.dir then
        if attr.recursive == nil then
            uri = uri .. '?dir=true';
        elseif not typeof.boolean( attr.recursive ) then
            return nil, EINVAL:format( 'recursive', 'boolean' );
        elseif attr.recursive then
            uri = uri .. '?dir=true&recursive=true';
        else
            uri = uri .. '?dir=true';
        end
    end
    
    return request( own.cli, 'delete', uri );
end


-- class
local Etcd = require('halo').class.Etcd;


function Etcd:init( cli, opts )
    local own = protected( self );
    local opt, host, uri;
    
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
    
    -- construct endpoints
    host = 'http' .. ( own.https and 's://' or '://' ) .. own.host .. ':';
    own.endpoints = {};
    -- construct client api endpoints
    uri = host .. own.clientPort;
    for k, v in pairs( CLIENT_ENDPOINTS ) do
        -- append prefix
        if k == 'keys' and own.prefix then
            opt = normalize( own.prefix );
            -- append prefix if it is not a slash.
            if opt ~= '/' then
                own.endpoints[k] = uri .. v .. opt;
            else
                own.endpoints[k] = uri .. v;
            end
        else
            own.endpoints[k] = uri .. v;
        end
    end
    -- construct admin api endpoints
    uri = host .. own.adminPort;
    for k, v in pairs( ADMIN_ENDPOINTS ) do
        own.endpoints[k] = uri .. v;
    end
    
    return self;
end


-- /version
function Etcd:version()
    local own = protected( self );
    return request( own.cli, 'get', own.endpoints.version );
end


-- /stats
function Etcd:statsLeader()
    local own = protected( self );
    return request( own.cli, 'get', own.endpoints.statsLeader );
end

function Etcd:statsSelf()
    local own = protected( self );
    return request( own.cli, 'get', own.endpoints.statsSelf );
end

function Etcd:statsStore()
    local own = protected( self );
    return request( own.cli, 'get', own.endpoints.statsStore );
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
    
    return request( own.cli, 'get', uri );
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
    
    return request( own.cli, 'delete', uri );
end


-- /admin/config
function Etcd:adminConfig()
    local own = protected( self );
    return request( own.cli, 'get', own.endpoints.adminConfig );
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
    
    return request( own.cli, 'put', own.endpoints.adminConfig, {
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
function Etcd:setx( key, val, ttl )
    return set( protected( self ), key, val, { 
        ttl = ttl,
        prevExist = true
    });
end


-- in-order keys
function Etcd:push( key, val, ttl )
    return set( protected( self ), key, val, { 
        ttl = ttl,
        inOrder = true
    });
end


function Etcd:get( key )
    local own = protected( self );
    local uri, entity, err;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    end
    uri = own.endpoints.keys .. normalize( key );
    
    entity, err = own.cli:get( uri );
    if err then
        return nil, err;
    elseif entity.status == 200 and entity.body.node then
        entity.body.node.value, err = decodeJSON( entity.body.node.value );
        if err then
            return nil, err;
        end
    end
    
    return entity;
end


function Etcd:delete( key )
    return delete( protected( self ), key, {} );
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


function Etcd:readdir( key, recursive )
    local own = protected( self );
    local uri;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    elseif recursive == nil then
        recursive = '';
    elseif not typeof.boolean( recursive ) then
        return nil, EINVAL:format( 'recursive', 'boolean' );
    elseif recursive then
        recursive = '?recursive=true';
    else
        recursive = '';
    end
    uri = own.endpoints.keys .. normalize( key ) .. recursive;
    
    return request( own.cli, 'get', uri );
end


function Etcd:rmdir( key, recursive )
    return delete( protected( self ), key, {
        dir = true,
        recursive = recursive
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
    entity, err = own.cli:get( uri );
    if err then
        return nil, err;
    elseif entity.status ~= 200 then
        return entity;
    end
    
    -- update with prev-value
    return request( own.cli, 'put', uri, {
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
