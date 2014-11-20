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
        typ = 'string',
        def = '/app/cache/'
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
local EENCODE = 'encoding error: %q';

-- private
local function request( cli, method, uri, opts )
    local entity, err = cli[method]( cli, uri, opts );
    
    if err then
        return nil, err;
    end
    
    return entity;
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
        if k == 'keys' then
            own.endpoints[k] = uri .. normalize( v, own.prefix );
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
    local own = protected( self );
    local opts = {
        body = {}
    };
    local uri, err;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    elseif ttl == nil then
        ttl = own.ttl;
    elseif not typeof.int( ttl ) then
        return nil, EINVAL:format( 'ttl', 'integer' );
    end
    uri = own.endpoints.keys .. normalize( key );
    
    -- set ttl
    opts.body.ttl = ttl >= 0 and ttl or '';
    -- encode
    opts.body.value, err = encodeJSON( val );
    if err then
        return nil, EENCODE:format( err );
    end
    
    return request( own.cli, 'put', uri, opts );
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
    local own = protected( self );
    local uri;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    end
    uri = own.endpoints.keys .. normalize( key );

    return request( own.cli, 'delete', uri );
end


-- dir
function Etcd:mkdir( key, ttl )
    local own = protected( self );
    local opts = {
        body = {
            dir = true
        }
    };
    local uri;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    elseif ttl == nil then
        ttl = own.ttl;
    elseif not typeof.int( ttl ) then
        return nil, EINVAL:format( 'ttl', 'integer' );
    end
    uri = own.endpoints.keys .. normalize( key );
    
    -- set ttl
    opts.body.ttl = ttl >= 0 and ttl or '';
    
    return request( own.cli, 'put', uri, opts );
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
    local own = protected( self );
    local uri;
    
    -- check arguments
    if not typeof.string( key ) then
        return nil, EINVAL:format( 'key', 'string' );
    elseif recursive == nil then
        recursive = '?dir=true';
    elseif not typeof.boolean( recursive ) then
        return nil, EINVAL:format( 'recursive', 'boolean' );
    elseif recursive then
        recursive = '?dir=true&recursive=true';
    else
        recursive = '?dir=true';
    end
    uri = own.endpoints.keys .. normalize( key ) .. recursive;
    
    return request( own.cli, 'delete', uri );
end


return Etcd.exports;
