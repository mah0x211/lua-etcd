package = "etcd"
version = "scm-1"
source = {
    url = "git://github.com/mah0x211/lua-etcd.git"
}
description = {
    summary = "etcd client module.",
    homepage = "https://github.com/mah0x211/lua-etcd", 
    license = "MIT/X11",
    maintainer = "Masatoshi Teruya"
}
dependencies = {
    "lua >= 5.1",
    "halo >= 1.1",
    "util >= 1.1",
    "path >= 1.0",
    "lua-cjson >= 2.1.0",
    "httpcli >= 1.1-1"
}
build = {
    type = "builtin",
    modules = {
        etcd = "etcd.lua",
        ["etcd.luasocket"] = "lib/luasocket.lua"
    }
}

