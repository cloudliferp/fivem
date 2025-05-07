#ifndef LUA_MYSQL_H
#define LUA_MYSQL_H

#include <lua.hpp>


#if !defined(LUAMOD_API)
#define LUAMOD_API LUALIB_API
#endif

#if defined(__cplusplus)
extern "C" {
#endif

#define LUA_MYSQLLIBNAME "mysql"
LUAMOD_API int luaopen_mysql(lua_State* L);

#if defined(__cplusplus)
}
#endif

#endif
