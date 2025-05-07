#include "StdInc.h"

#include "mysql_lib.h"
#include <mysql/mysql.h>
#include <string>
#include <vector>
#include <map>
#include <memory>

struct MySQLConnection
{
	MYSQL* handle;
	bool connected;

	MySQLConnection()
		: handle(nullptr), connected(false)
	{
		handle = mysql_init(nullptr);
	}

	~MySQLConnection()
	{
		if (handle)
		{
			if (connected)
			{
				mysql_close(handle);
			}
			handle = nullptr;
			connected = false;
		}
	}
};

struct MySQLResult
{
	MYSQL_RES* result;

	MySQLResult()
		: result(nullptr)
	{
	}

	~MySQLResult()
	{
		if (result)
		{
			mysql_free_result(result);
			result = nullptr;
		}
	}
};

static const char* MYSQL_CONNECTION_MT = "MySQL.Connection";
static const char* MYSQL_RESULT_MT = "MySQL.Result";

static MySQLConnection* check_mysql_connection(lua_State* L, int index)
{
	return (MySQLConnection*)luaL_checkudata(L, index, MYSQL_CONNECTION_MT);
}

static MySQLResult* check_mysql_result(lua_State* L, int index)
{
	return (MySQLResult*)luaL_checkudata(L, index, MYSQL_RESULT_MT);
}

static int mysql_connect(lua_State* L)
{
	const char* host = luaL_optstring(L, 1, "localhost");
	const char* user = luaL_optstring(L, 2, "root");
	const char* password = luaL_optstring(L, 3, "");
	const char* database = luaL_optstring(L, 4, "");
	unsigned int port = (unsigned int)luaL_optinteger(L, 5, 3306);

	MySQLConnection* conn = (MySQLConnection*)lua_newuserdata(L, sizeof(MySQLConnection));
	new (conn) MySQLConnection();

	luaL_getmetatable(L, MYSQL_CONNECTION_MT);
	lua_setmetatable(L, -2);

	if (!mysql_real_connect(conn->handle, host, user, password, database, port, NULL, 0))
	{
		const char* error = mysql_error(conn->handle);
		return luaL_error(L, "MySQL connection error: %s", error);
	}

	conn->connected = true;
	return 1;
}

static int mysql_conn_close(lua_State* L)
{
	MySQLConnection* conn = check_mysql_connection(L, 1);

	if (conn->handle && conn->connected)
	{
		mysql_close(conn->handle);
		conn->handle = nullptr;
		conn->connected = false;
	}

	lua_pushboolean(L, 1);
	return 1;
}

static int mysql_conn_ping(lua_State* L)
{
	MySQLConnection* conn = check_mysql_connection(L, 1);

	if (!conn->handle || !conn->connected)
	{
		return luaL_error(L, "Connection not established");
	}

	int result = mysql_ping(conn->handle);

	if (result == 0)
	{
		lua_pushboolean(L, 1);
		return 1;
	}
	else
	{
		return luaL_error(L, "MySQL ping error: %s", mysql_error(conn->handle));
	}
}

static int mysql_conn_select_db(lua_State* L)
{
	MySQLConnection* conn = check_mysql_connection(L, 1);
	const char* db_name = luaL_checkstring(L, 2);

	if (!conn->handle || !conn->connected)
	{
		return luaL_error(L, "Connection not established");
	}

	int result = mysql_select_db(conn->handle, db_name);

	if (result == 0)
	{
		lua_pushboolean(L, 1);
		return 1;
	}
	else
	{
		return luaL_error(L, "MySQL select_db error: %s", mysql_error(conn->handle));
	}
}

static int mysql_conn_query(lua_State* L)
{
	MySQLConnection* conn = check_mysql_connection(L, 1);
	const char* query = luaL_checkstring(L, 2);

	if (!conn->handle || !conn->connected)
	{
		return luaL_error(L, "Connection not established");
	}

	int result = mysql_query(conn->handle, query);

	if (result != 0)
	{
		return luaL_error(L, "MySQL query error: %s", mysql_error(conn->handle));
	}

	MYSQL_RES* mysql_res = mysql_store_result(conn->handle);

	if (mysql_res)
	{
		MySQLResult* res = (MySQLResult*)lua_newuserdata(L, sizeof(MySQLResult));
		new (res) MySQLResult();
		res->result = mysql_res;

		luaL_getmetatable(L, MYSQL_RESULT_MT);
		lua_setmetatable(L, -2);
		return 1;
	}
	else
	{
		if (mysql_field_count(conn->handle) == 0)
		{
			lua_pushboolean(L, 1);
			lua_pushinteger(L, (lua_Integer)mysql_affected_rows(conn->handle));
			lua_pushinteger(L, (lua_Integer)mysql_insert_id(conn->handle));
			return 3;
		}
		else
		{
			return luaL_error(L, "MySQL query error: %s", mysql_error(conn->handle));
		}
	}
}

static int mysql_conn_escape_string(lua_State* L)
{
	MySQLConnection* conn = check_mysql_connection(L, 1);
	size_t str_len;
	const char* str = luaL_checklstring(L, 2, &str_len);

	if (!conn->handle || !conn->connected)
	{
		return luaL_error(L, "Connection not established");
	}

	char* escaped = new char[str_len * 2 + 1];

	size_t escaped_len = mysql_real_escape_string(conn->handle, escaped, str, str_len);

	lua_pushlstring(L, escaped, escaped_len);

	delete[] escaped;

	return 1;
}

static int mysql_conn_gc(lua_State* L)
{
	MySQLConnection* conn = check_mysql_connection(L, 1);
	conn->~MySQLConnection();
	return 0;
}

static int mysql_conn_tostring(lua_State* L)
{
	MySQLConnection* conn = check_mysql_connection(L, 1);
	lua_pushfstring(L, "MySQL connection (%p)", conn->handle);
	return 1;
}

static int mysql_result_fetch(lua_State* L)
{
	MySQLResult* res = check_mysql_result(L, 1);

	if (!res->result)
	{
		return luaL_error(L, "Invalid result object");
	}

	MYSQL_ROW row = mysql_fetch_row(res->result);
	if (!row)
	{
		lua_pushnil(L);
		return 1;
	}

	unsigned int num_fields = mysql_num_fields(res->result);
	MYSQL_FIELD* fields = mysql_fetch_fields(res->result);
	unsigned long* lengths = mysql_fetch_lengths(res->result);

	lua_createtable(L, 0, num_fields);

	for (unsigned int i = 0; i < num_fields; i++)
	{
		if (row[i])
		{
			int field_type = fields[i].type;
			switch (field_type)
			{
				case MYSQL_TYPE_TINY:
				case MYSQL_TYPE_SHORT:
				case MYSQL_TYPE_LONG:
				case MYSQL_TYPE_INT24:
				case MYSQL_TYPE_LONGLONG:
					lua_pushstring(L, fields[i].name);
					lua_pushinteger(L, atoll(row[i]));
					lua_settable(L, -3);
					break;

				case MYSQL_TYPE_FLOAT:
				case MYSQL_TYPE_DOUBLE:
				case MYSQL_TYPE_DECIMAL:
				case MYSQL_TYPE_NEWDECIMAL:
					lua_pushstring(L, fields[i].name);
					lua_pushnumber(L, atof(row[i]));
					lua_settable(L, -3);
					break;

				default:
					lua_pushstring(L, fields[i].name);
					lua_pushlstring(L, row[i], lengths[i]);
					lua_settable(L, -3);
					break;
			}
		}
		else
		{
			lua_pushstring(L, fields[i].name);
			lua_pushnil(L);
			lua_settable(L, -3);
		}

		lua_pushinteger(L, i + 1);
		if (row[i])
		{
			lua_pushlstring(L, row[i], lengths[i]);
		}
		else
		{
			lua_pushnil(L);
		}
		lua_settable(L, -3);
	}

	return 1;
}

static int mysql_result_fetch_all(lua_State* L)
{
	MySQLResult* res = check_mysql_result(L, 1);

	if (!res->result)
	{
		return luaL_error(L, "Invalid result object");
	}

	mysql_data_seek(res->result, 0);

	unsigned int num_fields = mysql_num_fields(res->result);
	MYSQL_FIELD* fields = mysql_fetch_fields(res->result);

	lua_createtable(L, mysql_num_rows(res->result), 0);
	int results_tbl = lua_gettop(L);

	int row_count = 0;
	MYSQL_ROW row;

	while ((row = mysql_fetch_row(res->result)))
	{
		row_count++;
		unsigned long* lengths = mysql_fetch_lengths(res->result);

		lua_createtable(L, 0, num_fields);

		for (unsigned int i = 0; i < num_fields; i++)
		{
			if (row[i])
			{
				int field_type = fields[i].type;
				switch (field_type)
				{
					case MYSQL_TYPE_TINY:
					case MYSQL_TYPE_SHORT:
					case MYSQL_TYPE_LONG:
					case MYSQL_TYPE_INT24:
					case MYSQL_TYPE_LONGLONG:
						lua_pushstring(L, fields[i].name);
						lua_pushinteger(L, atoll(row[i]));
						lua_settable(L, -3);
						break;

					case MYSQL_TYPE_FLOAT:
					case MYSQL_TYPE_DOUBLE:
					case MYSQL_TYPE_DECIMAL:
					case MYSQL_TYPE_NEWDECIMAL:
						lua_pushstring(L, fields[i].name);
						lua_pushnumber(L, atof(row[i]));
						lua_settable(L, -3);
						break;

					default:
						lua_pushstring(L, fields[i].name);
						lua_pushlstring(L, row[i], lengths[i]);
						lua_settable(L, -3);
						break;
				}
			}
			else
			{
				lua_pushstring(L, fields[i].name);
				lua_pushnil(L);
				lua_settable(L, -3);
			}

			lua_pushinteger(L, i + 1);
			if (row[i])
			{
				lua_pushlstring(L, row[i], lengths[i]);
			}
			else
			{
				lua_pushnil(L);
			}
			lua_settable(L, -3);
		}

		lua_rawseti(L, results_tbl, row_count);
	}

	return 1;
}

static int mysql_result_num_rows(lua_State* L)
{
	MySQLResult* res = check_mysql_result(L, 1);

	if (!res->result)
	{
		return luaL_error(L, "Invalid result object");
	}

	lua_pushinteger(L, (lua_Integer)mysql_num_rows(res->result));
	return 1;
}

static int mysql_result_num_fields(lua_State* L)
{
	MySQLResult* res = check_mysql_result(L, 1);

	if (!res->result)
	{
		return luaL_error(L, "Invalid result object");
	}

	lua_pushinteger(L, (lua_Integer)mysql_num_fields(res->result));
	return 1;
}

static int mysql_result_field_info(lua_State* L)
{
	MySQLResult* res = check_mysql_result(L, 1);

	if (!res->result)
	{
		return luaL_error(L, "Invalid result object");
	}

	unsigned int num_fields = mysql_num_fields(res->result);
	MYSQL_FIELD* fields = mysql_fetch_fields(res->result);

	lua_createtable(L, num_fields, 0);

	for (unsigned int i = 0; i < num_fields; i++)
	{
		lua_createtable(L, 0, 5);

		lua_pushstring(L, "name");
		lua_pushstring(L, fields[i].name);
		lua_settable(L, -3);

		lua_pushstring(L, "type");
		switch (fields[i].type)
		{
			case MYSQL_TYPE_TINY:
				lua_pushstring(L, "TINYINT");
				break;
			case MYSQL_TYPE_SHORT:
				lua_pushstring(L, "SMALLINT");
				break;
			case MYSQL_TYPE_LONG:
				lua_pushstring(L, "INT");
				break;
			case MYSQL_TYPE_INT24:
				lua_pushstring(L, "MEDIUMINT");
				break;
			case MYSQL_TYPE_LONGLONG:
				lua_pushstring(L, "BIGINT");
				break;
			case MYSQL_TYPE_DECIMAL:
			case MYSQL_TYPE_NEWDECIMAL:
				lua_pushstring(L, "DECIMAL");
				break;
			case MYSQL_TYPE_FLOAT:
				lua_pushstring(L, "FLOAT");
				break;
			case MYSQL_TYPE_DOUBLE:
				lua_pushstring(L, "DOUBLE");
				break;
			case MYSQL_TYPE_BIT:
				lua_pushstring(L, "BIT");
				break;
			case MYSQL_TYPE_TIMESTAMP:
				lua_pushstring(L, "TIMESTAMP");
				break;
			case MYSQL_TYPE_DATE:
				lua_pushstring(L, "DATE");
				break;
			case MYSQL_TYPE_TIME:
				lua_pushstring(L, "TIME");
				break;
			case MYSQL_TYPE_DATETIME:
				lua_pushstring(L, "DATETIME");
				break;
			case MYSQL_TYPE_YEAR:
				lua_pushstring(L, "YEAR");
				break;
			case MYSQL_TYPE_STRING:
				lua_pushstring(L, "CHAR");
				break;
			case MYSQL_TYPE_VAR_STRING:
				lua_pushstring(L, "VARCHAR");
				break;
			case MYSQL_TYPE_BLOB:
				lua_pushstring(L, "BLOB");
				break;
			case MYSQL_TYPE_SET:
				lua_pushstring(L, "SET");
				break;
			case MYSQL_TYPE_ENUM:
				lua_pushstring(L, "ENUM");
				break;
			case MYSQL_TYPE_NULL:
				lua_pushstring(L, "NULL");
				break;
			default:
				lua_pushstring(L, "UNKNOWN");
				break;
		}
		lua_settable(L, -3);

		lua_pushstring(L, "length");
		lua_pushinteger(L, (lua_Integer)fields[i].length);
		lua_settable(L, -3);

		lua_pushstring(L, "max_length");
		lua_pushinteger(L, (lua_Integer)fields[i].max_length);
		lua_settable(L, -3);

		lua_pushstring(L, "flags");
		lua_pushinteger(L, (lua_Integer)fields[i].flags);
		lua_settable(L, -3);

		lua_rawseti(L, -2, i + 1);
	}

	return 1;
}

static int mysql_result_gc(lua_State* L)
{
	MySQLResult* res = check_mysql_result(L, 1);
	res->~MySQLResult();
	return 0;
}

static int mysql_result_tostring(lua_State* L)
{
	MySQLResult* res = check_mysql_result(L, 1);
	if (!res->result)
	{
		lua_pushstring(L, "MySQL result (closed)");
	}
	else
	{
		lua_pushfstring(L, "MySQL result (rows: %d, fields: %d)",
		(int)mysql_num_rows(res->result),
		(int)mysql_num_fields(res->result));
	}
	return 1;
}

static const struct luaL_Reg mysql_lib[] = {
	{ "connect", mysql_connect },
	{ NULL, NULL }
};

static const struct luaL_Reg mysql_conn_methods[] = {
	{ "close", mysql_conn_close },
	{ "ping", mysql_conn_ping },
	{ "select_db", mysql_conn_select_db },
	{ "query", mysql_conn_query },
	{ "escape_string", mysql_conn_escape_string },
	{ "__gc", mysql_conn_gc },
	{ "__tostring", mysql_conn_tostring },
	{ NULL, NULL }
};

static const struct luaL_Reg mysql_result_methods[] = {
	{ "fetch", mysql_result_fetch },
	{ "fetch_all", mysql_result_fetch_all },
	{ "num_rows", mysql_result_num_rows },
	{ "num_fields", mysql_result_num_fields },
	{ "field_info", mysql_result_field_info },
	{ "__gc", mysql_result_gc },
	{ "__tostring", mysql_result_tostring },
	{ NULL, NULL }
};

LUAMOD_API int luaopen_mysql(lua_State* L)
{
	mysql_library_init(0, NULL, NULL);

	luaL_newmetatable(L, MYSQL_CONNECTION_MT);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_setfuncs(L, mysql_conn_methods, 0);

	luaL_newmetatable(L, MYSQL_RESULT_MT);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_setfuncs(L, mysql_result_methods, 0);

	luaL_newlib(L, mysql_lib);

	lua_pushstring(L, "version");
	lua_pushstring(L, mysql_get_client_info());
	lua_settable(L, -3);

	return 1;
}
