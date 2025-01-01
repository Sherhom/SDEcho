#pragma once

#include <libpq-fe.h>

using namespace std;

class PGsql {
private:
	PGconn* conn = NULL;

public:
	//get connection with datebase and set conn
	PGsql(const char* hostaddr,const char* port,const char* dbname, const char* user, const char* password);

	PGconn* getConn();

	~PGsql();
};