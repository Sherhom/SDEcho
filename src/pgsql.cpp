#include "pgsql.h"
#include <iostream>

PGsql::PGsql(const char* hostaddr, const char* port, const char* dbname, const char* user, const char* password) {
	conn = PQsetdbLogin(hostaddr, port, NULL, NULL, dbname, user, password);
	if (PQstatus(conn) == CONNECTION_BAD) {
		cout << "[Error   ] can not connect with datebase: "<< PQerrorMessage(conn) << endl;
		PQfinish(conn);
		exit(1);
	}
	else {
		cout << "[Connect ] connect datebase success" << endl;
	}
}

PGconn* PGsql::getConn() {
	return conn;
}

PGsql::~PGsql() {
	PQfinish(conn);
}