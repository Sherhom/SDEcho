#include <libpq-fe.h>
#include <iostream>
#include <string>
#include "pgsql.h"
#include "trendiff.h"
#include "Topk.h"
#include <chrono>
#include <fstream>

using namespace std;

//database-config
const char* hostaddr = "*";
const char* port = "*";
const char* dbname = "*";
const char* user = "*";
const char* password = "*";

/*
 SELECT  table1Name TRENDIFF table2Name
 FOCUS ON focusColNames
 GROUP BY timeColName WITH aggFunc(aggColName)
 LIMIT k;
*/

//trendiff query config
string timeColName = "groupcol";		//name of time column
string aggColName = "aggcol";		//name of aggragated column
vector<string> focusColNames ;		//the columns to find explanations
string aggFunc = "sum";					
string table1Name ;		//name of the first table
string table2Name ;		//name of the second table
int k = 5;								//the number of explanations you want to get

time_t GetCurrentTimeMsec() {
	auto time = chrono::time_point_cast<chrono::milliseconds>(chrono::system_clock::now());
	time_t timestamp = time.time_since_epoch().count();
	return timestamp;
}

void outputEncodedTable(string mapPath,string tablePath) {
	PGsql pgsql(hostaddr, port, dbname, user, password);
	PGconn* conn = pgsql.getConn();
	Trendiff trendiff(conn, timeColName, aggColName, focusColNames, aggFunc, table1Name, table2Name);

	trendiff.inputDate();
	trendiff.table->encode();
	trendiff.table->printEncodeMap(mapPath);
	trendiff.table->outputEncodedTable(tablePath);
}

void Stringsplit(const string& str, const char split, vector<string>& res)
{
	if (str == "")		return;
	string strs = str + split;
	size_t pos = strs.find(split);

	while (pos != strs.npos)
	{
		string temp = strs.substr(0, pos);
		res.push_back(temp);
		strs = strs.substr(pos + 1, strs.size());
		pos = strs.find(split);
	}
}


int main(int argc,char* argv[]) {
	
	string path = "1.txt";
	int order_int = 5;
	
	if (argc <= 2) {
		return -1;
	}
	else {
		path = string(argv[1]);
	}
	order_int = stoi(string(argv[2]));
	

	//cout << path << endl;
	char buffer[2048];
	ifstream in(path,ios::in);
	in.getline(buffer,2048); string focus_s(buffer);
	in.getline(buffer,2048); table1Name = string(buffer);
	in.getline(buffer,2048); table2Name = string(buffer);
	in.close();

	vector<string> focusALL ;
	Stringsplit(focus_s,'|',focusALL);
	
	for(int i = 0 ;i < order_int ;i++){
		focusColNames.push_back(focusALL[i]);
	}

	PGsql pgsql(hostaddr,port,dbname,user,password);
	PGconn* conn = pgsql.getConn();
	Trendiff trendiff(conn,timeColName,aggColName,focusColNames,aggFunc,table1Name,table2Name);

	
	time_t t1 = GetCurrentTimeMsec();
	trendiff.inputDate();
	trendiff.table->encode();
	time_t t2 = GetCurrentTimeMsec();


	int ltime = 0;
	int rtime = trendiff.table->max_time ;
	
	
	trendiff.computeD0(ltime,rtime);
	trendiff.computeOneOrderExps(k, ltime, rtime);
	time_t t3 = GetCurrentTimeMsec();

	trendiff.cacheAllLeaf(ltime, rtime);
	trendiff.cubeAllExps();
	trendiff.ConsideAllExps(ltime, rtime);
	time_t t4 = GetCurrentTimeMsec();


	trendiff.printTopkResult();
	

	cout << "[TimeCost] input data and encode: " << (t2 - t1) << "ms"<< endl;
	cout << "[TimeCost] dimension prune : " << (t3 - t2) << "ms" << endl;
	cout << "[TimeCost] compute actual score of exps: " << (t4 - t3) << "ms" << endl;
	cout << "[TimeCost] total time cost: " << (t4 - t1) << "ms" << endl;
	cout << "path : " << path <<" order : " << order_int << endl;
	cout << "=========================" << endl;	
}

