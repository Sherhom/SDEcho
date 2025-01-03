#include <libpq-fe.h>
#include <iostream>
#include <string>
#include "pgsql.h"
#include "boexplain.h"
#include "Topk.h"
#include <chrono>
#include <fstream>


using namespace std;

//database-config
string DBcfg_path = "db_config.txt";
string hostaddr;
string port;
string dbname;
string user;
string password;

/*
 SELECT  table1Name TRENDIFF table2Name
 FOCUS ON focusColNames
 GROUP BY groupbyColName WITH aggFunc(aggColName)
 LIMIT k;
*/

//trendiff query config
string QueryCfg_path;
string groupbyColName;		//name of time column
string aggColName;		//name of aggragated column
vector<string> focusColNames ;		//the columns to find explanations
string aggFunc;					//aggragate function name:count sum only now
string table1Name ;		//name of the first table
string table2Name ;		//name of the second table
int k;								//the number of explanations you want to get
int order_int ;					//the max order of the explanation

time_t GetCurrentTimeMsec();
void Stringsplit(const string& str, const char split, vector<string>& res);
void getDBconfig();
void getQueryConfig();


int main(int argc,char* argv[]) {
	
	getDBconfig();
	
	if (argc <= 1) {
		cout << "[Error   ] please give the SQL config path" << endl;
		return -1;
	}
	QueryCfg_path= string(argv[1]);
	getQueryConfig();

	double wantScore = 0; 	
	
	cout << "query path : " << QueryCfg_path << endl; 
	cout << "k : " << k << endl;
	cout << "order : " << order_int << endl;


	PGsql pgsql(hostaddr.c_str(),port.c_str(),dbname.c_str(),user.c_str(),password.c_str());
	PGconn* conn = pgsql.getConn();

	Boexplain boexplain(conn,groupbyColName,aggColName,focusColNames,aggFunc,table1Name,table2Name,k);

	
	time_t t1 = GetCurrentTimeMsec();
	boexplain.inputDate();
	boexplain.table->encode();
	boexplain.computeD0();
	int maxnochange = int( pow(10,order_int/2 ) );
	if( maxnochange > 10000 ) maxnochange = 10000;
	boexplain.findTopkExps(  maxnochange   );
	boexplain.printTopk();
	time_t t2 = GetCurrentTimeMsec();

	cout << "[Timecost] timecost : " << (t2 - t1) << " ms " << endl;
	cout << "================================" << endl;	

}


time_t GetCurrentTimeMsec() {
	auto time = chrono::time_point_cast<chrono::milliseconds>(chrono::system_clock::now());
	time_t timestamp = time.time_since_epoch().count();
	return timestamp;
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

void substringAfterColon(string& str) {
    size_t colonPos = str.find(':');
    if (colonPos == std::string::npos) {
        return;
    }
    str = str.substr(colonPos + 1);
}

void getDBconfig(){
	char buffer[2048];
	ifstream in(DBcfg_path,ios::in);
	in.getline(buffer,2048); hostaddr = string(buffer); substringAfterColon(hostaddr);
	in.getline(buffer,2048); port = string(buffer); substringAfterColon(port);
	in.getline(buffer,2048); dbname = string(buffer); substringAfterColon(dbname);
	in.getline(buffer,2048); user = string(buffer); substringAfterColon(user);
	in.getline(buffer,2048); password = string(buffer); substringAfterColon(password);
	in.close();
}

void getQueryConfig(){
	char buffer[2048];
	ifstream in(QueryCfg_path,ios::in);
	in.getline(buffer,2048); string focus_s(buffer);
	in.getline(buffer,2048); table1Name = string(buffer);
	in.getline(buffer,2048); table2Name = string(buffer);
	in.getline(buffer,2048); groupbyColName = string(buffer);
	in.getline(buffer,2048); aggColName = string(buffer);
	in.getline(buffer,2048); aggFunc = string(buffer);
	in.getline(buffer,2048); k = stoi(string(buffer));
	in.getline(buffer,2048); order_int = stoi(string(buffer));
	in.close();

	vector<string> focusALL ;
	Stringsplit(focus_s,'|',focusALL);

	if( order_int > focusALL.size() ){
		cout << "[Error   ] explanation order too big " << endl;
		return;
	}	
	for(int i = 0 ;i < order_int ;i++){
		focusColNames.push_back(focusALL[i]);
	}
}