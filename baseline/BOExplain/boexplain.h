#pragma once

#include <libpq-fe.h>
#include <string>
#include <vector>
#include "table.h"
#include <iostream>
#include "SubVec.h"
#include <map>
#include <math.h>
#include "Topk.h"

using namespace std;

class Boexplain {
private:
	//name of time column
	string timeColName ;

	//name of aggragated column
	string aggColName ;

	//the columns to find explanations
	vector<string> focusColNames ;

	//aggragate function name
	string aggFunc ;

	//name of the first table
	string table1Name ;

	//name of the second table
	string table2Name ;

	PGconn* conn = NULL;

	PGresult* res = NULL;


public:
	Table* table = NULL;
	SubVec d0;
	Topk topk;

	Boexplain(PGconn* conn,string& timeColName,string& aggColName,vector<string>& focusColNames,
			string& aggFunc,string& table1Name,string& table2Name,int k);

	vector<Explantion> computed_exp_scores;
	set<vector<int>> computed_exps;

	bool ifallnull(vector<int>& exp);

	vector<int> rand_new_exp();

	void computeD0();

	void init_Data();

	//read date from datebase and init table
	void inputDate();

	vector<int>  getNextExp();

	void findTopkExps(int maxnochange);

	void printTopk();

	~Boexplain();
};
