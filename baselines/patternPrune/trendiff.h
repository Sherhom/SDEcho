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

class Trendiff {
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

	//the sub vector of two time series without removing any record
	SubVec d0;

	//name of table of all leaf explanations for mid results
	string allLeafTable = "allLeafProjAndSum";

	
	//the heap to save the present top-k explantions
	Topk topk;


public:
	Table* table = NULL;

	Trendiff(PGconn* conn,string& timeColName,string& aggColName,vector<string>& focusColNames,
			string& aggFunc,string& table1Name,string& table2Name);

	//read date from datebase and init table
	void inputDate();

	//compute the d0 vector
	void computeD0();
	void computeD0(int ltime, int rtime);

	//computer the proj and sum value of all leaf explanations and save to datebase
	void cacheAllLeaf();
	void cacheAllLeaf(int ltime, int rtime);

	//use cube to compute the proj and sum value of all explanations and save to PGresult
	void cubeAllExps();

	//show the proj and sum value of all explantions
	void printAllExpValues();

	//find topk explanations
	void FindTopkExplanations(int _k);
	void FindTopkExplanations(int _k,int ltime, int rtime);

	//show result
	void printTopkResult();

	~Trendiff();
};