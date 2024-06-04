#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <unordered_map>
#include <set>
#include <fstream>
#include "SubVec.h"

using namespace std;

class Table {
public:
	//the names of focused on columns 
	vector<string> focusColNames;

	//to distinguish table 1 record from table 2 record , 'true'  is  table 1 record  
	vector<bool> positive;

	//the values of time column
	vector<int> timeCol;

	//the values of aggragrated column
	vector<double> aggCol;

	//the values of fucused on columns
	vector<vector<string>> focusCols;

	//rows of table1 
	int table1Size = 0;

	//rows of table2
	int table2Size = 0;

	//total rows
	int totalSize = 0;

	//encode null to -1;
	int NULLVALUE = -1;

	//time col encode mapping
	unordered_map<int, int> timeEncodeMap;
	//time col decode mapping
	unordered_map<int, int> timeDecodeMap;
	//encoded time column
	vector<int> timeEncoded;


	//focus columns encode mappings
	vector<unordered_map<string, int>> focusEncodeMaps;
	//focus columns decode mappings
	vector<unordered_map<int, string>> focusDecodeMaps;
	//encoded focus columns
	vector<vector<int>> focusEncoded;


	int max_time = 0;

	Table(vector<string>& focusNames);

	//print the original table (unencoded)
	void printOriginTable();
	
	//encode the time column and focus columns 
	void encode();

	//print the encode maps
	void printEncodeMap(string path);
	void printTimeMap();

	//output the encoded table to path
	void outputEncodedTable(string path);

	//check the record satisfy the explanation or not
	bool isRecordOfExp(int index, const vector<int>& exp);

	//compute the actual score of the explanation
	double computeAcutalScore(const vector<int>& explanation, SubVec& d0,int ltime,int rtime,double& abssum);
};