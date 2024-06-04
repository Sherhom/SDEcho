#include "trendiff.h"

Trendiff::Trendiff(PGconn* conn, string& timeColName, string& aggColName, vector<string>& focusColNames,
	string& aggFunc, string& table1Name, string& table2Name) {
	this->conn = conn;
	this->timeColName = timeColName;
	this->aggColName = aggColName;
	this->focusColNames = focusColNames;
	this->aggFunc = aggFunc;
	this->table1Name = table1Name;
	this->table2Name = table2Name;

	//transform(this->aggFunc.begin(), this->aggFunc.end(), this->aggFunc.begin(), ::tolower);
	if (this->aggFunc != "sum" && this->aggFunc != "count") {
		cout << "[Error   ] aggragate function error : " << this->aggFunc << endl;
		exit(1);
	}

	table = new Table(focusColNames);
}

void Trendiff::inputDate() {
	string selectCols = timeColName;
	if (this->aggFunc == "sum") {
		selectCols.append("," + aggColName);
	}
	else if(this->aggFunc == "count" ) {
		selectCols.append(", 1 as agg" );
	}
	
	for (string& s : focusColNames) {
		selectCols.append("," + s);
	}
	string sql1 = "select " + selectCols + " from " + table1Name;
	string sql2 = "select " + selectCols + " from " + table2Name;

	res = PQexec(conn, sql1.c_str());
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		cout << "[Error   ] when execuate sql: "  << sql1 << endl;
		PQclear(res);
		exit(1);
	}
	else {
		int row = PQntuples(res);
		int col = PQnfields(res);
		table->table1Size = row;
		table->totalSize = row;
		for (int i = 0; i < row; i++) {
			table->positive.push_back(true);
			table->timeCol.push_back(stoi(PQgetvalue(res, i, 0)));
			//cout << stoi(PQgetvalue(res, i, 0)) << endl;
			table->aggCol.push_back(stod(PQgetvalue(res, i, 1)));
			for (int j = 2; j < col; j++) {
				if (PQgetisnull(res, i, j)) {
					cout << "[Warning ] null at row: " << i << " col: " << j << endl;
					table->focusCols[j - 2].push_back("void");
				}
				else {
					table->focusCols[j - 2].push_back(PQgetvalue(res, i, j));
				}
			}
		}
		PQclear(res);
	}

	res = PQexec(conn, sql2.c_str());
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		cout << "[Error   ] when execuate sql: " << sql1 << endl;
		PQclear(res);
		exit(1);
	}
	else {
		int row = PQntuples(res);
		int col = PQnfields(res);
		table->table2Size = row;
		table->totalSize += row;
		for (int i = 0; i < row; i++) {
			table->positive.push_back(false);
			table->timeCol.push_back(stoi(PQgetvalue(res, i, 0)));
			table->aggCol.push_back(stod(PQgetvalue(res, i, 1)));
			for (int j = 2; j < col; j++) {
				if (PQgetisnull(res, i, j)) {
					cout << "[Warning ] null at row: " << i << " col: " << j << endl;
					table->focusCols[j - 2].push_back("void");
				}
				else {
					table->focusCols[j - 2].push_back(PQgetvalue(res, i, j));
				}
			}
		}
		PQclear(res);
	}

	cout << "[Input   ] input data finished, total : " << table->totalSize << " rows, table1 : " 
		<< table->table1Size << " rows, table2 : " << table->table2Size << " rows " <<  endl;
}

void Trendiff::computeD0(int ltime,int rtime) {
	int timeSize = table->timeDecodeMap.size();

	d0 = SubVec(timeSize);

	for (int i = 0; i < table->totalSize; i++) {
		if (table->timeEncoded[i] < ltime || table->timeEncoded[i] > rtime) {
			continue;
		}
		if (table->positive[i]) {
			d0[table->timeEncoded[i]] = d0[table->timeEncoded[i]] + table->aggCol[i];
		}
		else {
			d0[table->timeEncoded[i]] = d0[table->timeEncoded[i]] - table->aggCol[i];
		}
	}
	//cout << "[Log]sub vector d0 computed" << endl;
}

void Trendiff::cacheAllLeaf(int ltime, int rtime) {
	// < leaf explanation ,  <sub vector ,  abs(sum(aggcol))> >
	map<vector<int>, SubVec*> allLeafMap;

	int timeSize = table->timeDecodeMap.size();

	for (int i = 0; i < table->totalSize; i++) {
		if (table->timeEncoded[i] < ltime || table->timeEncoded[i] > rtime) {
			continue;
		}
		vector<int> leafExp(0);
		for (int j = 0; j < table->focusEncoded.size(); j++) {
			leafExp.push_back(table->focusEncoded[j][i]);
		}
		if (allLeafMap.count(leafExp) == 0) {
			allLeafMap[leafExp] = new SubVec(timeSize);
		}
		SubVec* pSub = allLeafMap[leafExp];

		if (table->positive[i]) {
			(*pSub)[table->timeEncoded[i]] = (*pSub)[table->timeEncoded[i]] + table->aggCol[i];
		}
		else {
			(*pSub)[table->timeEncoded[i]] = (*pSub)[table->timeEncoded[i]] - table->aggCol[i];
		}
	}

	string scheme;
	for (int i = 0; i < table->focusColNames.size(); i++) {
		scheme += (table->focusColNames[i] + " int, ");
	}
	scheme += ("proj float ");

	string dropsql = "drop table if exists " + allLeafTable;
	PQexec(conn, dropsql.c_str());

	cout << "[Midtable] execute sql:" << dropsql << endl;

	string creatsql = string("create table  ");
	creatsql.append(allLeafTable);
	creatsql.append("(" + scheme + ");");
	
	PQexec(conn, creatsql.c_str());
	cout << "[Midtable] execute sql:" << creatsql << endl;

	double d0Norm = d0.norm();
	//int count_left = 0;
	for (auto& iter : allLeafMap) {
		//if( (count_left++)%5000 == 0  ) cout << count_left << " / " << allLeafMap.size() << endl;
		string values;
		for (auto i : iter.first) {
			values.append(to_string(i) + ", ");
		}
		double proj = iter.second->innerProduct(d0) / d0Norm;
		values.append(to_string(proj));

		string insertsql = "insert into " + allLeafTable + " values(" + values + ")";
		PQexec(conn, insertsql.c_str());
	}

	for (auto& iter : allLeafMap) {
		SubVec* p = iter.second;
		delete p;
	}

	cout << "[Midtable] proj and abs sum value of all leaf explantions computed" << endl;
}

void Trendiff::cubeAllExps() {
	string focus;
	for (int i = 0; i < focusColNames.size();i++ ) {
		if (i == 0) focus.append(focusColNames[i]);
		else focus.append("," + focusColNames[i]);
	}
	string sql = "select ";
	sql.append(focus);
	sql.append(",sum(proj) as proj0 from ");
	sql.append(allLeafTable);
	sql.append(" group by cube(");
	sql.append(focus);
	sql.append(") order by proj0 desc");

	cout << "[Midtable] execute cube sql now:" << sql << endl;
	res = PQexec(conn, sql.c_str());
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		cout << "[Error   ] when execuate sql: " << sql << endl;
		PQclear(res);
		exit(1);
	}
	cout << "[Midtable] there are totally " << PQntuples(res) << " explanations" << endl;
}

void Trendiff::printAllExpValues() {
	int row = PQntuples(res);
	int col = PQnfields(res);
	for (int i = 0; i < row; i++) {
		for (int j = 0; j < col; j++) {
			cout << PQgetvalue(res, i, j) << "\t";
		}
		cout << endl;
	}
}

void Trendiff::FindTopkExplanations(int _k,int ltime,int rtime) {
	topk.setK(_k);

	double d0Norm = d0.norm();

	int actual_compute = 0;
	
	int row = PQntuples(res);
	int projIndex = focusColNames.size();

	for (int i = 0; i < row; i++) {
		vector<int> exp;
		bool isAllNULL = true;
		for (int j = 0; j < projIndex; j++) {
			if (PQgetisnull(res, i, j)) {
				exp.push_back(table->NULLVALUE);
			}
			else {
				isAllNULL = false;
				exp.push_back(stoi(PQgetvalue(res, i, j)));
			}
		}
		if (isAllNULL) continue;

		double proj = stod(PQgetvalue(res, i, projIndex));
	
		double threshold = topk.getHighestScore();

		bool can_proj_prune = ((d0Norm - proj) > (threshold * d0Norm));
		
		if (can_proj_prune) {
			break;
		}
		else {
			//cout << "[Compute ] exp:" << sexp << " need to be computed" << endl;
			actual_compute++;
		}

		double actualScore = table->computeAcutalScore(exp, d0,ltime,rtime);
		topk.insert(exp, actualScore);
	}

	cout << "[Result  ] need compute : " << actual_compute << endl;
}

void Trendiff::printTopkResult() {
	while (topk.isNotEmpty()) {
		cout << "[Topk    ] ";
		const Explantion& exp = topk.top();
		for (int j = 0; j < focusColNames.size(); j++) {
			cout << focusColNames[j] << "=" << table->focusDecodeMaps[j][exp.encodedValues[j]] << " ,";
		}
		cout << "Score= " << exp.score << endl;
		topk.pop();
	}
}

Trendiff::~Trendiff() {
	delete table;
}
