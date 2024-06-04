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

	left_order = focusColNames.size();
	
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

	string scheme;
	for (int i = 0; i < focusColNames.size(); i++) {
		if (i == 0) scheme = table->focusColNames[i] + " int ";
		else {
			scheme += ("," + table->focusColNames[i] + " int ");
		}
	}

	string dropsql = "drop table if exists " + allLeafTable;
	PQexec(conn, dropsql.c_str());

	//cout << "[Midtable] execute sql:" << dropsql << endl;

	string creatsql = string("create table  ");
	creatsql.append(allLeafTable);
	creatsql.append("(" + scheme + ");");

	PQexec(conn, creatsql.c_str());
	cout << "[Midtable] execute sql:" << creatsql << endl;

	set<vector<int>> allleafSet;
	for (int i = 0; i < table->totalSize; i++) {
		if (table->timeEncoded[i] < ltime || table->timeEncoded[i] > rtime) {
			continue;
		}
		vector<int> leafExp(0);
		for (int j = 0; j < focusColNames.size(); j++) {
			leafExp.push_back(table->focusEncoded[j][i]);
		}
		allleafSet.insert(leafExp);
	}

	int count_leaf = 0;
	int allleaf_num = allleafSet.size();

	for (auto& v : allleafSet) {
		if ((count_leaf++) % 5000 == 0) cout << count_leaf << "/" << allleaf_num << endl;
		string values;
		for (int j = 0; j < v.size(); j++) {
			if (j == 0) values = to_string(v[j]);
			else {
				values.append( ","+ to_string(v[j]));
			}
		}
		
		string insertsql = "insert into " + allLeafTable + " values(" + values + ")";
		PQexec(conn, insertsql.c_str());
	}

	cout << "[Midtable]  all leaf explantions cached" << endl;
}

void Trendiff::cubeAllExps() {
	string focus;
	for (int i = 0; i < left_order;i++ ) {
		if (i == 0) focus.append(focusColNames[i]);
		else focus.append("," + focusColNames[i]);
	}
	string sql = "select ";
	sql.append(focus);
	sql.append(" from ");
	sql.append(allLeafTable);
	sql.append(" group by cube(");
	sql.append(focus);
	sql.append(") ");

	std::cout << "[Midtable] execute cube sql now:" << sql << endl;
	res = PQexec(conn, sql.c_str());
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		std::cout << "[Error   ] when execuate sql: " << sql << endl;
		PQclear(res);
		exit(1);
	}
	total_exps_num = PQntuples(res);
	std::cout << "[Midtable] there are totally " << total_exps_num << " explanations" << endl;
}

void Trendiff::getAllOrderExps() {
	all_order_exps.resize(focusColNames.size() + 1);

	int row = PQntuples(res);
	
	for (int i = 0; i < row; i++) {
		vector<int> exp;
		int order = 0;
		for (int j = 0; j < focusColNames.size(); j++) {
			if (PQgetisnull(res, i, j)) {
				exp.push_back(table->NULLVALUE);
			}
			else {
				order++;
				exp.push_back(stoi(PQgetvalue(res, i, j)));
			}
		}
		all_order_exps[order].push_back(exp);
	}
	/*
	for (int i = 0; i < all_order_exps.size(); i++) {
		for (int j = 0; j < all_order_exps[i].size(); j++) {
			for (auto k : all_order_exps[i][j]) {
				std::cout << k << "\t";
			}
			std::cout << std::endl;
		}
		std::cout << std::endl;
	}
	*/
}

void Trendiff::computeOneOrderExps(int _k, int ltime, int rtime) {
	topk.setK(_k);

	for (int i = 0; i < all_order_exps[1].size(); i++) {
		vector<int>& exp = all_order_exps[1][i];
		double abssum = 0.0;
		double actualScore = table->computeAcutalScore(exp, d0, ltime, rtime,abssum);
		topk.insert(exp, actualScore);
		actual_compute++;
	}

	std::cout << "[OneOrder] one order need computed" << endl;
}

void Trendiff::ConsideAllExps(int ltime, int rtime) {
	int counter = 0;
	for (int order = 2; order <=  focusColNames.size() ; order++) {
		cout << "begin compute order :" << order << endl;
		int this_order_size = all_order_exps[order].size();
		for (int index = 0; index < this_order_size; index++) {
			counter++;
			if (counter % 1000 == 0) { cout << (counter) << endl; }
			vector<int> exp = all_order_exps[order][index];
			bool can_be_pruned = false;
			for (int j = 0; j < focusColNames.size(); j++) {
				if (exp[j] != -1) {
					vector<int> parent_exp = exp;
					parent_exp[j] = -1;
					if (pruned_exps.count(parent_exp)) {
						can_be_pruned = true;
						break;
					}
				}
			}
			if (can_be_pruned) {
				pruned_exps.insert(exp);
				continue;
			}
			double abssum = 0.0;
			double actualScore = table->computeAcutalScore(exp, d0, ltime, rtime,abssum);
			topk.insert(exp, actualScore);
			actual_compute++;
			double threshold = topk.getHighestScore();
			double d0norm = d0.norm();
			bool can_abssum_pruned = ((d0norm - abssum) >= (threshold * d0norm));
			if (can_abssum_pruned) {
				pruned_exps.insert(exp);
			}
		}
	}
}

void Trendiff::printTopkResult() {
	cout << "[Result  ] actual compute: " << actual_compute << " / " << total_exps_num << endl;
	cout << "[Topk    ] threshold : " << topk.getHighestScore() << endl;
	while (topk.isNotEmpty()) {
		cout << "[Topk    ] ";
		const Explantion& exp = topk.top();
		for (int j = 0; j < focusColNames.size(); j++) {
			cout << focusColNames[j] << "=" << table->focusDecodeMaps[j][exp.encodedValues[j]] << " ,";
		}
		cout << "Score= " << exp.score << endl;
		topk.pop();
	}
	cout << "[Topk    ] d0norm : " << d0.norm() << endl;
}

Trendiff::~Trendiff() {
	delete table;
}
