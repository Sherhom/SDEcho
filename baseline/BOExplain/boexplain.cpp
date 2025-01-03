#include "boexplain.h"
#include <ctime>
#include <random>
#include <algorithm>

Boexplain::Boexplain(PGconn* conn, string& timeColName, string& aggColName, vector<string>& focusColNames,
	string& aggFunc, string& table1Name, string& table2Name,int k) {
	this->conn = conn;
	this->timeColName = timeColName;
	this->aggColName = aggColName;
	this->focusColNames = focusColNames;


	while (!aggFunc.empty() && std::isspace(static_cast<unsigned char>(aggFunc.back()))) {
        aggFunc.pop_back(); 
    }
	this->aggFunc = aggFunc;
	
	this->table1Name = table1Name;
	this->table2Name = table2Name;

	topk.setK(k);

	//modify here!
	//int totalColNum = focusColNames.size();
	//right_order = 0;
	//left_order = totalColNum - right_order;
		
	//transform(this->aggFunc.begin(), this->aggFunc.end(), this->aggFunc.begin(), ::tolower);
	if (this->aggFunc != "sum" && this->aggFunc != "count") {
		cout << "[Error   ] aggragate function error : " << this->aggFunc << endl;
		exit(1);
	}

	table = new Table(focusColNames);
	
}

void Boexplain::inputDate() {
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

void Boexplain::computeD0() {
	int timeSize = table->timeDecodeMap.size();

	d0 = SubVec(timeSize);

	for (int i = 0; i < table->totalSize; i++) {
		if (table->positive[i]) {
			d0[table->timeEncoded[i]] = d0[table->timeEncoded[i]] + table->aggCol[i];
		}
		else {
			d0[table->timeEncoded[i]] = d0[table->timeEncoded[i]] - table->aggCol[i];
		}
	}
	//cout << "[Log]sub vector d0 computed" << endl;
}

bool Boexplain::ifallnull(vector<int>& exp) {
	for (int i = 0; i < exp.size(); i++) {
		if (exp[i] != -1) return false;
	}
	return true;
}

vector<int> Boexplain::rand_new_exp() {
	int times = 0;
	vector<int> res;

	std::default_random_engine e;
	e.seed(time(0));

	while (times < 100000) {
		times++;
		for (int j = 0; j < focusColNames.size(); j++) {
			int max_value = table->focusEncodeMaps[j].size();
			res.push_back(e() % max_value - 1);
		}
		if (ifallnull(res) == false && computed_exps.count(res) == 0) {
			computed_exps.insert(res);
			return res;
		}
		else {
			res.clear();
		}
	}
	
	cout << "[warning ]  rand new exp error " << endl;
	res.clear();
	return res;
}

void Boexplain::init_Data() {
	int init_point_num = 10;
	for (int i = 0; i < init_point_num; i++) {
		
		vector<int> exp = rand_new_exp();
		
		double score = table->computeAcutalScore(exp, d0);
		Explantion exp_e(exp, score);
		computed_exp_scores.push_back(exp_e);
		topk.insert(exp, score);
	}

	/*
	for (auto& exp_e : computed_exp_scores) {
		for (auto i : exp_e.encodedValues) {
			cout << i << "|";
		}
		cout << "\t" << exp_e.score << endl;
	}
	*/
}

vector<int> Boexplain::getNextExp() {
	double yamma = 0.3;
	vector<double> scores;
	for (auto& e : computed_exp_scores) {
		scores.push_back(e.score);
	}
	sort(scores.begin(), scores.end());
	double threshold = scores[scores.size() * yamma + 1];
	//cout << "threshold: " << threshold << endl;
	vector<unordered_map<int, int>> good_value_count;
	vector<unordered_map<int, int>> bad_value_count;
	for (int j = 0; j < focusColNames.size(); j++) {
		good_value_count.push_back(unordered_map<int, int>());
		bad_value_count.push_back(unordered_map<int, int>());
		int max_value = table->focusEncodeMaps[j].size() - 2;
		for (int k = -1; k <= max_value; k++) {
			good_value_count[j][k] = 1;
			bad_value_count[j][k] = 1;
		}
	}

	for (auto& exp_e : computed_exp_scores) {
		if (exp_e.score <= threshold) {
			for (int j = 0; j < focusColNames.size(); j++) {
				good_value_count[j][exp_e.encodedValues[j]]++;
			}
		}
		else {
			for (int j = 0; j < focusColNames.size(); j++) {
				bad_value_count[j][exp_e.encodedValues[j]]++;
			}
		}
	}


	vector<int> res;
	for (int j = 0; j < focusColNames.size(); j++) {
		int best_index = -1;
		double best_score = 0;
		//cout << table->focusEncodeMaps[j].size() << endl;
		int max_value = table->focusEncodeMaps[j].size()-2;
		for (int k = -1 ; k <= max_value ; k++) {
			//cout << "col: " << j << " value: " << k << " good: " << good_value_count[j][k] << " bad : " << bad_value_count[j][k] << endl;
			double now_score = 1.0 * good_value_count[j][k]  / bad_value_count[j][k] ;
			if (now_score > best_score) {
				best_score = now_score;
				best_index = k;
			}
		}
		res.push_back(best_index);
	}

	while (computed_exps.count(res)) {
		for (int i = 0; i < res.size(); i++) {
			if (res[i] != -1) {
				res[i] = -1;
				break;
			}
		}
	}

	if (ifallnull(res)) {
		res.clear();
	}
	
	return res;

}

void Boexplain::findTopkExps(int maxnochange) {
	init_Data();
	double threshold = topk.getHighestScore();

	int times = 0;
	int nochange_times = 0;
	while ( nochange_times <= maxnochange ) {
		if (times++ % 100 == 0) {
			cout << "[log] literate times : " << times;
			cout << " threshold now : " << threshold;
			cout << " nochange times : " << nochange_times  << endl;
		}
		vector<int> nextExp = getNextExp();
		if (nextExp.size() != 0) {
			computed_exps.insert(nextExp);
			double score = table->computeAcutalScore(nextExp, d0);
			Explantion exp_e(nextExp, score);
			computed_exp_scores.push_back(exp_e);
			topk.insert(nextExp, score);
			
			//cout << "[log] next exp ";
			//for (auto i : nextExp) {
			//	cout << i << " | ";
			//}
			//cout << "score : " << score << endl;
		}
		vector<int> randExp = rand_new_exp();
		if( randExp.size() != 0 ){
			double score = table->computeAcutalScore(randExp, d0);
			Explantion exp_e(randExp, score);
			computed_exp_scores.push_back(exp_e);
			topk.insert(randExp, score);
		}
		//cout << "[log] rand exp ";
		//for (auto i : randExp) {
		//	cout << i << " | ";
		//}
		//cout << "score : " << score << endl;
		if (threshold > topk.getHighestScore()) {
			nochange_times = 0;
		}
		else {
			nochange_times++;
		}

		threshold = topk.getHighestScore();
	}

}

void Boexplain::printTopk() {
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

Boexplain::~Boexplain() {
	delete table;
}
