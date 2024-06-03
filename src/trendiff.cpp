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
	vector<double> t1agg(timeSize,0);
	vector<double> t2agg(timeSize,0);	

	for (int i = 0; i < table->totalSize; i++) {
		if (table->timeEncoded[i] < ltime || table->timeEncoded[i] > rtime) {
			continue;
		}
		if (table->positive[i]) {
			d0[table->timeEncoded[i]] = d0[table->timeEncoded[i]] + table->aggCol[i];
			t1agg[table->timeEncoded[i]] += table->aggCol[i];
		}
		else {
			d0[table->timeEncoded[i]] = d0[table->timeEncoded[i]] - table->aggCol[i];
			t2agg[table->timeEncoded[i]] += table->aggCol[i];
		}
	}
	
	cout <<" [d0]   [t1]    [t2]"   << endl;
	for(int i = 0 ;i < timeSize ;i++){
		cout << d0[i] << " " << t1agg[i] << " " << t2agg[i] << endl ;
	} 
	cout << endl;
		
	//cout << "[Log]sub vector d0 computed" << endl;
}

void Trendiff::splitColumns() {
	int row = table->totalSize;
	double total_agg = 0.0;
	for (int i = 0; i < row; i++) {
		total_agg += table->aggCol[i];
	}

	vector<int> col_value_nums;
	int col_Nums = focusColNames.size();
	for (int j = 0; j < col_Nums; j++) {
		col_value_nums.push_back(table->focusEncodeMaps[j].size());
	}

	double values_threshold = total_agg / (0.4 * d0.norm());
	int first_index_right = col_Nums - 1;
	for (int j = first_index_right; j >= 0; j--) {
		if (col_value_nums[j] > values_threshold) {
			if (j == first_index_right) {
				first_index_right--;
				continue;
			}
			int temp_col_value = col_value_nums[j];
			col_value_nums[j] = col_value_nums[first_index_right];
			col_value_nums[first_index_right] = temp_col_value;

			auto temp_focuscols = std::move(table->focusCols[j]);
			table->focusCols[j] = std::move(table->focusCols[first_index_right]);
			table->focusCols[first_index_right] = std::move(temp_focuscols);

			auto temp_focuscolnames = std::move(table->focusColNames[j]);
			table->focusColNames[j] = std::move(table->focusColNames[first_index_right]);
			table->focusColNames[first_index_right] = std::move(temp_focuscolnames);

			auto temp_focusencodemap = std::move(table->focusEncodeMaps[j]);
			table->focusEncodeMaps[j] = std::move(table->focusEncodeMaps[first_index_right]);
			table->focusEncodeMaps[first_index_right] = std::move(temp_focusencodemap);

			auto temp_focusdecodemap = std::move(table->focusDecodeMaps[j]);
			table->focusDecodeMaps[j] = std::move(table->focusDecodeMaps[first_index_right]);
			table->focusDecodeMaps[first_index_right] = std::move(temp_focusdecodemap);

			auto temp_focusencoded = std::move(table->focusEncoded[j]);
			table->focusEncoded[j] = std::move(table->focusEncoded[first_index_right]);
			table->focusEncoded[first_index_right] = std::move(temp_focusencoded);

			auto temp_string = focusColNames[j];
			focusColNames[j] = focusColNames[first_index_right];
			focusColNames[first_index_right] = temp_string;

			first_index_right--;
		}
	}

	left_order = first_index_right + 1;
	if( left_order > 12 ) left_order = 12;
	right_order = col_Nums - left_order;
	cout << "[Split   ] left order: " << left_order << " right order: " << right_order << endl;
}

void Trendiff::cacheAllLeaf(int ltime, int rtime) {
	// < leaf explanation ,  sub vector* >
	map<vector<int>, SubVec*> allLeafMap;

	int timeSize = table->timeDecodeMap.size();

	for (int i = 0; i < table->totalSize; i++) {
		if (table->timeEncoded[i] < ltime || table->timeEncoded[i] > rtime) {
			continue;
		}
		vector<int> leafExp(0);
		for (int j = 0; j < left_order; j++) {
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
	for (int i = 0; i < left_order; i++) {
		scheme += (table->focusColNames[i] + " int, ");
	}
	scheme += ("proj float ");

	string dropsql = "drop table if exists " + allLeafTable;
	PQexec(conn, dropsql.c_str());

	//cout << "[Midtable] execute sql:" << dropsql << endl;

	string creatsql = string("create table  ");
	creatsql.append(allLeafTable);
	creatsql.append("(" + scheme + ");");
	
	PQexec(conn, creatsql.c_str());
	cout << "[Midtable] execute sql:" << creatsql << endl;

	//cout << "[Midtable] there are : " << allLeafMap.size() << " leaf exps " << endl;

	int count_leaf = 0;
	int allleaf_num = allLeafMap.size();
	double d0Norm = d0.norm();
	for (auto& iter : allLeafMap) {
		if ((count_leaf++) % 10000 == 0) cout << count_leaf << "/" << allleaf_num << endl;
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

	cout << "[Midtable] proj value of all leaf explantions computed" << endl;
}

void Trendiff::cubeAllExps() {
	string focus;
	for (int i = 0; i < left_order;i++ ) {
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
	int projIndex = left_order;

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

		for (int j = left_order; j < focusColNames.size(); j++) {
			exp.push_back(table->NULLVALUE);
		}

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

	cout << "[Result  ] left order need compute : " << actual_compute << endl;
}

void Trendiff::AbssumOfRightOrderLeaf(int ltime,int rtime) {
	//=================leaf exps of  right order and its abssum=============================
	map<vector<int>, double> allLeafMap;

	int totalColNum = focusColNames.size();
	int timeSize = table->timeDecodeMap.size();

	for (int i = 0; i < table->totalSize; i++) {
		if (table->timeEncoded[i] < ltime || table->timeEncoded[i] > rtime) {
			continue;
		}
		vector<int> leafExp(0);
		for (int j = left_order; j < totalColNum; j++) {
			leafExp.push_back(table->focusEncoded[j][i]);
		}
		if (allLeafMap.count(leafExp) == 0) {
			allLeafMap[leafExp] = 0.0;
		}
		allLeafMap[leafExp] += abs(table->aggCol[i]);
	}

	string scheme;
	for (int i = left_order; i < totalColNum; i++) {
		scheme += (table->focusColNames[i] + " int, ");
	}
	scheme += ("abssum float ");

	string dropsql = "drop table if exists " + allLeafTable;
	PQexec(conn, dropsql.c_str());

	cout << "[Midtable] execute sql:" << dropsql << endl;

	string creatsql = string("create table  ");
	creatsql.append(allLeafTable);
	creatsql.append("(" + scheme + ");");

	PQexec(conn, creatsql.c_str());
	cout << "[Midtable] execute sql:" << creatsql << endl;

	cout << "[Midtable] there are : " << allLeafMap.size() << " leaf exps " << endl;
	
	int count_leaf = 0;

	for (auto& iter : allLeafMap) {
		if ((count_leaf++) % 1000 == 0) cout << count_leaf << endl;
		string values;
		for (auto i : iter.first) {
			values.append(to_string(i) + ", ");
		}

		values.append(to_string(iter.second));

		string insertsql = "insert into " + allLeafTable + " values(" + values + ")";
		PQexec(conn, insertsql.c_str());
	}

	cout << "[Midtable] right order abs sum value of all leaf explantions computed" << endl;
}

void Trendiff::cubeAllAbssum() {
	string focus;
	int totalColNum = focusColNames.size();
	for (int i = left_order; i < totalColNum; i++) {
		if (i == left_order) focus.append(focusColNames[i]);
		else focus.append("," + focusColNames[i]);
	}
	string sql = "select ";
	sql.append(focus);
	sql.append(",sum(abssum) as abssum0 from ");
	sql.append(allLeafTable);
	sql.append(" group by cube(");
	sql.append(focus);
	sql.append(") order by abssum0 desc");
	
	PQclear(res);
	cout << "[Midtable] execute cube sql now:" << sql << endl;
	res = PQexec(conn, sql.c_str());
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		cout << "[Error   ] when execuate sql: " << sql << endl;
		PQclear(res);
		exit(1);
	}
	cout << "[Midtable] there are totally " << PQntuples(res) << " explanations" << endl;
}

bool Trendiff::isRecordOfExpRight(int index, vector<int>& exp_right) {
	int col = exp_right.size();
	for (int j = 0; j < col; j++) {
		if (exp_right[j] == table->NULLVALUE) {
			continue;
		}
		else {
			if (exp_right[j] != table->focusEncoded[j+left_order][index]) {
				return false;
			}
		}
	}
	return true;
}

void Trendiff::projOfRightOrderLeaf(vector<int>& exp_right, int ltime, int rtime) {
	// < leaf explanation ,  sub vector* >
	map<vector<int>, SubVec*> allLeafMap;

	int timeSize = table->timeDecodeMap.size();

	for (int i = 0; i < table->totalSize; i++) {
		if (table->timeEncoded[i] < ltime || table->timeEncoded[i] > rtime) {
			continue;
		}
		if (!isRecordOfExpRight(i, exp_right)) {
			continue;
		}

		vector<int> leafExp(0);
		for (int j = 0; j < left_order; j++) {
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
	for (int i = 0; i < left_order; i++) {
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
	for (auto& iter : allLeafMap) {
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

	cout << "[Midtable] proj value of all leaf explantions computed" << endl;
}

void Trendiff::cubeAllproj() {
	string focus;
	for (int i = 0; i < left_order; i++) {
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
	res_right = PQexec(conn, sql.c_str());
	if (PQresultStatus(res_right) != PGRES_TUPLES_OK) {
		cout << "[Error   ] when execuate sql: " << sql << endl;
		PQclear(res_right);
		exit(1);
	}
	cout << "[Midtable] there are totally " << PQntuples(res_right) << " explanations" << endl;
}

void Trendiff::computeDeeperExps(vector<int>& exp_right, int ltime, int rtime) {
	projOfRightOrderLeaf(exp_right, ltime, rtime);
	cubeAllproj();
	double d0Norm = d0.norm();

	int actual_compute = 0;

	int row = PQntuples(res_right);
	int projIndex = left_order;

	for (int i = 0; i < row; i++) {
		vector<int> exp_total;
		for (int j = 0; j < projIndex; j++) {
			if (PQgetisnull(res_right, i, j)) {
				exp_total.push_back(table->NULLVALUE);
			}
			else {
				exp_total.push_back(stoi(PQgetvalue(res_right, i, j)));
			}
		}

		for (int j = left_order; j < focusColNames.size(); j++) {
			exp_total.push_back(exp_right[j-left_order]);
		}

		double proj = stod(PQgetvalue(res_right, i, projIndex));

		double threshold = topk.getHighestScore();

		bool can_proj_prune = ((d0Norm - proj) > (threshold * d0Norm));

		if (can_proj_prune) {
			break;
		}
		else {
			//cout << "[Compute ] exp:" << sexp << " need to be computed" << endl;
			actual_compute++;
		}

		double actualScore = table->computeAcutalScore(exp_total, d0, ltime, rtime);
		topk.insert(exp_total, actualScore);
	}
	PQclear(res_right);
	cout << "[Result  ] need compute : " << actual_compute << endl;
}

void Trendiff::dimensionPrune(int ltime, int rtime) {
	//  < value ,  abssum >
	vector<unordered_map<int, double>> allabssumMap;
	int totalColNum = focusColNames.size();
	for (int j = left_order; j < totalColNum; j++) {
		unordered_map<int, double> m;
		allabssumMap.push_back(m);
	}

	for (int i = 0; i < table->totalSize; i++) {
		if (table->timeEncoded[i] < ltime || table->timeEncoded[i] > rtime) {
			continue;
		}
		for (int j = left_order; j < totalColNum; j++) {
			if (allabssumMap[j - left_order].count(table->focusEncoded[j][i]) == 0) {
				allabssumMap[j - left_order][table->focusEncoded[j][i]] = 0.0;
			}
			allabssumMap[j - left_order][table->focusEncoded[j][i]] += abs(table->aggCol[i]);
		}
	}

	double threshold = topk.getHighestScore();
	double d0Norm = d0.norm();

	for (int j = right_order - 1; j >= 0; j--) {
		double max_abssum = 0;
		for (auto& iter : allabssumMap[j]) {
			if (iter.second > max_abssum) {
				max_abssum = iter.second;
			}
		}
		cout << "[dimension] threshold : " << threshold << " d0norm :" << d0Norm << endl;
		cout << "[dimension] abssum :" << max_abssum << "  d0norm - threshold * d0norm : " << (d0Norm - d0Norm * threshold) << endl;
		cout << "[dimension] col now: " << focusColNames[j + left_order] << endl;

		if ((d0Norm - max_abssum) < (d0Norm * threshold)) {
			cout << "[dimension] can not be pruned" << endl;
			continue;
		}
		cout << "[dimension]  can be pruned " << endl;

		right_order--;
		focusColNames.erase(focusColNames.begin() + left_order + j);
		table->focusColNames.erase(table->focusColNames.begin() + left_order + j);
		table->focusCols.erase(table->focusCols.begin() + left_order + j);
		table->focusEncodeMaps.erase(table->focusEncodeMaps.begin() + left_order + j);
		table->focusDecodeMaps.erase(table->focusDecodeMaps.begin() + left_order + j);
		table->focusEncoded.erase(table->focusEncoded.begin() + left_order + j);
	}


}

void Trendiff::HigeOrderCompute(int ltime, int rtime) {
	dimensionPrune(ltime, rtime);

	if (right_order <= 0) return;
	AbssumOfRightOrderLeaf(ltime, rtime);
	cubeAllAbssum();

	double d0Norm = d0.norm();

	int need_down = 0;

	int row = PQntuples(res);
	int abssumIndex = right_order;

	//conside all exps at right order 
	for (int i = 0; i < row; i++) {
		vector<int> exp_right;
		bool isAllNULL = true;
		for (int j = 0; j < right_order; j++) {
			if (PQgetisnull(res, i, j)) {
				exp_right.push_back(table->NULLVALUE);
			}
			else {
				isAllNULL = false;
				exp_right.push_back(stoi(PQgetvalue(res, i, j)));
			}
		}
		if (isAllNULL) continue;

		double abssum = stod(PQgetvalue(res, i, abssumIndex));

		double threshold = topk.getHighestScore();

		bool can_abssum_prune = ((d0Norm - abssum) > (threshold * d0Norm));

		if (can_abssum_prune) {
			cout << "========================================" << endl;
			cout << "threshold : " << threshold << " d0norm :" << d0Norm << endl;
			cout << "abssum :" << abssum << " < d0norm - threshold * d0norm : " << (d0Norm - d0Norm * threshold) << endl;
			cout << "now: " << i << "/" << row << endl;
			cout << "========================================" << endl;
			break;
		}
		else {
			//cout << "[Compute ] exp:" << sexp << " need to be computed" << endl;
			need_down++;
			cout << "========================================" << endl;
			cout << "threshold : " << threshold << " d0norm :" << d0Norm << endl;
			cout << "abssum :" << abssum << " >= d0norm - threshold * d0norm : " << (d0Norm - d0Norm * threshold) << endl;
			cout << "now: " <<  i << "/" << row << endl;
			computeDeeperExps(exp_right,ltime,rtime);
			cout << "========================================" << endl;
		}

		
	}

	cout << "[Result  ] need down deeper : " << need_down << endl;
}

void Trendiff::printTopkResult() {
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
