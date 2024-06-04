#include "table.h"

Table::Table(vector<string>& focusNames) :focusColNames(focusNames),
							focusCols(focusColNames.size(),vector<string>(0)){}

void Table::printOriginTable() {
	cout << "tableID" << "\t";
	cout << "time" << "\t";
	cout << "agg" << "\t";
	for (int j = 0; j < focusColNames.size(); j++) {
		cout << focusColNames[j] << "\t";
	}
	cout << endl;

	for (int i = 0; i < totalSize; i++) {
		cout << positive[i] << "\t";
		cout << timeCol[i] << "\t";
		cout << aggCol[i] << "\t";
		int col = focusColNames.size();
		for (int j = 0; j < col; j++) {
			cout << focusCols[j][i] << "\t";
		}
		cout << endl;
	}
}

void Table::encode() {
	set<int> times;
	for (int t : timeCol) {
		times.insert(t);
	}
	//timeEncodeMap[-1] = NULLVALUE;
	//timeDecodeMap[NULLVALUE] = -1;
	int encodeNum = 0;
	for (int t : times) {
		timeEncodeMap[t] = encodeNum;
		timeDecodeMap[encodeNum] = t;
		encodeNum++;
	}

	max_time = encodeNum - 1;


	for (int i = 0; i < totalSize; i++) {
		timeEncoded.push_back(timeEncodeMap[timeCol[i]]);
	}

	//cout << "[Encode  ] time col encoded, " << timeEncodeMap.size() << " values encoded" << endl;

	focusEncodeMaps.resize(focusCols.size(), unordered_map<string, int>());
	focusDecodeMaps.resize(focusCols.size(), unordered_map<int, string>());
	focusEncoded.resize(focusCols.size(), vector<int>());

	for (int j = 0; j < focusCols.size(); j++) {
		vector<string>& focusCol = focusCols[j];
		focusEncodeMaps[j]["null"] = NULLVALUE;
		focusDecodeMaps[j][NULLVALUE] = "null";
		int encodeNum = 0;
		for (int i = 0; i < totalSize; i++) {
			if (focusEncodeMaps[j].count(focusCol[i]) == 0) {
				focusEncodeMaps[j][focusCol[i]] = encodeNum;
				focusDecodeMaps[j][encodeNum] = focusCol[i];
				encodeNum++;
			}
			focusEncoded[j].push_back(focusEncodeMaps[j][focusCol[i]]);
		}
		//cout << "[Encode  ] " << focusColNames[j] << " col encoded, " << focusEncodeMaps[j].size() << " values encoded" << endl;
	}

}

void Table::printEncodeMap(string path) {
	ofstream out;
	out.open(path, ios::out);
	if (!out) {
		cout << "[Error   ]:cannot open file: " << path << endl;
		return;
	}

	out << "Time encode map: " << endl;
	for (auto& p : timeEncodeMap) {
		out << p.first << "\t" << p.second << endl;
	}
	out << endl;

	for (int j = 0; j < focusColNames.size(); j++) {
		out << focusColNames[j] << " encode map: " << endl;
		for (auto& p : focusEncodeMaps[j]) {
			out << p.first << "\t" << p.second << endl;
		}
		out << endl;
	}
}

void Table::printTimeMap() {
	cout << "Time encode map: " << endl;
	cout << "before" << "\t" << "after" << endl;
	for (auto& p : timeEncodeMap) {
		cout << p.first << "\t" << p.second << endl;
	}
	cout << endl;
}

void Table::outputEncodedTable(string path) {

	ofstream out;
	out.open(path, ios::out);
	if (!out) {
		cout << "[Error   ]:cannot open file: " << path << endl;
		return;
	}

	out << "tableID" << "\t";
	out << "time" << "\t";
	out << "agg" << "\t";
	for (int j = 0; j < focusColNames.size(); j++) {
		out << focusColNames[j] << "\t";
	}
	out << endl;

	for (int i = 0; i < totalSize; i++) {
		if (positive[i]) {
			out << 1 << "\t";
		}
		else {
			out << 2 << "\t";
		}
		
		out << timeEncoded[i] << "\t";
		out << aggCol[i] << "\t";
		int col = focusColNames.size();
		for (int j = 0; j < col; j++) {
			out << focusEncoded[j][i] << "\t";
		}
		out << endl;
	}
	out.close();
}

bool Table::isRecordOfExp(int index, const vector<int>& exp) {
	int col = exp.size();
	for (int j = 0; j < col; j++) {
		if (exp[j] == NULLVALUE) {
			continue;
		}
		else {
			if (exp[j] != focusEncoded[j][index]) {
				return false;
			}
		}
	}
	return true;
}

double Table::computeAcutalScore(const vector<int>& explanation, SubVec& d0,int ltime ,int rtime,double& abssum) {
	SubVec d(timeEncodeMap.size());
	int t1Rmove = 0, t2Romve = 0;
	for (int index = 0; index < totalSize; index++) {
		if (timeEncoded[index] < ltime || timeEncoded[index] > rtime) {
			continue;
		}
		if (isRecordOfExp(index, explanation)) {
			if (positive[index]) {
				t1Rmove++;
				d[timeEncoded[index]] = d[timeEncoded[index]] + aggCol[index];
			}
			else {
				t2Romve++;
				d[timeEncoded[index]] = d[timeEncoded[index]] - aggCol[index];
			}
			abssum += abs(aggCol[index]);
		}
	}
	double dist_score = ((d0 - d).norm()) / d0.norm() ;
	double penalty_score = 1.0 + (1.0 * t1Rmove / table1Size) + (1.0 * t2Romve / table2Size);
	return dist_score * penalty_score;
}