#pragma once

#include <vector>
#include <math.h>
#include <iostream>

using namespace std;

class SubVec {
private:
	vector<double> vec;

public:
	
	SubVec() :vec(0) {};
	SubVec(int size) :vec(size, 0) {};

	double& operator[] (int index) {
		return vec[index];
	}

	//the norm of the vector
	double norm() {
		double sum = 0;
		for (auto i : vec) {
			sum += i * i;
		}
		return sqrt(sum);
	}

	//the inner product of v1 and v2,v1*v2
	double innerProduct(SubVec& v2) {
		double res = 0;
		for (int i = 0; i < vec.size(); i++) {
			res += vec[i] * v2[i];
		}
		return res;
	}

	//show the vector
	void show() {
		cout << "[\t";
		for (auto i : vec) {
			cout << i << "\t";
		}
		cout << "]" << endl;
	}

	SubVec operator-(SubVec& subVec2) const {
		SubVec res(vec.size());
		for (int i = 0; i < vec.size(); i++) {
			res[i] = vec[i] - subVec2[i];
		}
		return res;
	}
};