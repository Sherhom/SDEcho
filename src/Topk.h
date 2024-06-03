#pragma once
#include <vector>
#include <queue>

using namespace std;

class Explantion {
public:
	vector<int> encodedValues;
	double score;
	
	Explantion(vector<int>& exp, double score0) {
		for (int i : exp) {
			encodedValues.push_back(i);
		}
		score = score0;
	}

	bool operator<(const Explantion& rhs) const {
		return score < rhs.score;
	}
};

class Topk {
private:
	//the size of heap now 
	int size;

	int k;
	priority_queue<Explantion> que;

public:
	Topk():Topk(0){}

	Topk(int _k) {
		this->k = _k;
		size = 0;
	}

	void setK(int _k) {
		this->k = _k;
	}

	//compare score to insert to heap or not
	void insert(vector<int>& exp, double score) {
		if (size < k) {
			que.push(Explantion(exp,score));
			size++;
			//cout << "[Update  ] threshold now is " << getHighestScore() << endl;
		}
		else {
			if (que.top().score > score) {
				que.pop();
				que.push(Explantion(exp, score));
				//cout << "[Update  ] threshold now is " << getHighestScore() << endl;
			}
		}
	}

	//get the threshold now
	double getHighestScore() {
		if (size < k) return 1e30;
		return que.top().score;
	}

	bool isNotEmpty() {
		return (size > 0);
	}

	void pop() {
		size--;
		que.pop();
	}

	const Explantion& top() {
		return que.top();
	}

};