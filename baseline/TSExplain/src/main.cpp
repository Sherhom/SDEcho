#include "tsexplain.h"

using namespace std;

int main(int argc, char* argv[])
{   
    if( argc <= 1 ){
        cout << "param num error" << endl;
        return 0;
    }

    string spec_path = string(argv[1]);
    //string output_prefix = string(argv[2]);
    //string spec_path = "demo.spec";
    int k ;
    int order_int ;
    
    string output_prefix = "_";
    cout << "config file: " << spec_path << endl;
   

    time_t start = CurrentTimeMillis();
    Spec spec = parse_spec_from_file(spec_path,k,order_int);

    cout << "k :" << k << endl;
    cout << "order : " << order_int << endl;

    if( spec.features.size() < order_int ){
	cout << "order error: too big " << endl;
	cout << "=========================" << endl;
	return 0;
    }    

    while( spec.features.size() > order_int ){
        spec.features.pop_back();
    }

    // initialize data source
    DataSource *source;


    source = new DataSource(spec);

    source->computeTopkExps(k);    

    // load data from database and compute trendline
    //compute_trendline(spec, source);
    source->clear();
    
    time_t endtime = CurrentTimeMillis();

    cout << "timecost : " << (endtime - start) << " ms " << endl;
    cout << "======================" << endl;

    return 0;
}

/*
 * Global data
 */

// map string to integer to speed up
unordered_map<string, int> strings;
vector<string> string_list;

vector<predicate_t> predicates;
unordered_map<predicate_t, int, predicate_hash> pred_idx;
unordered_map<pred_t, unordered_set<pred_t, pair_ii_hash>, pair_ii_hash> children_set;

int T;
vector<times_t> timeline;
vector<bool> valid_time;
vector<int> valid_time_idx;
trendlines_t trends;
trendlines_t trends_comp;
trendline_t trend_full;
range_scores_t pred_scores;
explanations_t explanation;
range_val_t seg_score;
double dp_score;
vector<vector<int>> all_starts;

vector<bool> explanation_computed;
vector<bool> seg_score_computed;

double readfile_time;
double compute_cube_time;
double compute_trendline_time;
double compute_metric_time;
double cascading_time;
double compute_sim_time;
double compute_segment_time;

bool phase1;
