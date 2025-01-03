#pragma once

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <string>
#include <vector>
#include <ctime>
#include <utility>
#include <chrono>
#include <map>

using namespace std;

/*
 * types
 */

typedef vector<int> times_t;
typedef vector<string> features_t;  //属性名组合
typedef pair<int, int> pred_t;
typedef vector<pred_t> predicate_t; //谓词组合，即解释
typedef pair<double, char> score_t;
typedef pair<int, int> time_range_t;

/*
 * Hash
 */

const size_t _P = 100000007;
const size_t _Q = 100000037;
const size_t _R = 100000039;

struct pair_ii_hash {
    size_t operator()(const pair<int, int> &p) const {
        return p.first * _P + p.second;
    }
};

struct predicate_hash {
    size_t operator()(const predicate_t &pred) const {
        size_t h = 0;
        for (const auto& p : pred) {
            h = (h * _Q) + p.first * _P + p.second;
        }
        return h;
    }
};

struct predicate_int_hash {
    size_t operator()(const pair<predicate_t, int> &pred) const {
        return predicate_hash()(pred.first) * _R + pred.second;
    }
};

struct features_hash {
    size_t operator()(const features_t &feats) const {
        size_t h = 0;
        auto _hash = hash<string>();
        for (const auto& f : feats) {
            h = (h * _P) + _hash(f);
        }
        return h;
    }
};

struct times_hash {
    size_t operator()(const times_t &feats) const {
        size_t h = 0;
        for (const auto& f : feats) {
            h = (h * _P) + f;
        }
        return h;
    }
};

typedef unordered_map<times_t, double, times_hash> trendline_t; //某个谓词在各个时间点上的取值
typedef unordered_set<times_t, times_hash> timeset_t;
typedef unordered_set<predicate_t, predicate_hash> predset_t;
typedef unordered_map<predicate_t, trendline_t, predicate_hash> pred_trend_t; //记录所有谓词（key），在所有时间点上的聚合值
typedef vector<trendline_t> trendlines_t; //多个时间序列的数组
typedef vector<vector<score_t> > range_scores_t;
typedef vector<vector<int> > explanations_t;
typedef vector<double> range_val_t;

typedef trendline_t (*trendline_processor)(trendline_t& init_trendline, vector<times_t>& timeline);
typedef pair<double, char> (*metric_t)(
        vector<double>& trendfull, vector<double>& trend, int first, int last);


/*
 * specification
 */

struct Spec {
    string tsv_file;  //读入数据的文件
    vector<features_t> feature_hier; //备选属性层次 features_t为vector<string>
    features_t features; //备选属性名 features_t为vector<string>
    features_t time_cols; //时间属性名 features_t为vector<string>

    double try_seg_len_ratio;   //分段长度的比率
    double try_total_len_ratio; //总长度的比率
    int try_seg_number; //尝试总共的段数
    int try_seg_len;    //尝试每段的长度

    string val_col;     //被聚合列属性名
    string tableidname; //区分来自不同的表名
    double supp_ratio;  //支持度要求

    times_t time_min;   //时间列的最小取值 times_t 类型为vector<int>
    times_t time_max;   //时间列的最大取值 times_t 类型为vector<int>

    trendline_processor post_process; //之前用的函数指针
    int mavg_window;   //窗口大小
    times_t explain_time_start; //要求解释的左端点
    times_t explain_time_end;   //要求解释的右端点

    metric_t metric;  //度量函数,函数指针
    int topK;       //最好的解释的个数
    int pred_len; //解释阶数的限制 [int of max predicate length, -1 means no restriction]
    string sim;   //相似度函数

    int seg_number; //最终分段的数量[number of segmentation in the result]
    int min_len; //每段中的最小长度 min_len: 2: the min length has two points, 1: the min length can only have 1 point
    int min_dist; // 两个连续段的最小距离 min_dist: 0: two segments can have overlapping ending points; 1: two segments can not have overlapping ending points

    int cascading_opt;  //CA选项

    vector<int> starts; //每一个分段的开头
};


/*
 * global variables
 */

// map string to integer to speed up
extern unordered_map<string, int> strings;  //将属性值转换为整数，便于计算
extern vector<string> string_list; //记录所有出现的字符串 features_t为vector<string>

extern vector<predicate_t> predicates; //所有可能的谓词

extern unordered_map<predicate_t, int, predicate_hash> pred_idx; //所有谓词及其对应的编号 

extern unordered_map<pred_t, unordered_set<pred_t, pair_ii_hash>, pair_ii_hash> children_set; //记录谓词，及其所有可能的后代

// phase indicator
extern bool phase1;

// trendlines
extern vector<times_t> timeline; //满足min/max要求的所有时间点

extern vector<bool> valid_time;
extern vector<int> valid_time_idx;

extern trendlines_t trends;  //所有谓词的时间列

extern trendlines_t trends_comp;  //所有在原数据中剔除谓词得到的时间列

extern trendline_t trend_full; //原数据（不做删除），在各个时间点上的聚合值。

extern int T;  //所有时间点的个数
static inline int time_range_idx(const pair<int, int>& range) { return range.first * T + range.second; }

// compute metrics
extern range_scores_t pred_scores;

// cascading result
extern explanations_t explanation;
extern vector<bool> explanation_computed;

// sim score
extern range_val_t seg_score;
extern vector<bool> seg_score_computed;

// dp results
extern double dp_score;
extern vector<vector<int>> all_starts;

// measurements
extern double readfile_time;
extern double compute_cube_time;
extern double compute_trendline_time;
extern double compute_metric_time;
extern double cascading_time;
extern double compute_sim_time;
extern double compute_segment_time;

/*
 * Data Source Class
 */

class DataSource
{
private:
    // map string to integer to accelarate
    // dim_index -> dim values of all rows
    unordered_map<int, vector<int>> content_dim;  //该属性下，所有行的取值
    // time_index -> time values of all rows
    unordered_map<int, vector<int>> content_time; //所有行，时间列的取值
    // value_index -> time values of all rows   聚合值
    vector<double> content_value; //所有行被聚合列的取值
    vector<int> content_tableid; //区分来自不同的表

    unordered_map<features_t, pred_trend_t, features_hash> trendlines; // 属性名组合  -> 该组合下所有谓词对应的在所有时间点上的取值

    void enumerate_combination(features_t& cur_dim, int next, const Spec& spec);
    pred_trend_t __fetch_data( const vector<string>& features, const Spec& spec);
public:
    int n_rows;   //总共的行数
    timeset_t all_time;      //所有可能的时间点的集合，即所有可能的分组属性，times_t 类型为vector<int>
    vector<times_t> timeline;  //符合min /max 要求的时间点

    unordered_map<times_t, times_t, times_hash> rollup_map; //key为分组属性的取值

    pred_trend_t fetch_data(const vector<string>& features);
    DataSource(const Spec& spec);
    void clear() {
        content_dim.clear();
        content_time.clear();
        content_value.clear();
        trendlines.clear();
    }
    void computeTopkExps(int K);
    
    ~DataSource();
};

/*
 * Metrics
 */

score_t MChangeAbs(vector<double>& trend_full, vector<double>& trend, int first, int last);

/*
 * procedures
 */

Spec parse_spec_from_file(string spec_path,int& k , int& order_int);
extern int mavg_window;
trendline_t moving_avg_left_k_right_k(trendline_t& init_trendline, vector<times_t>& timeline);
void compute_trendline(Spec &spec, DataSource* source);
void compute_metric_scores(const Spec& spec);
vector<time_range_t> visual_segmentation(trendline_t &trend, vector<times_t>& timeline, int seg_number);
void cascading(int first, int last, const Spec& spec);
double R2_error(const vector<double>& x, const vector<double>& y);
double L1_error(const vector<double>& x, const vector<double>& y);
double monotonic(const vector<double>& x, const vector<double>& y);
void dp_optimal_fixedk(const Spec& spec);

/*
 * Utils
 */
static int64_t CurrentTimeMillis()
{
    int64_t timems = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timems;
}

template <std::ctype_base::mask mask>
class IsNot
{
    std::locale myLocale;       // To ensure lifetime of facet...
    std::ctype<char> const* myCType;
public:
    IsNot( std::locale const& l = std::locale() )
            : myLocale( l )
            , myCType( &std::use_facet<std::ctype<char> >( l ) )
    {
    }
    bool operator()( char ch ) const
    {
        return ! myCType->is( mask, ch );
    }
};

typedef IsNot<std::ctype_base::space> IsNotSpace;

static std::string trim( std::string const& original )
{
    std::string::const_iterator right = std::find_if( original.rbegin(), original.rend(), IsNotSpace() ).base();
    std::string::const_iterator left = std::find_if(original.begin(), right, IsNotSpace() );
    return std::string( left, right );
}

static string time_to_string(const times_t& t)
{
    ostringstream s;
    for (int i = 0; i + 1 < t.size(); ++i) s << t[i] << "-";
    s << t[t.size() - 1];
    return s.str();
}
