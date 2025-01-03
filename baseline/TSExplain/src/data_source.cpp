#include "tsexplain.h"
#include <set>
#include <math.h>

string str_join(const vector<string>& strs, const string& delim)
{
    string ret;

    bool first = true;
    for (auto &s : strs)
    {
        if (!first)
            ret += delim;
        ret += s;
        first = false;
    }
    return ret;
}

//用于给所有属性值编号
static int n_symbols;

//将字符串转为对应的整数
static int _idx(const string& str)
{
    if (strings.count(str) == 0) {
        strings[str] = n_symbols++;
        string_list.push_back(str);
    }
    return strings[str];
}

int total_pred_nums = 0;

int now_col_combines = 0;
int all_col_combines = 0;

DataSource::DataSource(const Spec& spec)
{
    auto start = CurrentTimeMillis();

    // clear the string-to-index mapping, create index for empty string ""
    strings.clear();
    n_symbols = 0;
    _idx("");

    // build sets of feature names, and time column names (including rollup time columns)
    unordered_set<string> dim_set(spec.features.begin(), spec.features.end()); //所有备选属性名的集合
    unordered_set<string> time_set(spec.time_cols.begin(), spec.time_cols.end()); //所有时间列名的集合

    // clear contents
    content_dim.clear();
    content_time.clear();
    content_value.clear();
    all_time.clear();
    timeline.clear();
    content_tableid.clear();

    // initialize content mapping (column name -> empty column)
    for (const auto& s : dim_set) {
        content_dim[_idx(s)] = vector<int>();
    }
    for (const auto& s : time_set) {
        content_time[_idx(s)] = vector<int>();
    }
    content_value = vector<double>();
    content_tableid = vector<int>();


    ifstream fin(spec.tsv_file);
    cout << "data path :" << spec.tsv_file << endl;

    string line, token;

    // get header
    getline(fin, line);
    vector<string> header; //所有属性名
    stringstream ss(line);
    while (getline(ss, token, '\t')) {
        header.push_back(token);
    }

    // read file
    n_rows = 0;
    while (getline(fin, line)) {
        int i = 0;
        stringstream ss(line);
        while (getline(ss, token, '\t')) {
            if (dim_set.count(header[i]) > 0) content_dim[_idx(header[i])].push_back(_idx(token));
            if (time_set.count(header[i]) > 0) content_time[_idx(header[i])].push_back(stoi(token));
            if (spec.val_col == header[i]) content_value.push_back(stod(token));
            if (spec.tableidname == header[i]) content_tableid.push_back(stoi(token));
            i += 1;
        }
        n_rows++;
    }
    fin.close();

    cout << "read rows : " << n_rows << endl;  
    auto read = CurrentTimeMillis();
    readfile_time = double(read - start) / 1000;

    
    start = CurrentTimeMillis();

    all_col_combines = pow( 2,spec.features.size() );
    features_t cur_dim{}; //vector<string>类型
    enumerate_combination(cur_dim, 0, spec);

    //cout << "there are totally " << total_pred_nums << " exps" << endl;
    for (const auto& time : all_time) {
        if (time >= spec.time_min && time <= spec.time_max) {
            timeline.push_back(time);
        }
    }

    sort(timeline.begin(), timeline.end());
    auto cube = CurrentTimeMillis();
    compute_cube_time += double(cube - start) / 1000;

}


//计算所有属性组合下的，所有可能的谓词，在各个时间点上的取值。
void DataSource::enumerate_combination(features_t& cur_dim, int next, const Spec& spec)
{
    if( now_col_combines++ % 20 == 0 ) 
    	cout << now_col_combines << " / " << all_col_combines << endl;
    trendlines[cur_dim] = __fetch_data(cur_dim, spec);
    //for (const auto& d : cur_dim) cout << "[" << d << "] ";
    total_pred_nums += trendlines[cur_dim].size() ;
    //cout << "<" << "> " << "possible predicate num : " << trendlines[cur_dim].size() <<endl;

    if (next < spec.features.size()) {
        for (int i = next; i < spec.features.size(); ++i) {
            cur_dim.push_back(spec.features[i]);
            enumerate_combination(cur_dim, i + 1, spec);
            cur_dim.pop_back();
        }
    }
}

//计算该属性组合下，所有可能的谓词，在各个时间点上的取值
pred_trend_t DataSource::__fetch_data(const vector<string>& features, const Spec& spec)
{
    //cout << ".";
    pred_trend_t result;
    vector<int> _features, _time_cols;

    for (const string& s : features) {
        _features.push_back(_idx(s));
    }
    for (const string& s : spec.time_cols) _time_cols.push_back(_idx(s));

    for (int r = 0; r < n_rows; ++r)
    {
        predicate_t predicate;
        for (int i = 0; i < _features.size(); ++i) {
            predicate.push_back(make_pair(_features[i], content_dim[_features[i]][r]));
        }
        times_t time, rollup_time;
        for (int i = 0; i < _time_cols.size(); ++i) {
            time.push_back(content_time[_time_cols[i]][r]);
        }

        all_time.insert(time);
        rollup_map[time] = rollup_time;
        vector<int> table1_count = {-1};
        vector<int> table2_count = {-2};

        double val = content_value[r];

        if (result.count(predicate) == 0) {
            result[predicate] = trendline_t();
        }
        if (result[predicate].count(time) == 0) {
            result[predicate][time] = 0;
        }
        if( result[predicate].count(table1_count) == 0 ) result[predicate][table1_count] = 0;
        if( result[predicate].count(table2_count) == 0 ) result[predicate][table2_count] = 0;

        if( content_tableid[r] == 1 ){
            result[predicate][time] += val;
            result[predicate][table1_count] ++;
        }else{
            result[predicate][time] -= val;
            result[predicate][table2_count] ++;
        }
    
    }
    return result;
}

//输入属性名组合，输出该组合下，所有谓词，在各个时间点上的聚合值
pred_trend_t DataSource::fetch_data(const vector<string>& features)
{
    vector<string> f(features);
    sort(f.begin(), f.end());
    return trendlines[f];
}

void DataSource::computeTopkExps(int K){
    int exps_n = 0;
    int min_time = 0;
    int max_time = 0;
    for( auto& times : all_time){
        if( times[0] > max_time) max_time = times[0];
    }
    cout << "max time : " << max_time << endl;

    unordered_map<int,double> d0_vector;

    for(auto& feats : trendlines ){
        if( feats.first.size() == 0 ){
            for( auto& trendline : feats.second ){
                const predicate_t& predi = trendline.first;
                const trendline_t& trendl = trendline.second;
                for( auto& time_p : trendl ){
                    vector<int> timeid = time_p.first;
                    double timeval = time_p.second;
                    d0_vector[ timeid[0] ] = timeval;
                }
            }
        }
    }

    double d0norm = 0.0;
    for( int i = 0 ; i<= max_time ;i++ ){
        //cout <<i << " : "  << d0_vector[i] << endl;
        d0norm += ( d0_vector[i] * d0_vector[i] );
    }
    d0norm = sqrt(d0norm);

    cout << "d0norm " << d0norm << endl;
    double table1_count = d0_vector[-1];
    double table2_count = d0_vector[-2];

    set<double> topkscores;

    for(auto& feats : trendlines ){
        //cout << endl;
        for( auto& trendline : feats.second ){
            exps_n++;
            const predicate_t& predi = trendline.first;
            const trendline_t& trendl = trendline.second;
            unordered_map<int,double> d_vector;

            for( auto& time_p : trendl ){    
                vector<int> timeid = time_p.first;
                double timeval = time_p.second;
                d_vector[ timeid[0] ] = timeval;
                //cout << timeid[0] << ":" << timeval <<'\t';
            }
            
            double score = 0.0;
            for( int i = 0 ;i <=max_time ;i++ ){
                score += ( d0_vector[i] - d_vector[i] ) * ( d0_vector[i] - d_vector[i] );
            }
            score = sqrt(score);
            score = score / d0norm;
            score  = score * (1 + d_vector[-1] / table1_count + d_vector[-2] / table2_count );

            topkscores.insert(score);
        }
    }
    cout << "there are totally :" << exps_n << " exps " << endl;
    int count = 0;
    for( auto s : topkscores){
        cout <<"top " << count << " score : " <<  s << endl;
        if(count++ == K ) break;
    }
}


DataSource::~DataSource() {}
