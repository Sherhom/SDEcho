from metrics import *
import itertools
import heapq
import json
from tqdm import tqdm
import os
import random

class Pattern:
    def __init__(self, pattern_value, contribution, item):
        self.contribution = contribution
        self.pattern_value = pattern_value
        self.item = item
    
    def __lt__(self, other):
        return self.contribution < other.contribution
    
    def __str__(self):
        return f"{self.pattern_value}: {self.contribution}"

class ExplainEngine:
    def __init__(self, sequence:Sequence, metrics_name='ScoreFunction'):
        self.metrics_name = metrics_name
        self.conn = Connection('tpch_rq1')
        self.sequence = sequence
        self.metrics = Metrics(sequence)
        self.dir = 1  # 大小比较方向
        if metrics_name == 'Intervention':
            self.metrics = Intervention(sequence)
            self.dir = -1
        if metrics_name == 'Support':
            self.metrics = Support(sequence)
        if metrics_name == 'RiskRatio':
            self.metrics = RiskRatio(sequence)
        if metrics_name == 'Influence':
            self.metrics = Influence(sequence)
            self.dir = 1
        if metrics_name == 'Aggravation':
            self.metrics = Aggravation(sequence)
        if metrics_name == 'Distance':
            self.metrics = Distance(sequence)
            self.dir = -1
        if metrics_name == 'ScoreFunction':
            self.metrics = ScoreFunction(sequence)
            self.dir = -1
        if metrics_name == 'Diagnosis':
            self.metrics = Diagnosis(sequence)
        if metrics_name == 'AbsoluteChange':
            self.metrics = AbsoluteChange(sequence)
        self.dirdict = {}
        for metrics in ['Intervention','Distance','ScoreFunction']:
            self.dirdict[metrics] = -1
        for metrics in ['Support','RiskRatio','Influence','Aggravation','Diagnosis','AbsoluteChange']:
            self.dirdict[metrics] = 1
    
    # 获取指定的单张表中可能的谓词组合
    def get_patterns(self):
        table_name = self.sequence.table
        # 获取表中变量名
        self.fields = self.conn.desc_table(table_name)
        print(self.fields)
        # 去除基数为1的维度
        candidate_fields = []
        for field in self.sequence.dimension:
            field_cnt = np.array(self.conn.execute_sql(f'select count(*) from (select distinct {field} from (select * from {self.sequence.table} where {self.sequence.condition}) t1) t'),int)[0]
            if not (field_cnt == 1):
                candidate_fields.append(field)
        # 对变量值进行排列组合获得谓词
        pattern_list = []
        for i in range(1,min(5,len(candidate_fields)+1)):
           for each in itertools.combinations(candidate_fields,i):
                pattern_list.append(each)

        ori_result = np.array(conn_db.execute_sql(f'select {self.sequence.target} from {self.sequence.table} where {self.sequence.condition}'),float)[0][0]
        # 获取谓词的所有可能值
        pattern_values = []
        for pattern in pattern_list:
            print(pattern)
            for value in np.array(self.conn.execute_sql(f"select {','.join(pattern)} from {table_name} where {self.sequence.condition} group by {','.join(pattern)} having {self.sequence.target}>{ori_result*0.005}"),str):
                pattern_values.append(dict(zip(pattern,value)))
        print(f"谓词组合个数: {len(pattern_values)}")
        return pattern_values
    
    # 获取得分topk的解释
    def get_topk_explanations(self, k, pattern_values:dict):
        heap = []  # 用最小堆存储topk谓词和得分
        if len(pattern_values) == 0:
            pattern_values = self.get_patterns()
        pbar = tqdm(pattern_values)
        for value in pbar:
            pbar.set_description(self.metrics_name)
            score = self.metrics.get_score(value)
            result = Pattern(value, score*self.dir, self.metrics.get_items(value))
            # print(result)
            heapq.heappush(heap, result)
            if len(heap) > k:
                heapq.heappop(heap)
        return heap
    
    def get_all_topk_explanations(self, k, pattern_values: dict):
        heaps = {}
        metrics_list = ['Intervention','RiskRatio','Influence','Aggravation','Distance','ScoreFunction','Diagnosis','AbsoluteChange']
        for metrics in metrics_list:
            heaps[metrics] = []
        if len(pattern_values) == 0:
            pattern_values = self.get_patterns()
        pbar = tqdm(pattern_values)
        for value in pbar:
            pbar.set_description("ALL")
        #for i,value in enumerate(pattern_values):
            # print(f"{i}/{len(pattern_values)}")
            score = self.metrics.get_all_score_single_point(value)
            score['ScoreFunction'] = self.metrics.get_all_score(value)['ScoreFunction']
            for metrics, metrics_score in score.items():
                result = Pattern(value, metrics_score*self.dirdict[metrics], self.metrics.get_items(value))
                heapq.heappush(heaps[metrics], result)
                if len(heaps[metrics]) > k:
                    heapq.heappop(heaps[metrics])
        return heaps

class Test:
    def __init__(self, sequence:Sequence, noise_file:str, store_file="./result.json"):
        self.noise_file = noise_file
        self.sequence = sequence
        with open(noise_file,'r') as f:
            self.noises = json.load(f)
        self.store_file = store_file
        self.conn = Connection('tpch_rq1')
        try:
            self.add_noise()
            print("add noise")
        except Exception as e:
            print(e)
            print("add noise failed")
        self.pattern_values = ExplainEngine(sequence,'').get_patterns()

    def jaccard(self, explains:list):
        explains.sort(reverse=True)
        score = 0.0
        score_ = 0.0
        for i,explain in enumerate(explains):
            for noise in self.noises:
                contribution = noise['contribution']
                noise_items = noise['item']
                explain_items = explain.item
                intersection = list(set(noise_items) & set(explain_items))
                score += (contribution*(len(intersection)/(len(explain_items)+(len(noise_items)-len(intersection))))/math.log(i+2))
                score_ += (contribution*(len(intersection)/(len(explain_items)+(len(noise_items)-len(intersection))))/math.log(i+2))/len(noise_items)
        return score,score_
    
    def test(self, metrics_list, k, explain_dir = '', store_file = ''):
        score_dict = {}
        if store_file != '':
            self.store_file = store_file
        for metrics in metrics_list:

            # 从文件中读取解释结果计算得分
            if explain_dir != '':
                with open(os.path.join(explain_dir,f"{metrics}.json"),'r') as f:
                    data = json.load(f)
                    explains = []
                    for i,explain in enumerate(data):
                        if i >= k:
                            break
                        explains.append(Pattern(explain["pattern"],explain["score"],explain["item"]))
                score,score_ = self.jaccard(explains)
                score_dict[metrics] = [score,score_]
                continue
 
            explains = ExplainEngine(self.sequence,metrics).get_topk_explanations(k, self.pattern_values)
            explains = sorted(explains,reverse=True)
            data = []
            for explain in explains:
                print(explain)
                data.append(dict({'pattern':explain.pattern_value, 'score':explain.contribution, 'item':explain.item}))
            with open(os.path.join(os.path.dirname(self.store_file),f"{metrics}.json"), 'w') as f:
                json.dump(data, f, indent=2)
            score,score_ = self.jaccard(explains)
            score_dict[metrics] = [score,score_]
        
        with open(self.store_file,'w') as f:
            json.dump(score_dict, f, indent=2)

    def test_all(self, k):
        metrics_explains = ExplainEngine(self.sequence).get_all_topk_explanations(k, self.pattern_values)
        score_dict = {}
        for metrics,explains in metrics_explains.items():
            explains = sorted(explains,reverse=True)
            data = []
            for explain in explains:
                print(explain)
                data.append(dict({'pattern':explain.pattern_value, 'score':explain.contribution, 'item':explain.item}))
            with open(os.path.join(os.path.dirname(self.store_file),f"{metrics}.json"), 'w') as f:
                json.dump(data, f, indent=2)
            score,score_ = self.jaccard(explains)
            score_dict[metrics] = [score,score_]
    
        with open(self.store_file,'w') as f:
            json.dump(score_dict, f, indent=2)

    def add_noise(self):
        with open(self.noise_file,'r') as f:
            noises = json.load(f)
            for noise in noises:
                for item in noise['item']:
                    self.conn.execute_sql(f'insert into {self.sequence.table} values({item});')
        self.conn.execute_sql("commit")



if __name__=='__main__':
    scale = 0.001
    sql = 6

    conn_db = Connection('tpch_rq1')
    conn_ori = Connection('origin')
    sql_path = f'./scale_{scale}/sql_{sql}'
    with open(os.path.join(sql_path,"noise.json"),'r') as f:
        noise_patterns = json.load(f)
    with open(os.path.join(sql_path,'join.sql'),'r',encoding='utf-16') as f:
        join_sql = f.read()
    conn_db.execute_sql(join_sql)
    conn_db.execute_sql('commit')
    conn_ori.execute_sql(join_sql)
    conn_ori.execute_sql('commit')

    for n in range(3,8):
        # n_noise_patterns = random.sample(noise_patterns,n)
        if not os.path.exists(os.path.join(sql_path,f'noise_{n}')):
            os.makedirs(os.path.join(sql_path,f'noise_{n}'))
            with open(os.path.join(sql_path,f'noise_{n}/noise.json'),'w') as f:
                json.dump(random.sample(noise_patterns,n), f, indent=2)
        test = Test(Sequence(os.path.join(sql_path,'sql.json')),os.path.join(sql_path,f'noise_{n}/noise.json'),os.path.join(sql_path,f'noise_{n}/k_7.json'))
        test.test_all(7)
        for k in range(3,7):
            test.test(['Intervention','RiskRatio','Influence','Aggravation','Distance','ScoreFunction','Diagnosis','AbsoluteChange'],k,os.path.join(sql_path,f'noise_{n}'),os.path.join(sql_path,f'noise_{n}/k_{k}.json'))
        conn_db.execute_sql(join_sql)
        conn_db.execute_sql('commit')

