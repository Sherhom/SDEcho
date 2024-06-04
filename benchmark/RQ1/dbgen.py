from connect import Connection
from sequence import Sequence
from explainEngine import Pattern
import numpy as np
import itertools
import heapq
import json
import math
import os
from tqdm import tqdm
import random

# 生成包含噪声的序列
class DBgen:
    def __init__(self, sequence:Sequence, noise_file = './noise.json'):
        # 连接三个数据库
        self.conn_db = Connection('tpch_rq1')
        self.conn_noise = Connection('noise')
        self.conn_origin = Connection('origin')
        self.sequence = sequence
        self.noise_file = noise_file

    # 在数据库中加入噪声
    def add_noise(self, pattern_value):
        conditions = []
        # 获取pattern在noise数据库中对应的数据
        for key, value in pattern_value.items():
            conditions.append(f" {key} = '{value}' ")
            
        noise_items = []
        # 将数据加入原始数据库中
        for value in np.array(self.conn_noise.execute_sql('select * from temp_noise where {}'.format('and'.join(conditions))),str):
            try:
                values = '\',\''.join(value)
                self.conn_db.execute_sql(f"insert into {self.sequence.table} values ('{values}')")
                noise_items.append('\''+values+'\'')
            except Exception as e:
                # print(e)
                continue
        return noise_items
    
    def delete_noise(self, pattern_value):
        conditions = []
        # 获取pattern在noise数据库中对应的数据
        for key, value in pattern_value.items():
            conditions.append(f" {key} = '{value}' ")

        # 将数据从数据库中删除
        for value in np.array(self.conn_noise.execute_sql('select * from temp_noise where {}'.format('and'.join(conditions))),str):
            colum_conditions = []
            for i,field in enumerate(self.fields):
                colum_conditions.append(f" {field} = '{value[i]}' ") 
            try:
                self.conn_db.execute_sql("delete from {} where {}".format(self.sequence.table,'and'.join(colum_conditions)))
            except Exception as e:
                # print(e)
                continue
    
    # 计算单个pattern的contribution
    def get_contr(self, pattern_value):
        # 将数据加入原始数据库中
        # self.conn_db.execute_sql("start transaction;")
        self.conn_db.execute_sql("rollback;")
        items = self.add_noise(pattern_value)

        # 计算contribution
        contribution = 0.0
        # print(self.conn_origin.execute_sql(self.sequence.get_sql()))
        # print(self.conn_db.execute_sql(self.sequence.get_sql()))
        for x_value in np.array(self.conn_db.execute_sql(f"select {self.sequence.x_str} from {self.sequence.table} where {self.sequence.condition} group by {self.sequence.x_str}"),str):
            # 计算单点的距离
            ori_result = np.array(self.conn_origin.execute_sql(f'select {self.sequence.target} from (select * from {self.sequence.table} where {self.sequence.condition}) t where {self.sequence.get_x_condition(x_value)}'))[0]
            cur_result = np.array(self.conn_db.execute_sql(f'select {self.sequence.target} from (select * from {self.sequence.table} where {self.sequence.condition}) t where {self.sequence.get_x_condition(x_value)}'))[0]
            if not ori_result:
                ori_result = 0
            if not cur_result:
                cur_result = 0
            contribution += math.pow(cur_result-ori_result,2)
        # self.conn_db.execute_sql("rollback")  # 恢复数据
        self.delete_noise(pattern_value)
        return math.pow(contribution, 0.5),items  # l2 normalization
    
    # 在原数据库中加入noise pattern(k个，默认为1)
    def gen(self, k=3):
        # 找到可能影响结果的pattern
        self.conn_noise.execute_sql(f"drop view if exists temp_noise")
        self.conn_noise.execute_sql(f"create view temp_noise as select * from {self.sequence.table} where {self.sequence.condition}")
        cnt = np.array(self.conn_noise.execute_sql(f'select count(*) from temp_noise'),int)[0]
        self.fields = self.conn_noise.desc_table('temp_noise')
        candidate_fields = []
        # fields = np.array(self.conn_noise.execute_sql(f"desc temp_noise"))[:,0]
        # 将基数为1的维度筛除
        for field in self.sequence.dimension:
            field_cnt = np.array(self.conn_noise.execute_sql(f'select count(*) from (select distinct {field} from temp_noise) t'),int)[0]
            if not (field_cnt == 1):
                candidate_fields.append(field)
        print(candidate_fields)
        patterns = []
        for i in tqdm(range(1, 5),desc='iter'):
            patterns += itertools.combinations(candidate_fields,i)
            # for each in itertools.combinations(candidate_fields,i):
            #     print(each)
                # pattern中不应包含target variable和因变量x
                # if re.findall(r'\((.*?)\)',self.sequence.target)[0] not in each and self.sequence.x not in each:
                    # 不包含主键
                    # if not set(primary_key).issubset(set(each)) or not set(primary_key):
            #     patterns.append(each)
        
        ori_cnt = np.array(self.conn_db.execute_sql(f'select count(*) from {self.sequence.table} where {self.sequence.condition}'),int)[0]
        # 查询pattern可能对应的values
        threshold = int(ori_cnt*0.01)
        threshold2 = int(ori_cnt*0.02)
        pattern_values = []
        pbar = tqdm(patterns)
        for pattern in pbar:
            pbar.set_description("Pattern")
            # print(pattern)
            for value in np.array(self.conn_noise.execute_sql(f"select {','.join(pattern)} from {self.sequence.table} where {self.sequence.condition} group by {','.join(pattern)} having count(*) >= {threshold} and count(*) <= {threshold2}"),str):
                pattern_values.append(dict(zip(pattern,value)))

        # 对每个pattern计算contribution，选取贡献最大的前k个        
        heap = []
        pbar = tqdm(pattern_values[0:-1:int(len(pattern_values)/50)])
        for pattern_value in pbar:
            is_overleap = False
            pbar.set_description("Contribution")
            contr,noise_items = self.get_contr(pattern_value)
            # if len(noise_items) > 0.02:
            #     continue
            result = Pattern(pattern_value,contr,noise_items)
            if result.contribution == 0.0:
                continue
            # 不包含overlap的noise
            for pattern1 in heap:
                if set(pattern1.item)&set(result.item):
                    is_overleap = True
                    if result.contribution>pattern1.contribution:
                        heap.remove(pattern1)
                        heapq.heappush(heap, result)
            if is_overleap:
                continue
            # if len(heap) == k:
            #     break
            heapq.heappush(heap, result)
            if len(heap) > k:
                heapq.heappop(heap)
        for pattern_value in heap:
            print(pattern_value)
        
        # 随机选取k个
        # heap = []
        # rs = random.sample(pattern_values,k)
        # for pattern_value in rs:
        #     result = Pattern(self.get_contr(pattern_value),pattern_value)
        #     heap.append(result)
        
        # 保存noise pattern和contribution
        data = []
        for noise in heap:
            ratio = float(len(noise.item)/ori_cnt)
            data.append(dict({'pattern':noise.pattern_value, 'contribution':noise.contribution, 'item':noise.item, 'ratio':ratio}))
            with open(self.noise_file, 'w+') as f:
                json.dump(data, f, indent=2)

        self.conn_noise.execute_sql("drop view temp_noise")
        self.conn_db.execute_sql("commit")
        

if __name__=='__main__':
    scale = 0.1
    sql = 14
    sql_path = f'./scale_{scale}/sql_{sql}'

    conn_db = Connection('tpch_rq1')
    conn_ori = Connection('origin')
    conn_noise = Connection('noise')
    with open(os.path.join(sql_path,'join.sql'),'r',encoding='utf-16') as f:
        join_sql = f.read()
    conn_db.execute_sql(join_sql)
    conn_db.execute_sql('commit')
    conn_ori.execute_sql(join_sql)
    conn_ori.execute_sql('commit')
    conn_noise.execute_sql(join_sql)
    conn_noise.execute_sql('commit')

    dbgen = DBgen(Sequence(os.path.join(sql_path,'sql.json')),os.path.join(sql_path,'noise.json'))
    noises = dbgen.gen(20)
