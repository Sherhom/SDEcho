from connect import Connection
import numpy as np
import math
from sequence import Sequence

class Metrics:
    def __init__(self, sequence:Sequence):
        self.sequence = sequence
        self.conn_db = Connection('tpch_rq1')
        self.conn_ori = Connection('origin')
        # for table in self.conn_db.get_tables():
        #     self.conn_db.execute_sql(f"ALTER TABLE {table} DISABLE TRIGGER ALL")
        #     self.conn_ori.execute_sql(f"ALTER TABLE {table} DISABLE TRIGGER ALL")
        self.x_vaules = np.array(self.conn_db.execute_sql(f"select {self.sequence.x_str} from {self.sequence.table} where {self.sequence.condition} group by {self.sequence.x_str}"),str)

    # 删除pattern对应的行
    def delete_pattern(self, pattern:dict):
        conditions = []
        # 获取pattern对应的条件
        for key, value in pattern.items():
            conditions.append(f" {key} = '{value}' ")
        self.conn_db.execute_sql(f"delete from {self.sequence.table} where {'and'.join(conditions)}")  # 删除谓词
        self.conn_ori.execute_sql(f"delete from {self.sequence.table} where {'and'.join(conditions)}")
    
    def global_attr(self, x_value, pattern):
        conditions = []
        for key, value in pattern.items():
            conditions.append(f" {key} = '{value}' ")
        sql = 'select {} from (select * from {} where {}) t where {}'.format(self.sequence.target,self.sequence.table,self.sequence.condition,self.sequence.get_x_condition(x_value))
        global_db = np.array(self.conn_db.execute_sql(sql),float)[0]
        global_ori = np.array(self.conn_ori.execute_sql(sql),float)[0]

        if not global_db or math.isnan(global_db):
            global_db = 0
        if not global_ori or math.isnan(global_ori):
            global_ori = 0

        if len(pattern) == 0:
            return global_db,global_ori,0,0
        
        self.conn_db.execute_sql('start transaction')
        self.conn_ori.execute_sql('start transaction')
        self.delete_pattern(pattern)
        temp_db = np.array(self.conn_db.execute_sql(sql),float)[0]
        temp_ori = np.array(self.conn_ori.execute_sql(sql),float)[0]
        if not temp_db or math.isnan(temp_db):
            attr_db = global_db
        else:
            attr_db = global_db - temp_db
        if not temp_ori or math.isnan(temp_ori):
            attr_ori = global_ori
        else:
            attr_ori = global_ori - temp_ori
        self.conn_db.execute_sql("rollback")
        self.conn_ori.execute_sql("rollback")
        
        # print(f'{global_db},{global_ori},{attr_db},{attr_ori}')
        return global_db, global_ori, attr_db, attr_ori
    
    def global_attr_single_point(self, pattern):
        conditions = []
        for key, value in pattern.items():
            conditions.append(f" {key} = '{value}' ")
        sql = 'select {} from (select * from {} where {}) t'.format(self.sequence.target,self.sequence.table,self.sequence.condition)
        global_db = np.array(self.conn_db.execute_sql(sql),float)[0]
        global_ori = np.array(self.conn_ori.execute_sql(sql),float)[0]

        if not global_db or math.isnan(global_db):
            global_db = 0
        if not global_ori or math.isnan(global_ori):
            global_ori = 0

        if len(pattern) == 0:
            return global_db,global_ori,0,0
        
        self.conn_db.execute_sql('start transaction')
        self.conn_ori.execute_sql('start transaction')
        self.delete_pattern(pattern)
        temp_db = np.array(self.conn_db.execute_sql(sql),float)[0]
        temp_ori = np.array(self.conn_ori.execute_sql(sql),float)[0]
        if not temp_db or math.isnan(temp_db):
            attr_db = global_db
        else:
            attr_db = global_db - temp_db
        if not temp_ori or math.isnan(temp_ori):
            attr_ori = global_ori
        else:
            attr_ori = global_ori - temp_ori
        self.conn_db.execute_sql("rollback")
        self.conn_ori.execute_sql("rollback")
        
        # print(f'{global_db},{global_ori},{attr_db},{attr_ori}')
        return global_db, global_ori, attr_db, attr_ori
    
    def get_items(self, pattern):
        conditions = []
        for key, value in pattern.items():
            conditions.append(f" {key} = '{value}' ")
        sql = 'select * from (select * from {} where {}) t where {}'.format(self.sequence.table,self.sequence.condition,'and'.join(conditions))
        items = []
        for item in np.array(self.conn_db.execute_sql(sql)):
            item = [str(value) for value in item]
            items.append('\''+ '\',\''.join(item)+'\'')
        return items 

    def get_all_score_single_point(self,pattern):
        lamda = 0.5
        alpha = 0.1

        score = {}
        metrics_list = ['Intervention','RiskRatio','Influence','Aggravation','Distance','ScoreFunction','Diagnosis','AbsoluteChange']
        for metrics in metrics_list:
            score[metrics] = 0.0

        conditions = []
        for key, value in pattern.items():
            conditions.append(f" {key} = '{value}' ")


        global_db, global_ori, attr_db, attr_ori = self.global_attr_single_point(pattern)
            # print(f"{global_db},{global_ori},{attr_db},{attr_ori}")
        sql = 'select count(*) from (select * from {} where {}) as t where {}'.format(self.sequence.table,self.sequence.condition,'and'.join(conditions))
        cnt_db = np.array(self.conn_db.execute_sql(sql),int)[0]
        cnt_ori = np.array(self.conn_ori.execute_sql(sql),int)[0]

        score['Intervention'] += pow((global_db-attr_db)-(global_ori-attr_ori),2)
        if attr_db+attr_ori != 0 and global_db-attr_db != 0:
            score['RiskRatio'] += pow(attr_db*((global_db-attr_db)+(global_ori-attr_ori))/((attr_db+attr_ori)*(global_db-attr_db)),2)
        if cnt_db != 0 and cnt_ori != 0:
            score['Influence'] += pow((lamda*(global_db-attr_db)/(cnt_db)-(1-lamda)*(global_ori-attr_ori)/(cnt_ori)),2)
        score['Aggravation'] += pow((attr_db-attr_ori),2)
        score['Distance'] += pow((global_db-attr_db)-(global_ori-attr_ori),2)
        if attr_db != 0 and attr_ori != 0:
            score['Diagnosis'] += pow(math.log(1/alpha)+attr_db*math.log((attr_ori+attr_db)/attr_db)+attr_ori*math.log((attr_ori+attr_db)/attr_ori),2)
        score['AbsoluteChange'] += pow(math.fabs(global_db-global_ori)-math.fabs((global_db-attr_db)-(global_ori-attr_ori)),2)
            # print(score)

        for metrics in metrics_list:
            score[metrics] = float(pow(score[metrics],0.5))

        dis = Distance(self.sequence)
        distance = dis.get_score(dict({}))
        cnt_db = np.array(self.conn_db.execute_sql(f'select count(*) from {self.sequence.table} where {self.sequence.condition}'),int)[0]
        cnt_ori = np.array(self.conn_ori.execute_sql(f'select count(*) from {self.sequence.table} where {self.sequence.condition}'),int)[0]
        temp_distance = score['Distance']
        sql = 'select count(*) from (select * from {} where {}) as t where {}'.format(self.sequence.table,self.sequence.condition,'and'.join(conditions))
        temp_cnt_db = np.array(self.conn_db.execute_sql(sql),float)[0]
        temp_cnt_ori = np.array(self.conn_ori.execute_sql(sql),float)[0]
        # print(f'{distance},{cnt_db},{cnt_ori}')
        score['ScoreFunction'] = float((temp_distance/distance)*(1+temp_cnt_db/cnt_db+temp_cnt_ori/cnt_ori))
        #print(f"{temp_distance},{distance},{cnt_db},{cnt_ori},{temp_cnt_db},{temp_cnt_ori}")

        return score

    def get_all_score(self,pattern):
        lamda = 0.5
        alpha = 0.1

        score = {}
        metrics_list = ['Intervention','RiskRatio','Influence','Aggravation','Distance','ScoreFunction','Diagnosis','AbsoluteChange']
        for metrics in metrics_list:
            score[metrics] = 0.0

        conditions = []
        for key, value in pattern.items():
            conditions.append(f" {key} = '{value}' ")

        for x_value in self.x_vaules:
            # print(x_value)
            global_db, global_ori, attr_db, attr_ori = self.global_attr(x_value,pattern)
            # print(f"{global_db},{global_ori},{attr_db},{attr_ori}")
            sql = 'select count(*) from (select * from {} where {}) as t where {} and {}'.format(self.sequence.table,self.sequence.condition,'and'.join(conditions),self.sequence.get_x_condition(x_value))
            cnt_db = np.array(self.conn_db.execute_sql(sql),int)[0]
            cnt_ori = np.array(self.conn_ori.execute_sql(sql),int)[0]

            score['Intervention'] += pow((global_db-attr_db)-(global_ori-attr_ori),2)
            if attr_db+attr_ori != 0 and global_db-attr_db != 0:
                score['RiskRatio'] += pow(attr_db*((global_db-attr_db)+(global_ori-attr_ori))/((attr_db+attr_ori)*(global_db-attr_db)),2)
            if cnt_db != 0 and cnt_ori != 0:
                score['Influence'] += pow((lamda*(global_db-attr_db)/(cnt_db)-(1-lamda)*(global_ori-attr_ori)/(cnt_ori)),2)
            score['Aggravation'] += pow((attr_db-attr_ori),2)
            score['Distance'] += pow((global_db-attr_db)-(global_ori-attr_ori),2)
            if attr_db != 0 and attr_ori != 0:
                score['Diagnosis'] += pow(math.log(1/alpha)+attr_db*math.log((attr_ori+attr_db)/attr_db)+attr_ori*math.log((attr_ori+attr_db)/attr_ori),2)
            score['AbsoluteChange'] += pow(math.fabs(global_db-global_ori)-math.fabs((global_db-attr_db)-(global_ori-attr_ori)),2)
            # print(score)

        for metrics in metrics_list:
            score[metrics] = float(pow(score[metrics],0.5))

        dis = Distance(self.sequence)
        distance = dis.get_score(dict({}))
        cnt_db = np.array(self.conn_db.execute_sql(f'select count(*) from {self.sequence.table} where {self.sequence.condition}'),int)[0]
        cnt_ori = np.array(self.conn_ori.execute_sql(f'select count(*) from {self.sequence.table} where {self.sequence.condition}'),int)[0]
        temp_distance = score['Distance']
        sql = 'select count(*) from (select * from {} where {}) as t where {}'.format(self.sequence.table,self.sequence.condition,'and'.join(conditions))
        temp_cnt_db = np.array(self.conn_db.execute_sql(sql),float)[0]
        temp_cnt_ori = np.array(self.conn_ori.execute_sql(sql),float)[0]
        # print(f'{distance},{cnt_db},{cnt_ori}')
        score['ScoreFunction'] = float((temp_distance/distance)*(1+temp_cnt_db/cnt_db+temp_cnt_ori/cnt_ori))
        #print(f"{temp_distance},{distance},{cnt_db},{cnt_ori},{temp_cnt_db},{temp_cnt_ori}")

        return score

class Support(Metrics):
    def single_point_score(self, x_value, pattern:dict):
        sql = f'select count(*) from (select * from {self.sequence.table} where {self.sequence.condition}) t where {self.sequence.get_x_condition(x_value)}'
        total_cnt = np.array(self.conn_db.execute_sql(sql),float)[0]
        self.conn_db.execute_sql('start transaction')
        self.conn_ori.execute_sql('start transaction')
        self.delete_pattern(pattern)
        cnt = np.array(self.conn_db.execute_sql(sql),float)[0]
        self.conn_db.execute_sql("rollback")
        self.conn_ori.execute_sql("rollback")
        return 1-cnt/(total_cnt+1)
    
    def get_score(self, pattern):
        score = 0.0
        for x_value in self.x_vaules:
            score += self.single_point_score(x_value,pattern)
        return score
    
    
class RiskRatio(Metrics):
    def single_point_score(self, x_value:str, pattern:dict):
        global_db, global_ori, attr_db, attr_ori = self.global_attr(x_value,pattern)
        if attr_db+attr_ori == 0:
            return 0
        return attr_db*((global_db-attr_db)+(global_ori-attr_ori))/((attr_db+attr_ori)*(global_db-attr_db))

    def get_score(self, pattern):
        score = 0.0
        for x_value in self.x_vaules:
            score += math.pow(self.single_point_score(x_value,pattern),2)
        return math.pow(score,0.5)

class Influence(Metrics):
    def single_point_score(self, x_value, pattern:dict, lamda):
        conditions = []
        for key, value in pattern.items():
            conditions.append(f" {key} = '{value}' ")
        sql = 'select count(*) from {} where {}'.format(self.sequence.table,'and'.join(conditions))
        cnt_db = np.array(self.conn_db.execute_sql(sql),int)[0]
        cnt_ori = np.array(self.conn_ori.execute_sql(sql),int)[0]
        global_db, global_ori, attr_db, attr_ori = self.global_attr(x_value,pattern)
        if cnt_db == 0 or cnt_ori == 0:
            return 0
        return lamda*(global_db-attr_db)/(cnt_db)-(1-lamda)*(global_ori-attr_ori)/(cnt_ori)
    
    def get_score(self, pattern, lamda=0.9):
        score = 0.0
        for x_value in self.x_vaules:
            score += math.pow(self.single_point_score(x_value,pattern,lamda),2)
        return pow(score,0.5)

class Distance(Metrics):
    def single_point_score(self, x_value, pattern):
        global_db, global_ori, attr_db, attr_ori = self.global_attr(x_value,pattern)
        # print((global_db-attr_db)-(global_ori-attr_ori))
        return (global_db-attr_db)-(global_ori-attr_ori)

    def get_score(self, pattern):
        score = 0.0
        for x_value in self.x_vaules:
            score += math.pow(self.single_point_score(x_value,pattern),2)
        return math.pow(score,0.5)

class Aggravation(Metrics):
    def single_point_score(self, x_value, pattern:dict):
        conditions = []
        for key, value in pattern.items():
            conditions.append(f" {key} = '{value}' ")
        sql = 'select {} from (select * from {} where {}) t where {} and {}'.format(self.sequence.target,self.sequence.table,self.sequence.condition,self.sequence.get_x_condition(x_value),'and'.join(conditions))
        attr_db = np.array(self.conn_db.execute_sql(sql),float)[0]
        attr_ori = np.array(self.conn_ori.execute_sql(sql),float)[0]
        if not attr_db or math.isnan(attr_db):
            attr_db = 0
        if not attr_ori or math.isnan(attr_ori):
            attr_ori = 0
        return attr_db-attr_ori

    def get_score(self, pattern):
        score = 0.0
        for x_value in self.x_vaules:
            score += pow(self.single_point_score(x_value,pattern),2)
        return pow(score,0.5)

class Intervention(Metrics):
    def single_point_score(self, x_value, pattern:dict):
        global_db, global_ori, attr_db, attr_ori = self.global_attr(x_value,pattern)
        # print(f"{global_db},{global_ori},{attr_db},{attr_ori}")
        return (global_db-attr_db)-(global_ori-attr_ori)
    
    def get_score(self, pattern):
        score = 0.0
        for x_value in self.x_vaules:
            score += pow(self.single_point_score(x_value,pattern),2)
        return pow(score,0.5)

class ScoreFunction(Metrics):
    def get_score(self, pattern):
        conditions = []
        for key, value in pattern.items():
            conditions.append(f" {key} = '{value}' ")
        dis = Distance(self.sequence)
        distance = dis.get_score(dict({}))
        cnt_db = np.array(self.conn_db.execute_sql(f'select count(*) from {self.sequence.table} where {self.sequence.condition}'),int)[0]
        cnt_ori = np.array(self.conn_ori.execute_sql(f'select count(*) from {self.sequence.table} where {self.sequence.condition}'),int)[0]
        temp_distance = dis.get_score(pattern)
        sql = 'select count(*) from (select * from {} where {}) as t where {}'.format(self.sequence.table,self.sequence.condition,'and'.join(conditions))
        temp_cnt_db = np.array(self.conn_db.execute_sql(sql),float)[0]
        temp_cnt_ori = np.array(self.conn_ori.execute_sql(sql),float)[0]
        return (temp_distance/distance)*(1+temp_cnt_db/cnt_db+temp_cnt_ori/cnt_ori)

class Diagnosis(Metrics):
    def single_point_score(self, x_value, pattern:dict, alpha=0.1):
        _, _, attr_db, attr_ori = self.global_attr(x_value,pattern)
        if attr_db == 0 or attr_ori == 0:
            return 0
        return math.log(1/alpha)+attr_db*math.log((attr_ori+attr_db)/attr_db)+attr_ori*math.log((attr_ori+attr_db)/attr_ori)

    def get_score(self, pattern):
        score = 0.0
        for x_value in self.x_vaules:
            score += math.pow(self.single_point_score(x_value,pattern),2)
        return math.pow(score,0.5)   

class AbsoluteChange(Metrics):
    def single_point_score(self, x_value, pattern:dict):
        global_db, global_ori, attr_db, attr_ori = self.global_attr(x_value,pattern)
        return math.fabs(global_db-global_ori)-math.fabs((global_db-attr_db)-(global_ori-attr_ori))
    
    def get_score(self, pattern):
        score = 0.0
        for x_value in self.x_vaules:
            score += math.pow(self.single_point_score(x_value,pattern),2)
        return math.pow(score,0.5)  