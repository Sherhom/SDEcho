# Sql格式：select x,target from table where condtion group by x;
import json

class Sequence:
    def __init__(self, x:list, target:str, table:str, condition:str):
        self.x = x
        self.x_str = ','.join(self.x)
        self.target = target 
        self.table = table
        self.condition = condition
    
    def __init__(self, file):
        with open(file,'r') as f:
            data = json.load(f)
        self.x = data['x']
        self.x_str = ','.join(self.x)
        self.target = data['target']
        self.table = data['table']
        self.condition = data['condition']
        self.dimension = data['dimension']
        self.newValue = data['new value']
    
    def get_x_condition(self,value:list):
        conditions = []
        for i,temp in enumerate(value):
            conditions.append(f" {self.x[i]} = '{temp}' ")
        return 'and'.join(conditions)

    def get_sql(self):
        return f"select {','.join(self.x)},{self.target} from {self.table} where {self.condition} group by {self.x_str}"
    