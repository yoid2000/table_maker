import moz_sql_parser as moz

sql = '''
select count(*)
from t
where a = 1 and b = 2 and c = 3 or d = 4
'''

tree = moz.parse(sql)

print("Let's look at the entire sql tree")
for key,val in tree.items():
    print(key,val)

print("Let's look at the where clause")
wtree = tree['where']
for key,val in wtree.items():
    print(key,val)