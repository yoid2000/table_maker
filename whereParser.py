import moz_sql_parser as moz
import pprint

class simpleWhere:
    """Parse out the WHERE clause, and get various information

        Incoming SQL must be of the form:
           SELECT foo
           FROM table
           WHERE stuff

        All column names in the WHERE clause are of the form i1 (two letters)
        The first letter is 'i' if integer, 'r' if real, 't' if text, 'd' if date
    """

    def __init__(self, sqlStr=None):
        if not sqlStr:
            print("ERROR: simpleWhere: Need to define an SQL string")
            return
        self.schema = {}
        self.colTypes = {}
        self.conditions = []
        self.booleanTerms = ['and','or']
        self.operators = ['eq','neq','between','gt','lt','lte','gte']
        self.sqlStr = sqlStr
        self.sqlTree = moz.parse(sqlStr)    # The whole SQL tree
        if 'where' not in self.sqlTree:
            print("ERROR: simpleWhere: SQL must have WHERE clause")
            return
        self.wTree = self.sqlTree['where']   # The parsed WHERE clause
        self._makeTablesColumns()

    def iterConditions(self,table):
        for condition in self.conditions:
            yield condition

    def getColName(self,condition):
        return condition['colName']

    def getOperation(self,condition):
        return condition['operation']

    def getOperands(self,condition):
        return condition['operands']

    def iterCols(self,table):
        for col,_ in self.schema[table].items():
            yield(col)

    def iterTabs(self):
        for tab in self.schema.keys():
            yield tab

    def getColType(self,table,column):
        return self.schema[table][column]['type']

    def _getColTypeFromLeaf(self,leaf):
        ''' pulls the column and type from leaf node and puts in self.colTypes '''
        operation = next(iter(leaf))
        colName = leaf[operation][0]
        if colName in self.colTypes:
            return
        self.colTypes[colName] = {'type':''}
        if colName[0] == 'i':
            self.colTypes[colName]['type'] = 'integer'
        elif colName[0] == 't':
            self.colTypes[colName]['type'] = 'text'
        elif colName[0] == 'r':
            self.colTypes[colName]['type'] = 'real'
        else:
            print(f"ERROR: getColTypeFromLeaf: {leaf}")
            quit()

    def _getConditionFromLeaf(self,leaf):
        ''' pulls the column and type from leaf node and puts in self.colTypes '''
        operation = next(iter(leaf))
        colName = leaf[operation][0]
        operands = []
        for operand in leaf[operation][1:]:
            if type(operand) is dict and next(iter(operand)) == 'literal':
                # This is a string constant
                operands.append(operand['literal'])
            else:
                operands.append(operand)
        self.conditions.append({'operation':operation,
                                'colName':colName,
                                'operands':operands})

    def _printLeaf(self,leaf):
        print(f"    {leaf}")
    
    def _parseWhere(self,tree,func):
        ''' tree is a dict '''
        if len(tree) > 1:
            print(f"ERROR: parseWhere: only expcted one item in tree {tree}")
            quit()
        key = next(iter(tree))
        if key in self.booleanTerms:
            for subTree in tree[key]:
                self._parseWhere(subTree,func)
        else:
            func(tree)

    def _makeTablesColumns(self):
        ''' For now I'm assuming only one table (no JOIN) '''
        self.schema = {}
        self.colTypes = {}
        self._parseWhere(self.wTree, self._getColTypeFromLeaf)
        self.conditions = []
        self._parseWhere(self.wTree, self._getConditionFromLeaf)
        self.schema[self.sqlTree['from']] = self.colTypes

if __name__ == "__main__":
    pp = pprint.PrettyPrinter(indent=4)
    sqls = [
        "select count(*) from tab where t1='y'",
        "select count(*) from tab where t1='y' and i1=1",
        "select count(*) from tab where r1 <> 1 or (t1='y' and i1=1) or (i2 < 10 and r2 between 1.1 and 2.2)",
        "select count(*) from tab where r1 <> 1 or (t1='y' and i1 > 1) or (i2 <= 10 and r2 >= 1.1)",
        "select count(*) from tab where t1 in ('a','b','c','d')",
    ]
    for sql in sqls:
        print("--------")
        sw = simpleWhere(sql)
        print(sw.sqlStr)
        print("WHERE Tree:")
        pp.pprint(sw.wTree)
        print("Parse of WHERE Tree:")
        sw._parseWhere(sw.wTree,sw._printLeaf)
        print("Schema (native dict):")
        pp.pprint(sw.schema)
        print("Conditions (native dict):")
        pp.pprint(sw.conditions)
        print("Tables and various values:")
        for table in sw.iterTabs():
            print(f"    Table: {table}")
            print(f"    Cols and Types:")
            for col in sw.iterCols(table):
                print(F"        Col: {col}, Type: {sw.getColType(table,col)}")
            print(f"    Conditions:")
            for co in sw.iterConditions(table):
                print(f"        column {sw.getColName(co)}, operation {sw.getOperation(co)}, operands {sw.getOperands(co)}")