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
    # Let's put all the global variables here for conveniene
    sqlStr = None
    sqlTree = None  # The whole SQL tree
    wTree = None  # The parsed WHERE clause
    # List of tables and related schema
    schema = {}
    colTypes = {}
    booleanTerms = ['and','or']
    operators = ['eq']

    def __init__(self, sqlStr=None):
        if not sqlStr:
            print("ERROR: simpleWhere: Need to define an SQL string")
            return
        self.sqlStr = sqlStr
        self.sqlTree = moz.parse(sqlStr)
        if 'where' not in self.sqlTree:
            print("ERROR: simpleWhere: SQL must have WHERE clause")
            return
        self.wTree = self.sqlTree['where']
        self._makeTablesColumns()

    def iterColTypes(self,table):
        for col,colType in self.schema[table].items():
            yield(col,colType)

    def iterTabs(self):
        for tab in self.schema.keys():
            yield tab

    def _getColTypeFromLeaf(self,leaf):
        ''' pulls the column and type from leaf node and puts in self.colTypes '''
        colName = leaf[0]
        if colName in self.colTypes:
            return
        if colName[0] == 'i':
            self.colTypes[colName] = 'integer'
        elif colName[0] == 't':
            self.colTypes[colName] = 'text'
        elif colName[0] == 'r':
            self.colTypes[colName] = 'real'
        else:
            print(f"ERROR: getColTypeFromLeaf: {leaf}")
            quit()

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
            func(tree[key])

    def _makeTablesColumns(self):
        ''' For now I'm assuming only one table (no JOIN) '''
        self.schema = {}
        self.colTypes = {}
        self._parseWhere(self.wTree, self._getColTypeFromLeaf)
        self.schema[self.sqlTree['from']] = self.colTypes

if __name__ == "__main__":
    pp = pprint.PrettyPrinter(indent=4)
    sqls = [
        "select count(*) from tab where r1 = 1 or (t1='y' and i1=1)",
        "select count(*) from tab where t1='y'",
        "select count(*) from tab where t1='y' and i1=1",
    ]
    for sql in sqls:
        print("--------")
        x = simpleWhere(sql)
        print(x.sqlStr)
        print("WHERE Tree:")
        pp.pprint(x.wTree)
        print("Parse of WHERE Tree:")
        x._parseWhere(x.wTree,x._printLeaf)
        print("Schema:")
        pp.pprint(x.schema)
        print("Tables and column types:")
        for table in x.iterTabs():
            print(f"    Table: {table}")
            for (col,colType) in x.iterColTypes(table):
                print(F"        Col: {col}, Type: {colType}")