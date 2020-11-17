import sqlite3
import pprint
import whereParser

class rowFiller:
    """Generates the rows as sqlite commands
    """
    def __init__(self, sw, pruneSql,
            numAids=1,
            aidDist='distinctPerRow'):
        self.sw = sw
        self.pruneSql = pruneSql
        self.numAids = numAids
        self.aidDist = aidDist
        self.dbName = self._makeDbName()
        for table in self.sw.iterTabs():
            self._processOneTable(table)
    
    def _processOneTable(self,table):
        for (col,colType) in self.sw.iterColTypes(table):
            pass

    def _makeDbName(self):
        dbName = ''
        for table in self.sw.iterTabs():
            dbName += table
            for (col,colType) in self.sw.iterColTypes(table):
                pass
        return dbName

if __name__ == "__main__":
    pp = pprint.PrettyPrinter(indent=4)
    tests = [
        {   'sql': "select count(*) from tab where t1='y' or i1=1",
            'prune': "select * from tab where t1='y' and i1=1",
        },
    ]
    for test in tests:
        sw = whereParser.simpleWhere(test['sql'])
        rf = rowFiller(sw,test['prune'])