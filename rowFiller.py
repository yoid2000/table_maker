import sqlite3
import pprint
import os.path
import itertools
import whereParser

class rowFiller:
    """Generates the rows as sqlite commands
    """
    def __init__(self, sw, pruneSql,
            numAids=1,
            aidDist='distinctPerRow',
            numRowsPerCombination=10):
        self.sw = sw
        self.pruneSql = pruneSql
        self.numAids = numAids
        self.aidDist = aidDist
        self.numRowsPerCombination = numRowsPerCombination
        self.maxDbName = 50
        self.dbName = self._makeDbName()
        self.dbPath = os.path.join('tables',self.dbName)
        self.conn = sqlite3.connect(self.dbPath)
        self.cur = self.conn.cursor()
        self.failedCombinations = []
        for table in self.sw.iterTabs():
            self.conditions = list(self.sw.iterConditions(table))
            self._processOneTable(table)
        self.conn.close()
    
    def _processOneTable(self,table):
        columns = list(self.sw.iterCols(table))
        self._defineTableInDb(table,columns)
        # For now we have one distinct AID per row, numerically increasing
        # Later we'll have different AID distributions and multiple AIDs
        aids = [1]
        # Make all possible True/False column combinations
        for comb in itertools.product([True,False],repeat=len(self.conditions)):
            ''' For each combination, loop through each column and try to find a value
                that satisfies all of the conditions in the combination (noting that a
                given column can be in more than one condition). The approach will be
                to find valid values for all conditions, and then check each one against
                all other conditions to see if it passes all. If no values work for all
                conditions, then we presume that the conditions can't be satisfied and
                we move on. (This may fail to find working values when such values exist.)
            '''
            values = []
            # We are going to find all of the candidate values for all columns in advance,
            # and then resolve them, because some conditions can involve multiple columns
            candidateValues = {}
            relevantConditions = {}
            relevantResults = {}
            for column in columns:
                candidateValues[column] = []
                relevantConditions[column],relevantResults[column] = self._getRelevantConditions(column,comb)
                for i in range(len(relevantConditions[column])):
                    condition = relevantConditions[column][i]
                    result = relevantResults[column][i]
                    # At this point, `result` is the desired True/False result of `condition`
                    self._addCandidateValues(candidateValues[column],condition,result)
            # Now see if any of the candidate values work. For now we don't deal with multi-column
            # conditions
            allValuesWork = True
            for column in columns:
                workingValue = self._findWorkingValue(candidateValues,column,
                                                      relevantConditions, relevantResults)
                if workingValue is None:
                    # can't find values for this combination
                    self._addFailedCombination(columns,column,comb,
                                               relevantConditions[column],candidateValues[column])
                    allValuesWork = False
                else:
                    values.append(workingValue)
            if allValuesWork is False:
                continue
            # `values` contains the list of working values in the order that the columns
            # appear in the sqlite table
            for _ in range(self.numRowsPerCombination):
                self._makeRow(table,aids,values)
                aids[0] += 1
        self.conn.commit()

    def _addFailedCombination(self,columns,column,comb,conditions,values):
        self.failedCombinations.append({'columns':columns,
                                        'column':column,
                                        'combination':comb,
                                        'candidateValues':values,
                                        'conditions':conditions})

    def _getRelevantConditions(self,column,comb):
        relevantConditions = []
        relevantResults = []
        for i in range(len(self.conditions)):
            condition = self.conditions[i]
            result = comb[i]
            # At this point, `result` is the desired True/False result of `condition`
            condColumn = self.sw.getColName(condition)
            if column == condColumn:
                relevantConditions.append(condition)
                relevantResults.append(result)
        return relevantConditions,relevantResults

    def _findWorkingValue(self,candidateValues,column,relevantConditions,relevantResults):
        for value in candidateValues[column]:
            passed = True
            for i in range(len(relevantConditions[column])):
                if self._valuePasses(value,relevantConditions[column][i],
                                     relevantResults[column][i]) is False:
                    passed = False
                    break
            if passed is True:
                # Found working value
                return value
            else:
                continue    # try next value
        return None

    def _makeRow(self,table,aids,values):
        sql = f'''INSERT INTO {table} VALUES ('''
        for aid in aids:
            sql += f'''{aid}, '''
        for value in values:
            if type(value) is str:
                sql += f"'{value}', "
            else:
                sql += f"{value}, "
        sql = sql[:-2]
        sql += ')'
        self.cur.execute(sql)

    def _addCandidateValues(self,candidateValues,condition,result):
        operation = self.sw.getOperation(condition)
        operands = self.sw.getOperands(condition)
        if ((operation == 'eq' and result is True) or
            (operation == 'neq' and result is False) or
            (operation == 'between' and result is True)):
            candidateValues.append(operands[0])
            if operation == 'between':
                candidateValues.append(operands[1])
        elif ((operation == 'eq' and result is False) or
            (operation == 'neq' and result is True) or
            ((operation == 'gt' or operation == 'gte') and result is True) or
            ((operation == 'lt' or operation == 'lte') and result is False)):
            self._addBiggerValues(operands[0],candidateValues)
        elif (((operation == 'gt' or operation == 'gte') and result is False) or
            ((operation == 'lt' or operation == 'lte') and result is True)):
            self._addSmallerValues(operands[0],candidateValues)
        elif (operation == 'between' and result is False):
            self._addSmallerValues(operands[0],candidateValues)
            self._addBiggerValues(operands[1],candidateValues)
        else:
            print(f"Error: addCandidateValues: no matching branch {condition}, {result}")
            quit()

    def _valuePasses(self,value,condition,result):
        operation = self.sw.getOperation(condition)
        operands = self.sw.getOperands(condition)
        if ((operation == 'eq' and result is True) or
            (operation == 'neq' and result is False)):
            if value == operands[0]: return True
            else: return False
        elif ((operation == 'eq' and result is False) or
            (operation == 'neq' and result is True)):
            if value != operands[0]: return True
            else: return False
        elif ((operation == 'gt' and result is True) or
            (operation == 'lte' and result is False)):
            if value > operands[0]: return True
            else: return False
        elif ((operation == 'gt' and result is False) or
            (operation == 'lte' and result is True)):
            if value <= operands[0]: return True
            else: return False
        elif ((operation == 'lt' and result is True) or
            (operation == 'gte' and result is False)):
            if value < operands[0]: return True
            else: return False
        elif ((operation == 'lt' and result is False) or
            (operation == 'gte' and result is True)):
            if value >= operands[0]: return True
            else: return False
        elif (operation == 'between' and result is True):
            if value >= operands[0] and value <= operands[1]: return True
            else: return False
        elif (operation == 'between' and result is False):
            if value < operands[0] or value > operands[1]: return True
            else: return False
        else:
            print(f"Error: valuePasses: no matching branch {condition}, {result}")
            quit()

    def _getValueFromCondition(self,condition,result):
        column = self.sw.getColName(condition)
        operation = self.sw.getOperation(condition)
        operands = self.sw.getOperands(condition)
        if ((operation == 'eq' and result is True) or
            (operation == 'neq' and result is False) or
            (operation == 'between' and result is True)):
            value = operands[0]
        elif ((operation == 'eq' and result is False) or
            (operation == 'neq' and result is True) or
            ((operation == 'gt' or operation == 'gte') and result is True) or
            ((operation == 'lt' or operation == 'lte') and result is False)):
            value = self._makeBiggerValue(operands[0])
        elif (((operation == 'gt' or operation == 'gte') and result is False) or
            ((operation == 'lt' or operation == 'lte') and result is True)):
            value = self._makeSmallerValue(operands[0])
        elif (operation == 'between' and result is False):
            value = self._makeBiggerValue(operands[1])
        else:
            print(f"Error: getValueFromCondition: no matching branch {condition}, {result}")
            quit()
        return column,value

    def _addBiggerValues(self,value,valList):
        if type(value) is str:
            # Not guaranteed to be bigger, but good chance
            valList.append('zz')
        else:
            valList.append(value+1)
            valList.append(value+2)

    def _makeBiggerValue(self,value):
        if type(value) is str:
            # Not guaranteed to be bigger, but good chance
            return 'zz'
        else:
            return value+1

    def _addSmallerValues(self,value,valList):
        if type(value) is str:
            # Not guaranteed to be bigger, but good chance
            valList.append('AA')
        else:
            valList.append(value-1)
            valList.append(value-2)

    def _makeSmallerValue(self,value):
        if type(value) is str:
            # Not guaranteed to be smaller, but good chance
            return 'AA'
        else:
            return value-1

    def _defineTableInDb(self,table,columns):
        sql = f'''DROP TABLE IF EXISTS {table}'''
        self.cur.execute(sql)
        sql = f'''CREATE TABLE {table} ('''
        # add AID columns
        for i in range(self.numAids):
            sql += f'''aid{i} integer, '''
        # add columns
        for column in columns:
            sql += f'''{column} {self.sw.getColType(table,column)}, '''
        # strip last comma-space
        sql = sql[:-2]
        sql += ')'
        self.cur.execute(sql)

    def _makeDbName(self):
        dbName = ''
        for table in self.sw.iterTabs():
            if len(dbName) > self.maxDbName:
                break
            dbName += table
            for condition in self.sw.iterConditions(table):
                dbName += '_'
                dbName += self.sw.getColName(condition)
                dbName += self.sw.getOperation(condition)
                dbName += str(self.sw.getOperands(condition)[0])
        return dbName + '.db'

if __name__ == "__main__":
    pp = pprint.PrettyPrinter(indent=4)
    tests = [
        {   'sql': "select count(*) from tab where t1='y' or i1=1 or r1=1.1",
            'prune': "select * from tab where (t1='y' or i1=1) and r1=1.1 ",
        },
    ]
    for test in tests:
        sw = whereParser.simpleWhere(test['sql'])
        rf = rowFiller(sw,test['prune'])
        print(rf.dbPath)
        print(test['sql'])
        pp.pprint(rf.failedCombinations)