import sqlite3
import pprint
import os.path
import itertools
import random
import string
import pandas as pd
import numpy as np
import whereParser

class rowFiller:
    """Generates the rows as sqlite commands
    """
    def __init__(self, sw,
            numAids=1,
            aidDist='distinctPerRow',
            useTestDbName=True,
            printIntermediateTables=True,
            numRowsPerCombination=10):
        self.pp = pprint.PrettyPrinter(indent=4)
        self.sw = sw
        self.printIntermediateTables = printIntermediateTables
        self.useTestDbName = useTestDbName
        self.numAids = numAids
        self.aidDist = aidDist
        self.numRowsPerCombination = numRowsPerCombination
        self.maxDbName = 50
        self.dbName = self._makeDbName()
        self.dbPath = os.path.join('tables',self.dbName)
        self.failedCombinations = []
        self.allColumns = []
        self.newRows = []
        # This will be one list per table
        self.baseData = {}
        self.baseDf = {}

    def queryDb(self,sql):
        self.conn = sqlite3.connect(self.dbPath)
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        answer = self.cur.fetchall()
        self.conn.close()
        return answer

    def queryDf(self,table,query):
        df = self.baseDf[table].query(query)
        return df

    def makeBaseTables(self):
        ''' This builds the basic table that has as many matching combinations
            as possible. It also makes the base dataframe from the baseData
        '''
        for table in self.sw.iterTabs():
            self.baseData[table] = []
            self.conditions = list(self.sw.iterConditions(table))
            self._processOneTable(table,self.baseData[table])
            if self.printIntermediateTables:
                self.pp.pprint(self.baseData[table])
        for table,data in self.baseData.items():
            self.allColumns = []
            for i in range(self.numAids):
                self.allColumns.append(f"aid{i+1}")
            for column in list(self.sw.iterCols(table)):
                self.allColumns.append(column)
            self.baseDf[table] = pd.DataFrame(data, columns=self.allColumns) 
    
    def baseTablesToDb(self):
        ''' This takes the base table and writes it to an sql db '''
        self.conn = sqlite3.connect(self.dbPath)
        for table,df in self.baseDf.items():
            df.to_sql(table,con=self.conn, if_exists='replace')
        self.conn.close()

    def appendDf(self,table,spec):
        ''' This adds the rows defined by the spec to the base dataframe
            Columns that are absent in the spec are assumed to require new distinct values
        '''
        dfSpec = {}
        # First figure out how many rows we need
        numRows = 1   # default assumption
        for _,vals in spec.items():
            numRows = max(numRows,len(vals))
        for column in self.allColumns:
            dfSpec[column] = []
            for i in range(numRows):
                if column not in spec or len(spec[column]) <= i or spec[column][i] == 'unique':
                    dfSpec[column].append(self._getNewVal(table,column))
                else:
                    dfSpec[column].append(spec[column][i])
        df = pd.DataFrame(dfSpec)
        self._addToNewRows(dfSpec)
        self.baseDf[table] = self.baseDf[table].append(df)

    def stripDf(self,table,query):
        ''' This removes the rows that match the dataframe query
        '''
        bdf = self.baseDf[table]
        notQuery = f"not({query})"
        dfKeep = bdf.query(notQuery)
        # dfKeep contains everything except what matches the query
        self.baseDf[table] = dfKeep
    
    def iterNewRows(self):
        for newRow in self.newRows:
            yield newRow

    def getNewRowColumn(self,col):
        return(self.newRows[0][col])

    def _addToNewRows(self,spec):
        someCol = next(iter(spec))
        numRows = len(spec[someCol])
        for i in range(numRows):
            newRow = {}
            for col,val in spec.items():
                newRow[col] = val[i]
            self.newRows.append(newRow)

    def _getNewVal(self,table,column):
        col = self.baseDf[table][column]
        maxVal = col.max()
        if type(maxVal) is str:
            return ''.join(random.choice(string.ascii_lowercase) for _ in range(3))
        elif np.issubdtype(maxVal,np.integer) or np.issubdtype(maxVal,np.float):
            return maxVal + 1
        else:
            print(f"ERROR: _getNewVal: {table}, {column}, {maxVal}")
            quit()

    def stripAllButX(self,table,query,numLeft=1):
        ''' This removes the rows that match the dataframe query leaving numLeft
            number of distinct AIDs
        '''
        bdf = self.baseDf[table]
        dfRemove = bdf.query(query)
        notQuery = f"not({query})"
        dfKeep = bdf.query(notQuery)
        # dfRemove contains the rows that we want to drop
        # dfKeep contains everything else
        # We want to shift rows for numLeft distinct users from dfRemove to dfKeep
        for _ in range(numLeft):
            aidVal = dfRemove['aid1'].iloc[0]
            dfShift = dfRemove.query("aid1 == @aidVal")
            dfKeep = dfKeep.append(dfShift)
            dfRemove = dfRemove.query("aid1 != @aidVal")
        self.baseDf[table] = dfKeep
        self.conn = sqlite3.connect(self.dbPath)
        dfKeep.to_sql(table,con=self.conn, if_exists='replace')
        self.conn.close()
    
    def _processOneTable(self,table,data):
        columns = list(self.sw.iterCols(table))
        # For now we have one distinct AID per row, numerically increasing
        # Later we'll have different AID distributions and multiple AIDs
        aids = [0]
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
                self._makeRow(data,aids,values)
                aids[0] += 1

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

    def _makeRow(self,data,aids,values):
        row = []
        for aid in aids:
            row.append(aid)
        for value in values:
            row.append(value)
        data.append(row)

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

    def _makeDbName(self):
        if self.useTestDbName:
            return 'testAttack.db'
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
        {   # The attack here is where there is one user with i1=12345. We want to know
            # if that user has value t1='y' or not.
            'sql': "select count(*) from tab where t1='y' or i1=12345",
            'attack1': "select count(*) from tab where t1='y' or i1=12345",
            'attack2': "select count(*) from tab where t1='y'",
            # I want to make a scenario where the victim does not have t1=y. So I remove all
            # but one of the users that has i1=12345 but not t1=y
            'strip': {'table':'tab','query': "t1 != 'y' and i1 == 12345"},
        },
    ]
    for test in tests:
        sw = whereParser.simpleWhere(test['sql'])
        rf = rowFiller(sw)
        rf.makeBaseTables()
        if len(rf.failedCombinations) > 0:
            print("Failed Combinations:")
            pp.pprint(rf.failedCombinations)
        rf.baseTablesToDb()
        print("Original base dataframe:")
        pp.pprint(rf.baseDf)
        print(rf.dbPath)
        rf.stripAllButX(test['strip']['table'],test['strip']['query'])
        print("Stripped base dataframe:")
        pp.pprint(rf.baseDf)
        print(f"{test['attack1']}:")
        pp.pprint(rf.queryDb(test['attack1']))
        print(f"{test['attack2']}:")
        pp.pprint(rf.queryDb(test['attack2']))