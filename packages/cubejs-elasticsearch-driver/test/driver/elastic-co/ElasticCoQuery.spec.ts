import {ElasticCoQuery, ElasticCoQueryFilter} from "../../../src";

describe('ElasticCoQuery', () => {
    let sut: ElasticCoQuery;

    beforeEach(() => {
        const compilers = {compiler: {compilerCache: {getQueryCache:()=>{}}}, joinGraph:{buildJoin:() => {}}};
        sut = new ElasticCoQuery(compilers, null);
    });

    it('should return new filter', async () => {
        const result = sut.newFilter(
            {
                "dimension": "Transitions.projecttype",
                "operator": "equals",
                "values": [
                    "Homecharge Domestic EV Installation"
                ]
            });
        expect(result instanceof ElasticCoQueryFilter).toBeTruthy();
    });

    it('should return convertTz', async () => {
        const result = sut.convertTz('test');
        expect(result ).toBe('test'); // Not supported - respond with input
    });

    it('should return timeStampCast', async () => {
        const result = sut.timeStampCast('test');
        expect(result ).toBe('test'); // Not supported - respond with input
    });

    it('should return dateTimeCast', async () => {
        const result = sut.dateTimeCast('test');
        expect(result ).toBe('test'); // Not supported - respond with input
    });

    it('should return subtractInterval', async () => {
        const result = sut.subtractInterval('test', 5);
        expect(result ).toBe('test - INTERVAL 5');
    });

    it('should return addInterval', async () => {
        const result = sut.addInterval('test', 5);
        expect(result ).toBe('test + INTERVAL 5');
    });

    it('should return timeGroupedColumn - day', async () => {
        const result = sut.timeGroupedColumn('day', 'test');
        expect(result ).toBe('DATE_TRUNC(\'day\', test::datetime)');
    });

    it('should return timeGroupedColumn - week', async () => {
        const result = sut.timeGroupedColumn('week', 'test');
        expect(result ).toBe('DATE_TRUNC(\'week\', test::datetime)');
    });

    it('should return timeGroupedColumn - hour', async () => {
        const result = sut.timeGroupedColumn('hour', 'test');
        expect(result ).toBe('DATE_TRUNC(\'hour\', test::datetime)');
    });

    it('should return timeGroupedColumn - minute', async () => {
        const result = sut.timeGroupedColumn('minute', 'test');
        expect(result ).toBe('DATE_TRUNC(\'minute\', test::datetime)');
    });

    it('should return timeGroupedColumn - second', async () => {
        const result = sut.timeGroupedColumn('second', 'test');
        expect(result ).toBe('DATE_TRUNC(\'second\', test::datetime)');
    });

    it('should return timeGroupedColumn - month', async () => {
        const result = sut.timeGroupedColumn('month', 'test');
        expect(result ).toBe('DATE_TRUNC(\'month\', test::datetime)');
    });

    it('should return timeGroupedColumn - year', async () => {
        const result = sut.timeGroupedColumn('year', 'test');
        expect(result ).toBe('DATE_TRUNC(\'year\', test::datetime)');
    });

    it('should return groupByClause ungrouped', async () => {
        sut.ungrouped = true;
        const result = sut.groupByClause();
        expect(result ).toBe('');
    });

    it('should return groupByClause grouped', async () => {
        sut.ungrouped = false;
        sut.dimensions = [{
            dimension: 'test',
            selectColumns: () => {
                return 'testing';
            },
            dimensionSql: () => {
                return 'test';
            }
        }];
        const result = sut.groupByClause();
        expect(result ).toBe(' GROUP BY test');
    });

    it('should return orderHashToString', async () => {
        sut.dimensions = [{
            dimension: 'test',
            selectColumns: () => {
                return 'testing';
            },
            dimensionSql: () => {
                return 'test';
            }
        }];
        const result = sut.orderHashToString({id: 'test', desc: 'DESC'});
        expect(result ).toBe('test DESC');
    });

    it('should return null if hash is null', async () => {
        const result = sut.orderHashToString(null);
        expect(result ).toBeNull();
    });

    it('should return null if hash has no id', async () => {
        const result = sut.orderHashToString({});
        expect(result ).toBeNull();
    });

    it('should return getFieldAlias - using dimensions', async () => {
        sut.dimensions = [{
            dimension: 'test',
            selectColumns: () => {
                return 'testing';
            },
            dimensionSql: () => {
                return 'test';
            }
        }];
        const result = sut.getFieldAlias('test');
        expect(result ).toBe('test');
    });

    it('should return getFieldAlias - using measures fallback', async () => {
        sut.measures = [{
            expressionName: 'test',
            aliasName: () => {
                return 'testing';
            }
        }];
        const result = sut.getFieldAlias('test');
        expect(result ).toBe('testing');
    });

    it('should return escapeColumnName', async () => {
        const result = sut.escapeColumnName('test');
        expect(result).toBe('test');
    });
});
