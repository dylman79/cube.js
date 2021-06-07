import {OpenDistroQuery, OpenDistroQueryFilter} from "../../../src";

describe('OpenDistroQuery', () => {
    let sut: OpenDistroQuery;

    beforeEach(() => {
        const compilers = {compiler: {compilerCache: {getQueryCache:()=>{}}}, joinGraph:{buildJoin:() => {}}};
        sut = new OpenDistroQuery(compilers, null);
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
        expect(result instanceof OpenDistroQueryFilter).toBeTruthy();
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
        expect(result ).toBe('DATE_SUB(test, INTERVAL 5)');
    });

    it('should return addInterval', async () => {
        const result = sut.addInterval('test', 5);
        expect(result ).toBe('DATE_ADD(test, INTERVAL 5)');
    });

    it('should return timeGroupedColumn - day', async () => {
        const result = sut.timeGroupedColumn('day', 'test');
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-MM-dd 00:00:00.000\')');
    });

    it('should return timeGroupedColumn - week', async () => {
        const result = sut.timeGroupedColumn('week', 'test');
        expect(result ).toBe('DATE_FORMAT(DATE_SUB(test, INTERVAL CAST(DATE_FORMAT(test, \'%w\') AS INTEGER) - 1 day), \'yyyy-MM-dd HH:00:00.000\')');
    });

    it('should return timeGroupedColumn - hour', async () => {
        const result = sut.timeGroupedColumn('hour', 'test');
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-MM-dd HH:00:00.000\')');
    });

    it('should return timeGroupedColumn - minute', async () => {
        const result = sut.timeGroupedColumn('minute', 'test');
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-MM-dd HH:mm:00.000\')');
    });

    it('should return timeGroupedColumn - second', async () => {
        const result = sut.timeGroupedColumn('second', 'test');
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-MM-dd HH:mm:ss.000\')');
    });

    it('should return timeGroupedColumn - month', async () => {
        const result = sut.timeGroupedColumn('second', 'test');
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-MM-01 00:00:00.000\')');
    });

    it('should return timeGroupedColumn - year', async () => {
        const result = sut.timeGroupedColumn('year', 'test');
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-01-01 00:00:00.000\')');
    });

    it('should return groupByClause ungrouped', async () => {
        sut.ungrouped = true;
        const result = sut.groupByClause();
        expect(result ).toBe('');
    });

    it('should return groupByClause grouped', async () => {
        const result = sut.groupByClause();
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-01-01 00:00:00.000\')');
    });

    it('should return orderHashToString', async () => {
        const result = sut.orderHashToString('test');
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-01-01 00:00:00.000\')');
    });

    it('should return getFieldAlias', async () => {
        const result = sut.getFieldAlias('test');
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-01-01 00:00:00.000\')');
    });

    it('should return escapeColumnName', async () => {
        const result = sut.escapeColumnName('test');
        expect(result ).toBe('DATE_FORMAT(test, \'yyyy-01-01 00:00:00.000\')');
    });
});
