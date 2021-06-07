import {ElasticCoQuery, ElasticCoQueryFilter} from "../../../src";

describe('ElasticCoQueryFilter', () => {
    jest.mock('../../../src/driver/elastic-co/ElasticCoQuery');
    let sut: ElasticCoQueryFilter;

    beforeEach(() => {
        const filter = {dimension: null, measure: null, operator: ''};
        let compilers = {compiler: {compilerCache: {getQueryCache:()=>{}}}, joinGraph:{buildJoin:() => {}}};
        const query = new ElasticCoQuery(compilers, null);
        sut = new ElasticCoQueryFilter(query, filter);
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    it('should return ElasticCoQueryFilter', async () => {
        const filter = sut.likeIgnoreCase('test', false, 'test1');
        expect(filter).toBe(' MATCH(test, $0$, \'fuzziness=AUTO:1,5\')');
    });
});
