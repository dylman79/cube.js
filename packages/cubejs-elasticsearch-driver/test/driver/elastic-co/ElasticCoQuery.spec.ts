import {ElasticCoQuery, ElasticCoQueryFilter} from "../../../src";

describe('ElasticCoQuery', () => {
    let sut: ElasticCoQuery;

    beforeEach(() => {
        const compilers = {compiler: {compilerCache: {getQueryCache:()=>{}}}, joinGraph:{buildJoin:() => {}}};
        sut = new ElasticCoQuery(compilers, null);
    });

    it('should return ElasticCoQueryFilter', async () => {
        const filter = sut.newFilter('test');
        expect(filter instanceof ElasticCoQueryFilter).toBeTruthy();
    });
});
