import {OpenDistroQuery, OpenDistroQueryFilter} from "../../../src";

describe('OpenDistroQueryFilter', () => {
    jest.mock('../../../src/driver/open-distro/OpenDistroQuery');
    let sut: OpenDistroQueryFilter;

    beforeEach(() => {
        const filter = {dimension: null, measure: null, operator: ''};
        let compilers = {compiler: {compilerCache: {getQueryCache:()=>{}}}, joinGraph:{buildJoin:() => {}}};
        const query = new OpenDistroQuery(compilers, {});
        sut = new OpenDistroQueryFilter(query, filter);
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    it('should return OpenDistroQueryFilter', async () => {
        const filter = sut.likeIgnoreCase('test', false, 'test1');
        expect(filter).toBe(' test LIKE \'%$0$%\')');
    });
});
