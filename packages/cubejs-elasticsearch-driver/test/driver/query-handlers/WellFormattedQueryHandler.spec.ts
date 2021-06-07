import {Client} from '@elastic/elasticsearch';
import { mocked } from 'ts-jest/utils';
import {WellFormattedQueryHandler} from '../../../src/driver/query-handlers/WellFormattedQueryHandler';

jest.mock('@elastic/elasticsearch', () => {
    return {
        Client: jest.fn().mockImplementation(() => {
            return {
                sql: {
                    query: () => {return "test" },
                }
            };
        })
    };
});

describe('WellFormattedQueryHandler', () => {

    const MockedClient = mocked(Client, true);
    let sut: WellFormattedQueryHandler;
    let client: Client;

    beforeEach(async () => {
        MockedClient.mockClear();
        client = new Client();
        sut = new WellFormattedQueryHandler(client, 'json');
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    it('should only support json and jdbc(as default)', async () => {
        expect(() => { new WellFormattedQueryHandler(client, 'json')}).not.toThrow();
        expect(() => { new WellFormattedQueryHandler(client, 'jdbc')}).not.toThrow();
        expect(() => { new WellFormattedQueryHandler(client, 'incorrect')}).toThrow();
    });

    it('should query index', async () => {
        const result = {
            "columns": [{
                "name": "count",
                "type": "integer"
            }],
            "total": 1,
            "rows": [[2000]],
            "size": 1,
            "status": 200
        }

        jest.spyOn(client.sql, 'query').mockReturnValue({ body: result }  as any);
        await sut.query('test', []);
        expect(client.sql.query).toHaveBeenCalledTimes(1);
    });

    it('should throw if no body', async (done) => {
        jest.spyOn(client.sql, 'query').mockReturnValue({ } as any);
        try {
            await sut.query('test', []);
            fail();
        } catch (e) {
            done();
        }
    });

    it('should parse results for elastic co format', async () => {

        const result = {
            "columns": [{
                "name": "count",
                "type": "integer"
            }],
            "total": 1,
            "rows": [[2000]],
            "size": 1,
            "status": 200
        }

        jest.spyOn(client.sql, 'query').mockReturnValue({ body: result } as any);
        const actual = await sut.query('test', []);
        expect(actual).toStrictEqual([{"count": 2000}]);
    });

    it('should parse results for open distro format', async () => {

        const result = {
            "schema": [{
                "name": "count",
                "type": "integer"
            }],
            "total": 1,
            "datarows": [[2000]],
            "size": 1,
            "status": 200
        }

        const localHandler = new WellFormattedQueryHandler(client, 'jdbc', 'datarows', 'schema');
        jest.spyOn(client.sql, 'query').mockReturnValue({ body: result } as any);
        const actual = await localHandler.query('test', []);

        expect(actual).toStrictEqual([{"count": 2000}]);
    });
});
