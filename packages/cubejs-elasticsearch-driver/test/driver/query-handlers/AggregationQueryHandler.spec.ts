import {Client} from '@elastic/elasticsearch';
import { mocked } from 'ts-jest/utils';
import {AggregationQueryHandler} from "../../../src/driver/query-handlers/AggregationQueryHandler";

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

describe('AggregationQueryHandler', () => {

    const MockedClient = mocked(Client, true);
    let sut: AggregationQueryHandler;
    let client: Client;

    beforeEach(async () => {
        MockedClient.mockClear();
        client = new Client({});
        sut = new AggregationQueryHandler(client, 'json');
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    it('should only support json and jdbc(as default)', async () => {
        expect(() => { new AggregationQueryHandler(client, 'json')}).not.toThrow();
        expect(() => { new AggregationQueryHandler(client, 'jdbc')}).not.toThrow();
        expect(() => { new AggregationQueryHandler(client, 'incorrect')}).toThrow();
    });

    it('should query index', async () => {
        jest.spyOn(client.sql, 'query').mockReturnValue({ body: { aggregations: {}} } as any);
        await sut.query('test', []);
        expect(client.sql.query).toHaveBeenCalledTimes(1);
    });

    it('should traverse aggregations', async () => {

        const result = {
            "took" : 0,
            "timed_out" : false,
            "_shards" : {
                "total" : 1,
                "successful" : 1,
                "skipped" : 0,
                "failed" : 0
            },
            "hits" : {
                "total" : {
                    "value" : 2000,
                    "relation" : "eq"
                },
                "max_score" : null,
                "hits" : [ ]
            },
            "aggregations" : {
                "count" : {
                    "value" : 2000
                }
            }
        }

        jest.spyOn(client.sql, 'query').mockReturnValue({ body: result } as any);
        const actual = await sut.query('test', []);

        expect(actual).toStrictEqual([{"count": 2000}]);
    });
});
