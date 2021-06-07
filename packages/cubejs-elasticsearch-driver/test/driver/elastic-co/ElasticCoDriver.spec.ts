import {Client} from '@elastic/elasticsearch';
import {mocked} from 'ts-jest/utils';
import {ElasticCoDriver, ElasticCoQuery} from "../../../src";
import {WellFormattedQueryHandler} from "../../../src/driver/query-handlers/WellFormattedQueryHandler";

jest.mock('@elastic/elasticsearch', () => {
    return {
        Client: jest.fn().mockImplementation(() => {
            return {
                sql: {
                    query: () => {return "test" },
                },
                cat: {
                    indices: () => {return "test" },
                },
                indices: {
                    getMapping: () => {return "test"}
                },
                close: () => {}
            };
        })
    };
});

describe('ElasticCoDriver', () => {
    const MockedClient = mocked(Client, true);
    let sut: ElasticCoDriver;
    let client: Client;
    let sqlClient: Client;

    beforeEach(() => {
        MockedClient.mockClear();

        sut = new ElasticCoDriver({
            url: 'testing - url',
            queryFormat: 'json',
            openDistro: false
        });
        // @ts-ignore
        client = sut.client;
        // @ts-ignore
        sqlClient = sut.sqlClient;
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    it('should return driver envs', async () => {
        expect(ElasticCoDriver.driverEnvVariables()).toStrictEqual([
            'CUBEJS_DB_URL',
            'CUBEJS_DB_ELASTIC_QUERY_FORMAT',
            'CUBEJS_DB_ELASTIC_OPENDISTRO',
            'CUBEJS_DB_ELASTIC_APIKEY_ID',
            'CUBEJS_DB_ELASTIC_APIKEY_KEY',
        ]);
    });

    it('should return elastic co query as dialect', async () => {
        expect(ElasticCoDriver.dialectClass()).toBe(ElasticCoQuery);
    });

    it('should set sql client to the same es client', () => {
        expect(client).toBe(sqlClient);
    });

    it('should call cat indices to check connection', async () => {
        jest.spyOn(client.cat, 'indices').mockReturnValue(Promise.resolve({}) as any);
        await sut.testConnection()
        expect(client.cat.indices).toHaveBeenCalledTimes(1);
    });

    it('should call indices - get mapping to get schema data', async () => {
        const index = [
            {
                "health" : "green",
                "status" : "open",
                "index" : "test",
                "uuid" : "8SSKUMuvQkONdGfEIWloAA",
                "pri" : "1",
                "rep" : "0",
                "docs.count" : "74",
                "docs.deleted" : "30",
                "store.size" : "277.1kb",
                "pri.store.size" : "277.1kb"
            }
        ];

        const mapping = {
            "test" : {
                "mappings" : {
                    "properties" : {
                        "doc" : {
                            "properties" : {
                                "testing" : {
                                    "type" : "text",
                                    "fields" : {
                                        "keyword" : {
                                            "type" : "keyword",
                                            "ignore_above" : 256
                                        }
                                    }
                                },
                            }
                        },
                        "query" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                    "type" : "keyword",
                                    "ignore_above" : 256
                                }
                            }
                        }
                    }
                }
            }
        }

        jest.spyOn(client.cat, 'indices').mockReturnValue({ body: index } as any);
        jest.spyOn(client.indices, 'getMapping').mockReturnValue({ body: mapping } as any);
        await sut.tablesSchema();
        expect(client.cat.indices).toHaveBeenCalledTimes(1);
        expect(client.indices.getMapping).toHaveBeenCalledTimes(1);
    });

    it('should get AggregationQueryHandler', () => {
        // Calling protected members for testing only
        // @ts-ignore
        sut.config.queryFormat = 'json';
        // @ts-ignore
        sut.config.openDistro = false;
        // @ts-ignore
        const handler = sut.getQueryHandler();
        expect(handler instanceof WellFormattedQueryHandler).toBe(true);
    });

    it('should throw is no query handler with associated settings', () => {
        // Calling protected members for testing only
        // @ts-ignore
        sut.config.queryFormat = 'jdbc';
        // @ts-ignore
        sut.config.openDistro = false;
        // @ts-ignore
        expect(() => { sut.getQueryHandler() }).toThrow();
    });

    it('should call close on client', async () => {
        jest.spyOn(client, 'close');
        jest.spyOn(sqlClient, 'close');
        await sut.release()
        expect(client.close).toHaveBeenCalledTimes(1);
        expect(sqlClient.close).toHaveBeenCalledTimes(1);
    });
});
