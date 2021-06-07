import {Client} from '@elastic/elasticsearch';
import { mocked } from 'ts-jest/utils';
import {ElasticCoDriver, OpenDistroDriver, OpenDistroQuery} from '../../../src';
import {WellFormattedQueryHandler} from "../../../src/driver/query-handlers/WellFormattedQueryHandler";
import {AggregationQueryHandler} from "../../../src/driver/query-handlers/AggregationQueryHandler";

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

describe('OpenDistroDriver', () => {
    const MockedClient = mocked(Client, true);
    let sut: OpenDistroDriver;
    let client: Client;
    let sqlClient: Client;

    beforeEach(() => {
        MockedClient.mockClear();
        client = new Client();
        sqlClient = new Client();

        sut = new OpenDistroDriver({});
        // @ts-ignore
        sut.client = client;
        // @ts-ignore
        sut.sqlClient = sqlClient;
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

    it('should return open distro query', async () => {
        expect(OpenDistroDriver.dialectClass()).toBe(OpenDistroQuery);
    });

    it('should set sql client to new es client', () => {
        expect(client).not.toBe(sqlClient);
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

        jest.spyOn(client.cat, 'indices').mockReturnValue(Promise.resolve({ body: index }) as any);
        jest.spyOn(client.indices, 'getMapping').mockReturnValue(Promise.resolve({ body: mapping }) as any);
        await sut.tablesSchema();
        expect(client.cat.indices).toHaveBeenCalledTimes(1);
        expect(client.indices.getMapping).toHaveBeenCalledTimes(1);
    });

    it('should get WellFormattedQueryHandler', () => {
        // Calling protected members for testing only
        // @ts-ignore
        sut.config.queryFormat = 'jdbc';
        // @ts-ignore
        sut.config.openDistro = true;
        // @ts-ignore
        const handler = sut.getQueryHandler();
        expect(handler instanceof WellFormattedQueryHandler).toBe(true);
    });

    it('should get AggregationQueryHandler', async () => {
        // Calling protected members for testing only
        // @ts-ignore
        sut.config.queryFormat = 'json';
        // @ts-ignore
        sut.config.openDistro = true;
        // @ts-ignore
        const handler = sut.getQueryHandler();
        expect(handler instanceof AggregationQueryHandler).toBe(true);
    });

    it('should throw is no query handler with associated settings', async () => {
        // Calling protected members for testing only
        // @ts-ignore
        sut.config.queryFormat = 'incorrect';
        // @ts-ignore
        sut.config.openDistro = true;
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
