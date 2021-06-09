/* globals describe, afterAll, beforeAll, test, expect, jest, it */
import {ElasticsearchContainer, StartedElasticsearchContainer, Wait} from "testcontainers";
import {ElasticCoDriver, ElasticSearchDriverOptions, OpenDistroDriver} from "../src";
import {IndexHelper} from "./IndexHelper";

describe('ElasticSearchDriver For Elastic.co', () => {
  let container: StartedElasticsearchContainer;

  jest.setTimeout(60000);

  const version = process.env.TEST_ELASTIC_ELASTICCO_VERSION || '7.10.2';

  const startContainer = () => new ElasticsearchContainer(`docker.elastic.co/elasticsearch/elasticsearch:${version}`)
    .withEnv('discovery.type', 'single-node')
    .withEnv('bootstrap.memory_lock', 'true')
    .withEnv('ES_JAVA_OPTS', '-Xms512m -Xmx512m')
    .withExposedPorts(9200)
    // .withHealthCheck({
    //   test: 'curl -k -u admin:admin --silent --fail http://localhost:9200/_cluster/health || exit 1',
    //   interval: 3 * 1000,
    //   startPeriod: 15 * 1000,
    //   timeout: 5000,
    //   retries: 3
    // })
    .withWaitStrategy(Wait.forLogMessage('Active license is now'))
    .start();

  const createDriver = (c: StartedElasticsearchContainer, format: string = 'json') => {
    const port = c && c.getMappedPort(9200) || 9200;

    let config = {
      url: `http://localhost:${port}`,
      ssl: {
        rejectUnauthorized: false,
      },
      auth: {
        username: 'admin',
        password: 'admin',
      },
      openDistro: false,
      queryFormat: format
    } as ElasticSearchDriverOptions;

    return new ElasticCoDriver(config);
  };

  beforeAll(async () => {
    container = await startContainer();
  });

  describe('json query format',  () => {
    let elasticSearchDriver: OpenDistroDriver;
    let indexHelper: IndexHelper;

    beforeAll(async () => {
      elasticSearchDriver = createDriver(container);
      elasticSearchDriver.setLogger((msg: string, event: string) => console.log(`${msg}: ${JSON.stringify(event)}`));
      indexHelper = new IndexHelper('jsontest','localhost', container.getMappedPort(9200))
      await indexHelper.createIndex();
    });

    it('should connect', async () => {
      await elasticSearchDriver.testConnection();
    });

    it('should handle a simple select using WellFormattedQueryHandler',  async () => {
      const expected = { id: 1, name: 'test', date: new Date().toISOString()};
      await indexHelper.addDocument(1, expected);
      const actual = await elasticSearchDriver.query(`SELECT * FROM ${indexHelper.index}`);
      expect(actual.length).toBe(1);
      expect(actual[0]).toStrictEqual(expected);
      await indexHelper.removeDocument(1);
    });

    it('should handle a simple aggregation using the WellFormattedQueryHandler',  async () => {
      const doc1 = { id: 1, name: 'test', date: new Date().toISOString()};
      const doc2 = { id: 2, name: 'test', date: new Date().toISOString()};
      await indexHelper.addDocument(1, doc1);
      await indexHelper.addDocument(2, doc2);
      const actual = await elasticSearchDriver.query(`SELECT name, count(id) FROM ${indexHelper.index} GROUP BY name`);
      expect(actual.length).toBe(1);
      expect(actual[0]).toStrictEqual({'count(id)': 2, 'name': 'test'});
      await indexHelper.removeDocument(1);
      await indexHelper.removeDocument(2);
    });

    afterAll(async () => {
      await indexHelper.deleteIndex();
      await elasticSearchDriver.release();
    });
  });

  describe('jdbc query format',  () => {
    let elasticSearchDriver: OpenDistroDriver;
    let indexHelper: IndexHelper;

    beforeAll(async () => {
      elasticSearchDriver = createDriver(container, 'jdbc');
      elasticSearchDriver.setLogger((msg: string, event: string) => console.log(`${msg}: ${JSON.stringify(event)}`));
      indexHelper = new IndexHelper('jdbctest','localhost', container.getMappedPort(9200))
      await indexHelper.createIndex();
    });

    it('should connect', async () => {
      await elasticSearchDriver.testConnection();
    });

    it('should throw - not applicable for elastic.co',  async () => {
      const expected = { id: 1, name: 'test', date: new Date().toISOString()};
      await indexHelper.addDocument(1, expected);
      return expect(elasticSearchDriver.query(`SELECT * FROM ${indexHelper.index}`))
          .rejects
          .toThrow('Query format not supported for elastic.co: jdbc');
    });

    afterAll(async () => {
      await indexHelper.deleteIndex();
      await elasticSearchDriver.release();
    });
  });

  afterAll(async () => {
    if (container) {
      console.log('[container] Stopping');

      await container.stop();

      console.log('[container] Stopped');
    }
  });
});
