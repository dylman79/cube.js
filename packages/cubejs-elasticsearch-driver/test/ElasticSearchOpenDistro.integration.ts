/* globals describe, afterAll, beforeAll, test, expect, jest, it */
import {
  ElasticsearchContainer, GenericContainer,
  GenericContainerBuilder,
  StartedElasticsearchContainer,
  StartedTestContainer,
  Wait
} from "testcontainers";
import {ElasticSearchDriverOptions, OpenDistroDriver} from "../src";
import {IndexHelper} from "./IndexHelper";

describe('ElasticSearchDriver For Open Distro', () => {
  let container: StartedTestContainer;

  jest.setTimeout(60000);

  const startContainer = async () => {
    let genericContainer = await GenericContainer.fromDockerfile('.', 'Dockerfile').build();
    return genericContainer
        .withEnv('discovery.type', 'single-node')
        .withEnv('bootstrap.memory_lock', 'true')
        .withEnv('ES_JAVA_OPTS', '-Xms512m -Xmx512m')
        .withExposedPorts(9200)
        // .withHealthCheck({
        //   test: 'curl -k -u admin:admin --silent --fail http://localhost:9200/_cluster/health || exit 1',
        //   interval: 5 * 1000,
        //   startPeriod: 15 * 1000,
        //   timeout: 500,
        //   retries: 3
        // })
        .withWaitStrategy(Wait.forLogMessage('Active license is now'))
        .start();
  }

  const createDriver = (c: StartedTestContainer, format: string = 'json') => {
    const port = c && c.getMappedPort(9200) || 9200;

    let config: ElasticSearchDriverOptions = {
      url: `http://localhost:${port}`,
      ssl: {
        rejectUnauthorized: false,
      },
      auth: {
        username: 'admin',
        password: 'admin',
      },
      openDistro: true,
      queryFormat: format
    } as ElasticSearchDriverOptions;

    return new OpenDistroDriver(config);
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

    it('should handle a simple select using AggregationQueryHandler',  async () => {
      const expected = { id: 1, name: 'test', date: new Date().toISOString()};
      await indexHelper.addDocument(1, expected);
      const actual = await elasticSearchDriver.query(`SELECT * FROM ${indexHelper.index}`);
      expect(actual.length).toBe(1);
      expect(actual[0]).toStrictEqual(expected);
      await indexHelper.removeDocument(1);
    });

    it('should handle a simple aggregation using the AggregationQueryHandler',  async () => {
      const doc1 = { id: 1, name: 'test', date: new Date().toISOString()};
      const doc2 = { id: 2, name: 'test', date: new Date().toISOString()};
      await indexHelper.addDocument(1, doc1);
      await indexHelper.addDocument(2, doc2);
      const actual = await elasticSearchDriver.query(`SELECT name, count(id) FROM ${indexHelper.index} GROUP BY name`);
      expect(actual.length).toBe(1);
      expect(actual[0]).toStrictEqual({ 'COUNT(id)': 2, 'name.keyword': 'test'});
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

    it('should handle a simple select using the WellFormattedQueryHandler',  async () => {
      const expected = { id: 1, name: 'test', date: new Date().toISOString()};
      await indexHelper.addDocument(1, expected);
      const actual = await elasticSearchDriver.query(`SELECT * FROM ${indexHelper.index}`);
      expect(actual.length).toBe(1);
      expect(actual[0].id).toBe(expected.id);
      expect(actual[0].name).toBe(expected.name);
      // Open distro returns dates as 'yyyy-MM-dd hh:mm:ss[.fraction]' so create a new UTC date
      expect(new Date(actual[0].date + 'UTC').toISOString()).toStrictEqual(expected.date);
      await indexHelper.removeDocument(1);
    });

    it('should handle a simple aggregation using the WellFormattedQueryHandler',  async () => {
      const doc1 = { id: 1, name: 'test', date: new Date().toISOString()};
      const doc2 = { id: 2, name: 'test', date: new Date().toISOString()};
      await indexHelper.addDocument(1, doc1);
      await indexHelper.addDocument(2, doc2);
      const actual = await elasticSearchDriver.query(`SELECT name, count(id) FROM ${indexHelper.index} GROUP BY name`);
      expect(actual.length).toBe(1);
      expect(actual[0]).toStrictEqual({'count(id)': 2, name: 'test'});
      await indexHelper.removeDocument(1);
      await indexHelper.removeDocument(2);
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
