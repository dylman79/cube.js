/* globals describe, afterAll, beforeAll, test, expect, jest, it */
import {ElasticsearchContainer, StartedElasticsearchContainer, Wait} from "testcontainers";
import {ElasticSearchDriverOptions, OpenDistroDriver} from "../src";

describe('ElasticSearchDriver Open Distro', () => {
  let container: StartedElasticsearchContainer;
  let elasticSearchDriver: OpenDistroDriver;

  jest.setTimeout(50000);

  const version = process.env.TEST_ELASTIC_ELASTICCO_VERSION || '7.5.2';

  const startContainer = () => new ElasticsearchContainer(`docker.elastic.co/elasticsearch/elasticsearch:${version}`)
      .withEnv('discovery.type', 'single-node')
      .withEnv('bootstrap.memory_lock', 'true')
      .withEnv('ES_JAVA_OPTS', '-Xms512m -Xmx512m')
      .withExposedPorts(9200)
      .withHealthCheck({
        test: 'curl -k -u admin:admin --silent --fail https://localhost:9200/_cluster/health || exit 1',
        interval: 3 * 1000,
        startPeriod: 15 * 1000,
        timeout: 500,
        retries: 3
      })
      .withWaitStrategy(Wait.forHealthCheck())
      .start();

  const createDriver = (c: StartedElasticsearchContainer, format: string = 'json') => {
    const port = c && c.getMappedPort(9200) || 9200;

    let config: ElasticSearchDriverOptions = {
      url: `https://localhost:${port}`,
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
    elasticSearchDriver = createDriver(container);
    elasticSearchDriver.setLogger((msg: string, event: string) => console.log(`${msg}: ${JSON.stringify(event)}`));
  });

  it('testConnection', async () => {
    await elasticSearchDriver.testConnection();
  });

  // It's not supported in Open Distro, probably it's supported in v2 Query Engine for Open Distro
  // it('SELECT 1', async () => {
  //   await elasticSearchDriver.query('SELECT 1');
  // });

  afterAll(async () => {
    await elasticSearchDriver.release();

    if (container) {
      console.log('[container] Stopping');

      await container.stop();

      console.log('[container] Stopped');
    }
  });
});
