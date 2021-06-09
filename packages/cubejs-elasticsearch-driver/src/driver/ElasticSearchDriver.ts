import { BaseDriver } from "@cubejs-backend/query-orchestrator";
import {ElasticSearchDriverOptions} from "./ElasticSearchDriverOptions";
import {ElasticSearchQueryFormat} from "./ElasticSearchQueryFormat";
import {Client} from "@elastic/elasticsearch";
import {ApiKeyAuth, BasicAuth} from "@elastic/elasticsearch/lib/pool";
import {ElasticSearchQueryHandler} from "./ElasticSearchQueryHandler";
import {WellFormattedQueryHandler} from "./query-handlers/WellFormattedQueryHandler";
import {AggregationQueryHandler} from "./query-handlers/AggregationQueryHandler";

export abstract class ElasticSearchDriver extends BaseDriver {

  private config?: ElasticSearchDriverOptions;
  private readonly client: Client;
  private readonly sqlClient: Client;

  static driverEnvVariables() {
    return [
      'CUBEJS_DB_URL',
      'CUBEJS_DB_ELASTIC_QUERY_FORMAT',
      'CUBEJS_DB_ELASTIC_OPENDISTRO',
      'CUBEJS_DB_ELASTIC_APIKEY_ID',
      'CUBEJS_DB_ELASTIC_APIKEY_KEY',
    ];
  }

  protected get cloud() {
    return this.config?.cloud;
  }

  protected get url() : string | undefined {
    return this.config?.url || process.env.CUBEJS_DB_URL;
  }

  protected get ssl() : any {
    return this.getSslOptions();
  }

  protected get auth() : BasicAuth | ApiKeyAuth | undefined {

    let username = process.env.CUBEJS_DB_USER;
    let password = process.env.CUBEJS_DB_PASS;
    if (this.config?.auth && 'username' in this.config?.auth) {
      username = this.config.auth.username;
    }
    if (this.config?.auth && 'password' in this.config?.auth) {
      password = this.config.auth.password;
    }

    if (username && password) {
      return {
        username,
        password
      }
    }

    let apiKeyId = process.env.CUBEJS_DB_ELASTIC_APIKEY_ID;
    let apiKey = process.env.CUBEJS_DB_ELASTIC_APIKEY_KEY;

    if (this.config?.auth && 'apiKey' in this.config?.auth) {
      if (typeof this.config.auth.apiKey === 'string') {
        return {
          apiKey: this.config.auth.apiKey
        }
      }

      apiKeyId = this.config.auth.apiKey.id;
      apiKey = this.config.auth.apiKey.api_key;
    }

    if (apiKeyId && apiKey) {
      return {
        apiKey: {
          id: apiKeyId,
          api_key: apiKey
        }
      };
    }

    if (apiKey) {
      return {
        apiKey: apiKey
      }
    }

    return undefined;
  }

  protected get openDistro(): boolean {
    return (this.config?.openDistro === true) || ((process.env.CUBEJS_DB_ELASTIC_OPENDISTRO || 'false').toLowerCase() === 'true' ||
        process.env.CUBEJS_DB_TYPE === 'odelasticsearch');
  }

  protected get queryFormat(): ElasticSearchQueryFormat {
    return this.config?.queryFormat || process.env.CUBEJS_DB_ELASTIC_QUERY_FORMAT || 'jdbc';
  }

  protected constructor(config?: ElasticSearchDriverOptions) {
    super();
    this.config = config;

    this.client = new Client({
      node: this.url,
      cloud: this.cloud,
      auth: this.auth,
      ssl: this.ssl
    });

    this.sqlClient = !this.openDistro ?
        this.client :
        new Client({
          node: this.openDistro ? `${this.url}/_opendistro` : this.url,
          cloud: this.cloud,
          auth: this.auth,
          ssl: this.ssl
        });
  }

  async testConnection(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.client.cat.indices({
        format: 'json'
      }).then(() => resolve())
          .catch((e) => reject(e))
    });
  }

  async tablesSchema() {
    const indices = await this.client.cat.indices({
      format: 'json'
    });

    const schema = (await Promise.all(indices.body.map(async (i: any) => {
      const props = (await this.client.indices.getMapping({ index: i.index })).body[i.index].mappings.properties || {};
      return {
        [i.index]: Object.keys(props).map(p => ({ name: p, type: props[p].type })).filter(c => !!c.type)
      };
    }))).reduce((a: any, b: any) => ({ ...a, ...b }));

    return {
      main: schema
    };
  }

  async query(query: string, values?: Array<unknown>): Promise<Array<any>> {
    try {
      return await this.getQueryHandler().query(query, values);
    } catch (e) {
      if (e.body) {
        throw new Error(JSON.stringify(e.body, null, 2));
      }
      throw e;
    }
  };

  async release() {
    await this.client.close();

    if (this.sqlClient != this.client) {
      await this.sqlClient.close();
    }
  }

  protected getQueryHandler(): ElasticSearchQueryHandler {
    if (this.openDistro) {
      if (this.queryFormat === 'json') {
        return new AggregationQueryHandler(this.sqlClient, this.queryFormat);
      }
      if (!this.queryFormat || this.queryFormat === 'jdbc') {
        return new WellFormattedQueryHandler(this.sqlClient, this.queryFormat, 'datarows', 'schema');
      }
      throw new Error(`Query format not supported for open distro: ${this.queryFormat}`);
    }

    if (!this.queryFormat || this.queryFormat === 'json') {
      return new WellFormattedQueryHandler(this.sqlClient, this.queryFormat);
    }

    throw new Error(`Query format not supported for elastic.co: ${this.queryFormat}`);
  }
}
