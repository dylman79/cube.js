import {ElasticSearchQueryHandler} from "../ElasticSearchQueryHandler";
import * as SqlString from "sqlstring";
import {Client} from "@elastic/elasticsearch";
import {ElasticSearchQueryFormat} from "../ElasticSearchQueryFormat";

export class AggregationQueryHandler implements ElasticSearchQueryHandler {

    constructor(private client: Client, private format: ElasticSearchQueryFormat, private rowField: string = 'rows', private columnField: string = 'columns') {
        // none (defaults to jdbc) for elastic co and json for open distro
        if (!['json', 'jdbc'].includes(this.format)) {
            throw new Error('Unsupported format');
        }
    }

    async query(query: string, values?: Array<unknown>): Promise<Array<any>> {
        // TODO - handle scroll / paged queries if required
        const response = await this.client.sql.query({
            format: this.format,
            body: {
                query: SqlString.format(query, values)
            }
        });

        if (!response?.body?.aggregations) {
            throw new Error('Invalid Response');
        }

        return this.traverseAggregations(response.body.aggregations);
    }

    private traverseAggregations(aggregations: any) {
        const fields = Object.keys(aggregations).filter((k: any) => k !== 'key' && k !== 'doc_count');
        if (fields.find(f => aggregations[f].hasOwnProperty('value'))) {
            return [fields.map(f => ({ [f]: aggregations[f].value })).reduce((a: any, b: any) => ({ ...a, ...b }))];
        }
        if (fields.length === 0) {
            return [{}];
        }
        if (fields.length !== 1) {
            throw new Error(`Unexpected multiple fields at ${fields.join(', ')}`);
        }
        const dimension = fields[0];
        if (!aggregations[dimension].buckets) {
            throw new Error(`Expecting buckets at dimension ${dimension}: ${aggregations[dimension]}`);
        }
        return aggregations[dimension].buckets.map((b: any) => this.traverseAggregations(b).map(
            (innerRow: any) => ({ ...innerRow, [dimension]: b.key })
        )).reduce((a: any, b: any) => a.concat(b), []);
    }
}
