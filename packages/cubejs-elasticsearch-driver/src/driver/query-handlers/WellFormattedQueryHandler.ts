import {ElasticSearchQueryHandler} from "../ElasticSearchQueryHandler";
import * as SqlString from "sqlstring";
import {Client} from "@elastic/elasticsearch";
import {ElasticSearchQueryFormat} from "../ElasticSearchQueryFormat";

export class WellFormattedQueryHandler implements ElasticSearchQueryHandler {

    constructor(private client: Client, private format: ElasticSearchQueryFormat, private rowField: string = 'rows', private columnField: string = 'columns') {
        // json for elastic co and jdbc for open distro
        if (!['json', 'jdbc'].includes(this.format)) {
            throw new Error('Unsupported format');
        }
    }

    async query(query: string, values?: Array<unknown>): Promise<Array<any>> {
        let cursor = null;
        let result = null;
        do {
            const response = await this.client.sql.query({
                format: this.format,
                body: {
                    query: SqlString.format(query, values)
                }
            });

            if (!response?.body) {
                throw new Error('Invalid Response');
            }
            cursor = response.body.cursor || null;

            result = this.transform(response.body, result);

        } while (!!cursor)

        return result;
    }

    private transform(result: any, previous: any) {

        if (!(this.rowField in result)) {
            throw new Error("Invalid Result Format - Row data missing");
        }

        if (!previous && !(this.columnField in result)) {
            throw new Error("Invalid Result Format - Column data missing");
        }

        if (this.columnField in result) {
            return result[this.rowField].map(
                (r: any) => result[this.columnField].reduce((prev: any, cur: any, idx: number) => ({ ...prev, [cur.alias || cur.name]: r[idx] }), {})
            );
        }

        result[this.rowField].map(
            (r: any) => previous[this.columnField].reduce((prev: any, cur: any, idx: number) => ({ ...prev, [cur.alias || cur.name]: r[idx] }), {})
        );

        return previous;
    }
}
