import {BaseFilter} from "@cubejs-backend/schema-compiler";

export class ElasticCoQueryFilter extends BaseFilter {
    likeIgnoreCase(column: string, not: boolean, param: any) {
        return `${not ? ' NOT' : ''} MATCH(${column}, ${this.allocateParam(param)}, 'fuzziness=AUTO:1,5')`;
    }
}
