export interface ElasticSearchQueryHandler {
    query(query: string, values?: Array<unknown>): Promise<Array<any>>;
}
