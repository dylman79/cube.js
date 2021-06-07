
export type ElasticCoSearchQueryFormat = string | 'json';
export type OpenDistroSearchQueryFormat = string | 'jdbc' | 'json';
export type ElasticSearchQueryFormat = ElasticCoSearchQueryFormat | OpenDistroSearchQueryFormat;
