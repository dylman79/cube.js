
export type ElasticCoSearchQueryFormat = 'json';
export type OpenDistroSearchQueryFormat = 'jdbc' | 'json';
export type ElasticSearchQueryFormat = string | ElasticCoSearchQueryFormat | OpenDistroSearchQueryFormat;
