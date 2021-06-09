import { ClientOptions } from '@elastic/elasticsearch';

export type ElasticSearchDriverOptions = Pick<ClientOptions, 'ssl' | 'auth' | 'cloud'> & {
    url?: string;
    queryFormat?: string;
    openDistro?: boolean;
};
