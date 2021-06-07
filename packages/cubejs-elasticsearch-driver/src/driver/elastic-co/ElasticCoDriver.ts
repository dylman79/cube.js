import {ElasticSearchDriver} from "../ElasticSearchDriver";
import {ElasticSearchDriverOptions} from "../ElasticSearchDriverOptions";
import {ElasticCoQuery} from "./ElasticCoQuery";

export class ElasticCoDriver extends ElasticSearchDriver {

    constructor(config: ElasticSearchDriverOptions) {
        super(config);
    }

    // TODO - does this need to be static - cant use config for discriminator
    static dialectClass() {
        return ElasticCoQuery;
    }
}
