import {ElasticSearchDriver} from "../ElasticSearchDriver";
import {ElasticSearchDriverOptions} from "../ElasticSearchDriverOptions";
import {OpenDistroQuery} from "./OpenDistroQuery";

export class OpenDistroDriver extends ElasticSearchDriver {

    constructor(config: ElasticSearchDriverOptions) {
        super(config);
    }

    // TODO - does this need to be static - cant use config for discriminator
    static dialectClass() {
        return OpenDistroQuery;
    }
}
