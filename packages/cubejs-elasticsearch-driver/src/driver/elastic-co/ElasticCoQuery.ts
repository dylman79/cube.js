import {BaseQuery} from '@cubejs-backend/schema-compiler';
import R from 'ramda';
import {ElasticCoQueryFilter} from "./ElasticCoQueryFilter";

const GRANULARITY_TO_INTERVAL = {
    day: (date: any): string => `DATE_TRUNC('day', ${date}::datetime)`,
    week: (date: any): string => `DATE_TRUNC('week', ${date}::datetime)`,
    hour: (date: any): string => `DATE_TRUNC('hour', ${date}::datetime)`,
    minute: (date: any): string => `DATE_TRUNC('minute', ${date}::datetime)`,
    second: (date: any): string => `DATE_TRUNC('second', ${date}::datetime)`,
    month: (date: any): string => `DATE_TRUNC('month', ${date}::datetime)`,
    year: (date: any): string => `DATE_TRUNC('year', ${date}::datetime)`
};

export class ElasticCoQuery extends BaseQuery {
    newFilter(filter: any) {
        return new ElasticCoQueryFilter(this, filter);
    }

    convertTz(field: any) {
        return `${field}`; // TODO
    }

    timeStampCast(value: any) {
        return `${value}`;
    }

    dateTimeCast(value: any) {
        return `${value}`; // TODO
    }

    subtractInterval(date: any, interval: any) {
        // TODO: Test this, note sure how value gets populated here
        return `${date} - INTERVAL ${interval}`;
    }

    addInterval(date: any, interval: any) {
        // TODO: Test this, note sure how value gets populated here
        return `${date} + INTERVAL ${interval}`;
    }

    timeGroupedColumn(granularity: any, dimension: any): string {
        // @ts-ignore
        return GRANULARITY_TO_INTERVAL[granularity](dimension);
    }

    unixTimestampSql() {
        return `TIMESTAMP_DIFF('seconds', '1970-01-01T00:00:00.000Z'::datetime, CURRENT_TIMESTAMP())`;
    }

    groupByClause() {
        if (this.ungrouped) {
            return '';
        }
        const dimensionsForSelect = this.dimensionsForSelect();
        const dimensionColumns = R.flatten(
            dimensionsForSelect.map((s: any) => s.selectColumns() && s.dimensionSql())
        ).filter(s => !!s);

        return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
    }

    orderHashToString(hash: any) {
        if (!hash || !hash.id) {
            return null;
        }

        const fieldAlias = this.getFieldAlias(hash.id);

        if (fieldAlias === null) {
            return null;
        }

        const direction = hash.desc ? 'DESC' : 'ASC';
        return `${fieldAlias} ${direction}`;
    }

    getFieldAlias(id: any) {
        const equalIgnoreCase = (a: any, b: any) => typeof a === 'string' &&
            typeof b === 'string' &&
            a.toUpperCase() === b.toUpperCase();

        let field;

        field = this.dimensionsForSelect().find((d: any) => equalIgnoreCase(d.dimension, id));

        if (field) {
            return field.dimensionSql();
        }

        field = this.measures.find(
            (d: any) => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id)
        );

        if (field) {
            return field.aliasName(); // TODO isn't supported
        }

        return null;
    }

    escapeColumnName(name: any) {
        return `${name}`; // TODO
    }
}

