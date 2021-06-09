import R from 'ramda';
import {BaseQuery} from "@cubejs-backend/schema-compiler";
import {OpenDistroQueryFilter} from "./OpenDistroQueryFilter";

const GRANULARITY_TO_INTERVAL = {
    day: (date: any): string => `DATE_FORMAT(${date}, 'yyyy-MM-dd 00:00:00.000')`,
    week: (date: any): string => `DATE_FORMAT(DATE_SUB(${date}, INTERVAL CAST(DATE_FORMAT(${date}, '%w') AS INTEGER) - 1 day), 'yyyy-MM-dd HH:00:00.000')`,
    hour: (date: any): string => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:00:00.000')`,
    minute: (date: any): string => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:00.000')`,
    second: (date: any): string => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:ss.000')`,
    month: (date: any): string => `DATE_FORMAT(${date}, 'yyyy-MM-01 00:00:00.000')`,
    year: (date: any): string => `DATE_FORMAT(${date}, 'yyyy-01-01 00:00:00.000')`
};

export class OpenDistroQuery extends BaseQuery {
    newFilter(filter: any) {
        return new OpenDistroQueryFilter(this, filter);
    }

    convertTz(field: any) {
        return `${field}`; // TODO - not currently supported
    }

    timeStampCast(value: any) {
        return `${value}`;
    }

    dateTimeCast(value: any) {
        return `${value}`; // TODO
    }

    subtractInterval(date: any, interval: any) {
        return `DATE_SUB(${date}, INTERVAL ${interval})`;
    }

    addInterval(date: any, interval: any) {
        return `DATE_ADD(${date}, INTERVAL ${interval})`;
    }

    timeGroupedColumn(granularity: any, dimension: any): string {
        // @ts-ignore
        return GRANULARITY_TO_INTERVAL[granularity](dimension);
    }

    groupByClause() {
        if (this.ungrouped) {
            return '';
        }
        const dimensionsForSelect = this.dimensionsForSelect();
        const dimensionColumns = R.flatten(dimensionsForSelect.map((s: any) => s.selectColumns() && s.dimensionSql()))
            .filter(s => !!s);
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
        const equalIgnoreCase = (a: any, b: any) => (
            typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
        );

        let field = this.dimensionsForSelect().find((d: any) => equalIgnoreCase(d.dimension, id));
        if (field) {
            return field.dimensionSql();
        }

        field = this.measures.find((d: any) => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id));

        if (field) {
            return field.aliasName(); // TODO isn't supported
        }

        return null;
    }

    escapeColumnName(name: any) {
        return `${name}`; // TODO
    }
}
