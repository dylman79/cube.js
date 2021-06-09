import { BaseFilter } from '@cubejs-backend/schema-compiler';

export class OpenDistroQueryFilter extends BaseFilter {
  likeIgnoreCase(column: string, not: boolean, param: any) {
    return ` ${column}${not ? ' NOT' : ''} LIKE '%${this.allocateParam(param)}%')`;
  }
}
