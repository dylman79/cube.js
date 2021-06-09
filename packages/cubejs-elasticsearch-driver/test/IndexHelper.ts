import * as http from "http";
import {RequestOptions} from "http";


export class IndexHelper {

    get index() {
        return this.indexName;
    }

    constructor(private indexName: string, private host: string, private port: number) {
    }

    async createIndex() {
        console.log('Create Index', this.indexName);
        await this.request({ method: 'PUT' })
    }

    async addDocument(id: number, body: any) {
        console.log('Adding Document', id);
        await this.request({
            path: `${this.indexName}/_doc/${id}`,
            method: 'PUT'
        }, body);
        // Create a delay to allow reindex
        await new Promise((resolve) => {
            setTimeout(() => {
                resolve(true);
            }, 1500);
        });
    }

    async removeDocument(id: number) {
        console.log('Remove Document', id);
        await this.request({
            path: `${this.indexName}/_doc/${id}`,
            method: 'DELETE'
        })
    }

    async deleteIndex() {
        console.log('Delete index', this.indexName);
        await this.request({ method: 'DELETE' })
    }

    private request(options: RequestOptions, body: any = null) {
        const defaultOptions = {
            host: this.host,
            port: this.port,
            path: `${this.indexName}`
        }

        options = {...defaultOptions, ...options };

        if (body) {
            options = {...{ headers: { 'Content-Type': 'application/json' }}, ...options };
        }

        // console.log(options);
        return new Promise((resolve, reject) => {
            let request = http.request(options, res => {
                if (!res || !res.statusCode || res.statusCode >= 300) {
                    reject(new Error(res.statusMessage));
                }
                const chunks: any[] = [];
                res.on('data', chunk => {
                    chunks.push(chunk);
                });
                res.on('end', () => {
                    const result = Buffer.concat(chunks).toString();
                    // console.log(result);
                    resolve(JSON.parse(result));
                });
            });

            request.on('error', e => {
                reject(e);
            });

            if (body) {
                request.write(JSON.stringify(body));
            }
            request.end();
        });
    }
}
