
import * as mongodb from "mongodb";
import { Hub } from "@belongs/asyncutil";
import * as url from "url";

export type Fields<T> = {
    [P in keyof T]: 1;
}

export type PartialFields<T> = Partial<Fields<T>>;

export class CollClient<T> {
    private readonly _colhub: Hub<mongodb.Collection>;
    private readonly _fields: Fields<T>;

    public async getOne(filter: Object, fields?: PartialFields<T>): Promise<T> {
        const col = await this._colhub.get();
        return new Promise<T>((res, rej) => {
            col.findOne(filter, { projection: fields || this._fields }, (err: Error, doc: T) => {
                err ? rej(err) : res(doc);
            });
        });
    }

    public async getAll(filter: Object, fields?: PartialFields<T>): Promise<Array<T>> {
        const col = await this._colhub.get();
        return new Promise<Array<T>>((res, rej) => {
            const cursor = col.find(filter, { projection: fields || this._fields });
            cursor.toArray((err: Error, docs: Array<T>) => {
                err ? rej(err) : res(docs);
            });
        });
    }

    public async getMul(filter: Object, fields: PartialFields<T>, orderby: Object, skip: number, take: number): Promise<Array<T>> {
        const col = await this._colhub.get();
        return new Promise<Array<T>>((res, rej) => {
            let cursor = col.find(filter, { projection: fields || this._fields });
            if (orderby) {
                cursor = cursor.sort(orderby);
            }
            cursor.limit(take + skip).skip(skip).limit(take).toArray((err: Error, docs: Array<T>) => {
                err ? rej(err) : res(docs);
            });
        });
    }

    public async count(filter: Object = {}): Promise<number> {
        const col = await this._colhub.get();
        return new Promise<number>((res, rej) => {
            col.countDocuments(filter, (err: Error, ct: number) => {
                err ? rej(err) : res(ct);
            });
        });
    }

    public async updateAll(filter: Object, update: Object, upsert?: boolean): Promise<number> {
        const col = await this._colhub.get();
        return new Promise<number>((res, rej) => {
            col.updateMany(filter, update, { upsert: upsert || false, arrayFilters: undefined }, (err: Error, ret: mongodb.UpdateWriteOpResult) => {
                err ? rej(err) : res(ret.modifiedCount);
            });
        });
    }

    public async bulkUpdate(arr: Array<{ filter: Object, update: Object }>, upsert: boolean): Promise<void> {
        const col = await this._colhub.get();
        return new Promise<void>((res, rej) => {
            const bulk = col.initializeUnorderedBulkOp();
            for (const item of arr) {
                if (upsert) {
                    bulk.find(item.filter).upsert().update(item.update);
                }
                else {
                    bulk.find(item.filter).update(item.update);
                }
            }
            bulk.execute((err: Error, ret: mongodb.BulkWriteResult) => {
                err ? rej(err) : res();
            });
        });
    }

    public async createOne(item: T): Promise<mongodb.ObjectID> {
        const col = await this._colhub.get();
        return new Promise<mongodb.ObjectID>((res, rej) => {
            col.insertOne(item, (err: Error, ret: mongodb.InsertOneWriteOpResult) => {
                err ? rej(err) : res(ret.insertedId);
            });
        });
    }

    public async bulkInsert(arr: Array<T>): Promise<Array<mongodb.ObjectID>> {
        const col = await this._colhub.get();
        const bulk = col.initializeUnorderedBulkOp();
        for (const item of arr) {
            bulk.insert(item);
        }
        return new Promise<Array<mongodb.ObjectID>>((res, rej) => {
            bulk.execute((err: Error, ret: mongodb.BulkWriteResult) => {
                if (err) {
                    rej(err);
                } else {
                    res((<Array<{ _id: mongodb.ObjectID }>>ret.getInsertedIds()).map(x => x._id));
                }
            });
        });
    }

    public async findAndModify(filter: Object, update: Object, options?: {
        fields?: PartialFields<T>;
        returnnew?: boolean;
        upsert?: boolean;
        sort?: Object;
    }): Promise<T> {
        options = options || {};
        const col = await this._colhub.get();
        return new Promise<T>((res, rej) => {
            col.findOneAndUpdate(filter, update, {
                sort: options.sort,
                returnOriginal: !options.returnnew,
                upsert: options.upsert,
                projection: options.fields || this._fields
            }, (err: Error, ret: mongodb.FindAndModifyWriteOpResultObject) => {
                err ? rej(err) : res(ret.value);
            });
        });
    }

    constructor(dbhub: Hub<mongodb.Db>, collname: string, fields: Fields<T>) {
        this._colhub = new Hub<mongodb.Collection>(() => dbhub.get().then(db => db.collection(collname)));
        this._fields = fields;
    }
}

export class DbClient {
    private readonly _dbhub: Hub<mongodb.Db>;

    public getCollClient<T>(collname: string, fields: Fields<T>): CollClient<T> {
        return new CollClient<T>(this._dbhub, collname, fields);
    }

    constructor(connstr: string) {
        this._dbhub = new Hub<mongodb.Db>(async () => {
            const client = await mongodb.connect(connstr, { useNewUrlParser: true });
            return client.db(dbNameFromUrl(connstr));
        })
    }
}

function dbNameFromUrl(connstr: string): string {
    const pathname = url.parse(connstr).pathname;
    return pathname[0] === "/" ? pathname.slice(1) : pathname;
}
