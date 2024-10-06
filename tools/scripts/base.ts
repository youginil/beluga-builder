import Database from "better-sqlite3";
import cliProgress from "cli-progress";

export type Entry = {
    id: number;
    name: string;
    text: string;
};

export type Token = {
    name: string;
    entries: string[];
};

export interface Resource {
    id: number;
    name: string;
    binary: Uint8Array;
}

type Operation<E> = (db: Database.Database, item: E) => Promise<(keyof E)[]>;

export type EntryOperation = Operation<Entry>;
export type ResourceOperation = Operation<Resource>;
export type AfterTraverse = (db: Database.Database) => Promise<void>;

export function normalizeName(name: string): string {
    const r = name.replace(/[/\\]/g, "-").replace(/\-{2,}/g, "-");
    return r.startsWith("-") ? r.substring(1) : r;
}

export async function traverse<T extends { id: number }>(
    db: Database.Database,
    table: string,
    ops: Operation<T>[],
    cbs: AfterTraverse[],
    start?: number,
) {
    let id = start || 0;
    const size = 100;
    const where = `WHERE id >= ?`;
    const whereParams = [id];
    const { total } = db
        .prepare(`SELECT count(*) AS total FROM ${table} ${where}`)
        .get(...whereParams) as { total: number };
    if (total === 0) {
        console.log("No data");
        return;
    }
    const bar = new cliProgress.SingleBar({ barsize: 100 });
    bar.start(total, 0);
    const sql = `SELECT * FROM ${table} WHERE id >= ? ORDER BY id ASC LIMIT ?`;
    while (true) {
        const rows = db.prepare(sql).all(id, size) as T[];
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const fieldsChanged: (keyof T)[] = [];
            for (let j = 0; j < ops.length; j++) {
                try {
                    const fields = await ops[j](db, row);
                    fields.forEach((field) => {
                        if (!fieldsChanged.includes(field)) {
                            fieldsChanged.push(field);
                        }
                    });
                } catch (e) {
                    throw e;
                }
            }
            if (fieldsChanged.length > 0) {
                const values: unknown[] = [];
                const fields = fieldsChanged.map((field) => {
                    values.push(row[field]);
                    return `${String(field)} = ?`;
                });
                db.prepare(
                    `UPDATE ${table} SET ${fields.join(",")} WHERE id = ${row.id}`,
                ).run(...values);
            }
            bar.increment();
        }
        if (rows.length < size) {
            break;
        }
        if (rows.length > 0) {
            id = rows[rows.length - 1].id + 1;
        }
    }
    bar.stop();
    for (let i = 0; i < cbs.length; i++) {
        await cbs[i](db);
    }
}
