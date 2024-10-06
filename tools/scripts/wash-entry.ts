import cliProgress from "cli-progress";
import * as cheerio from "cheerio";
import promots from "prompts";
import Database from "better-sqlite3";
import nlp from "compromise";
import fs from "fs";
import { exec } from "child_process";
import { Stream } from "stream";
import {
    AfterTraverse,
    Entry,
    EntryOperation,
    Token,
    normalizeName,
    traverse,
} from "./base";
import { afterMyOperation, entryOperation } from "./ops";
const lemmatize = require("wink-lemmatizer");

const log = console.log;

function print_change(id: number, from: any, to: any, layout: "h" | "v" = "h") {
    log(id);
    if (layout === "h") {
        console.log(from + " >>> " + to);
    } else {
        console.dir(from, { depth: null, color: true });
        log(">>>");
        console.dir(to, { depth: null, color: true });
    }
}

const EntryTable = "entry";
const TokenTable = "token";
const EntryExtName = "bel-db";

const fixHtml: EntryOperation = async (db: Database.Database, entry: Entry) => {
    return new Promise<(keyof Entry)[]>((resolve, reject) => {
        const options = [
            "--tidy-mark no",
            "--show-info no",
            "--custom-tags yes",
            "--show-warnings no",
            "--drop-empty-elements no",
            "--fix-uri no",
            "--drop-empty-paras no",
            "--fix-style-tags no",
        ];
        const p = exec(
            `tidy ${options.join(" ")} -iq`,
            (err, stdout, stderr) => {
                if (!err || err.code === 1) {
                    print_change(entry.id, entry.text, stdout, "v");
                    entry.text = stdout;
                    resolve(["text"]);
                } else {
                    reject(stderr);
                }
            },
        );
        const stream = new Stream.Readable();
        stream.push(entry.text);
        stream.push(null);
        stream.pipe(p.stdin!);
    });
};

const trimSpaces: EntryOperation = async (
    db: Database.Database,
    entry: Entry,
) => {
    const fields: (keyof Entry)[] = [];
    const name = entry.name.trim().replace(/\s+/g, " ");
    if (name !== entry.name) {
        entry.name = name;
        fields.push("name");
    }
    const text = entry.text.trim();
    if (text !== entry.text) {
        entry.text = text;
        fields.push("text");
    }
    return fields;
};

export function _tokenize(text: string): string[] {
    const terms = nlp(text).termList();
    const lowerCaseText = text.toLowerCase();
    const words: string[] = [];
    function addWord(name: string) {
        if (name.toLowerCase() !== lowerCaseText && !words.includes(name)) {
            words.push(name);
        }
    }
    terms.forEach((term) => {
        if (term.text) {
            addWord(term.text);
        }
        if (term.normal) {
            addWord(term.normal);
            let lemma: string = "";
            if (term.chunk === "Noun") {
                lemma = lemmatize.noun(term.normal);
            } else if (term.chunk === "Verb") {
                lemma = lemmatize.verb(term.normal);
            } else if (term.chunk === "Adjective") {
                lemma = lemmatize.adjective(term.normal);
            }
            if (lemma) {
                addWord(lemma);
            }
        }
        // @ts-ignore
        if (term.implicit) {
            // @ts-ignore
            addWord(term.implicit);
        }
    });
    return words;
}

const tokenCache = new Map<string, string[]>();
const tokenize: EntryOperation = async (
    db: Database.Database,
    entry: Entry,
) => {
    const tokens = _tokenize(entry.name);
    tokens.forEach((item) => {
        const entry_names: string[] = tokenCache.get(item) ?? [];
        if (!entry_names.includes(entry.name)) {
            entry_names.push(entry.name);
        }
        tokenCache.set(item, entry_names);
    });
    return [];
};

const afterTokenize: AfterTraverse = async (db: Database.Database) => {
    const stmt = db.prepare(
        `INSERT INTO ${TokenTable} (name, entries) VALUES (?, ?)`,
    );
    const trans = db.transaction((tokens: Token[]) => {
        for (let i = 0; i < tokens.length; i++) {
            const tk = tokens[i];
            stmt.run(tk.name, JSON.stringify(tk.entries));
        }
    });
    const tokens: Token[] = [];
    const pb = new cliProgress.SingleBar({ barsize: 100 });
    pb.start(tokenCache.size, 0);
    const step = 100;
    tokenCache.forEach((v, k) => {
        tokens.push({ name: k, entries: v });
        if (tokens.length === step) {
            trans.immediate(tokens);
            tokens.length = 0;
            pb.increment(step);
        }
    });
    trans.immediate(tokens);
    pb.increment(tokens.length);
    pb.stop();
};

function clearTokenTable(db: Database.Database) {
    db.exec(`delete from ${TokenTable}`);
    db.exec(`update sqlite_sequence SET seq = 0 where name ='${TokenTable}'`);
}

const replaceResName: EntryOperation = async (
    db: Database.Database,
    entry: Entry,
) => {
    entry.text = entry.text.trim();
    if (entry.text.startsWith("@@@LINK=")) {
        return [];
    }
    const filePrefix = "file://";
    const $ = cheerio.load(entry.text);
    const imgs = $("img");
    let changed = false;
    imgs.each(function () {
        let src = $(this).attr("src");
        if (
            src &&
            !/^https?:\/\//.test(src) &&
            !src.startsWith("/@resource?")
        ) {
            if (src.startsWith(filePrefix)) {
                src = src.substring(filePrefix.length);
            }
            const src2 =
                "/@resource?name=" + encodeURIComponent(normalizeName(src));
            $(this).attr("src", src2);
            changed = true;
        }
    });
    const soundPrefix = "sound://";
    const alinks = $("a");
    alinks.each(function () {
        const href = $(this).attr("href");
        if (href && href.startsWith(soundPrefix)) {
            const name = href.substring(soundPrefix.length);
            const href2 = "sound://" + normalizeName(name);
            $(this).attr("href", href2);
            changed = true;
        }
    });
    if (changed) {
        entry.text = $("body").html() ?? entry.text;
        return ["text"];
    }
    return [];
};

(async () => {
    const r1 = await promots({
        type: "text",
        name: "filepath",
        message: "Please input the dictionary location",
    });
    const filepath: string = r1.filepath;
    if (!fs.existsSync(filepath) || !filepath.endsWith("." + EntryExtName)) {
        console.error("Invalid dictionary path:", filepath);
        return;
    }
    const db = new Database(filepath);
    const r2 = await promots({
        type: "multiselect",
        name: "ops",
        message: "What do you want to do?",
        choices: [
            {
                title: "Fix document",
                value: "fix",
            },
            {
                title: "Trim spaces",
                value: "trim",
            },
            {
                title: "Tokenization",
                value: "token",
            },
            {
                title: "Clear Token Table",
                value: "clear_token_table",
            },
            {
                title: "Replace Resource Name",
                value: "replace_res_name",
            },
            {
                title: "My Operation",
                value: "my_operation",
            },
        ],
    });
    const ops: EntryOperation[] = [];
    const cbs: AfterTraverse[] = [];
    if (r2.ops.includes("fix")) {
        ops.push(fixHtml);
    }
    if (r2.ops.includes("trim")) {
        ops.push(trimSpaces);
    }
    if (r2.ops.includes("token")) {
        ops.push(tokenize);
        cbs.push(afterTokenize);
    }
    if (r2.ops.includes("clear_token_table")) {
        clearTokenTable(db);
    }
    if (r2.ops.includes("replace_res_name")) {
        ops.push(replaceResName);
    }
    if (r2.ops.includes("my_operation")) {
        ops.push(entryOperation);
        cbs.push(afterMyOperation);
    }
    if (ops.length > 0) {
        await traverse(db, EntryTable, ops, cbs);
    }
})();
