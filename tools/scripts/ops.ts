import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import {
    AfterTraverse,
    Entry,
    EntryOperation,
    Resource,
    ResourceOperation,
} from "./base";

export const entryOperation: EntryOperation = async (
    db: Database.Database,
    entry: Entry,
) => {
    if (entry.text.startsWith("@@@LINK=")) {
        return [];
    }
    let changed = true;
    const $ = cheerio.load(entry.text);
    // $(
    //     'link[href="thes.css"]',
    // ).remove();
    // $(
    //     'script[src="thes.js"]',
    // ).remove();
    // $(
    //     'script[src="config.ini"]',
    // ).remove();
    $('a[href^="bword://"]').each((_, el) => {
        const href = $(el).attr("href")!;
        const newHref = href.replace(/^bword/, "entry");
        $(el).attr("href", newHref);
    });
    if (changed) {
        entry.text = $("body").html() ?? entry.text;
        return ["text"];
    }
    return [];
};

export const resourceOperation: ResourceOperation = async (
    db: Database.Database,
    resource: Resource,
) => {
    return [];
};

export const afterMyOperation: AfterTraverse = async (
    db: Database.Database,
) => {
    //
};
