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
    $(
        'link[href="LongmanDictionaryOfContemporaryEnglish6thEnEn.css"]',
    ).remove();
    $(
        'script[src="LongmanDictionaryOfContemporaryEnglish6thEnEn.js"]',
    ).remove();
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
