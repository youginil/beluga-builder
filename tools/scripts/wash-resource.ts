import promots from "prompts";
import Database from "better-sqlite3";
import fs from "fs";
import {
    AfterTraverse,
    Resource,
    ResourceOperation,
    normalizeName,
    traverse,
} from "./base";
import { afterMyOperation, resourceOperation } from "./ops";

const ResourceTable = "entry";
const ResourceExtName = "beld-db";

const normalizeResName: ResourceOperation = async (
    db: Database.Database,
    resource: Resource,
) => {
    const name = normalizeName(resource.name);
    if (name !== resource.name) {
        resource.name = name;
        return ["name"];
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
    if (!fs.existsSync(filepath) || !filepath.endsWith("." + ResourceExtName)) {
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
                title: "Normalize Name",
                value: "normalize_name",
            },
            {
                title: "My Operation",
                value: "my_operation",
            },
        ],
    });
    const ops: ResourceOperation[] = [];
    const cbs: AfterTraverse[] = [];
    if (r2.ops.includes("normalize_name")) {
        ops.push(normalizeResName);
    }
    if (r2.ops.includes("my_operation")) {
        ops.push(resourceOperation);
        cbs.push(afterMyOperation);
    }
    if (ops.length > 0) {
        await traverse(db, ResourceTable, ops, cbs);
    }
})();
