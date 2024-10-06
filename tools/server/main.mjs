import Koa from "koa";
import Router from "@koa/router";
import bodyParser from "koa-bodyparser";
import multer from "@koa/multer";
import Database from "better-sqlite3";
import Joi from "joi";
import mime from "mime";
import { respondBuffer, respondError, respondJSON } from "./utils.mjs";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENTRY_TABLE = "entry";
const RES_DB_EXT = "beld-db";

const WebRoot = path.resolve(__dirname, "../dist");

const app = new Koa();
const port = 5000;
const router = new Router();
const upload = multer();

let entryDB;
let resourceDB;
let root = "";
let dbBasename = "";

const checkDatabase = (ctx, next) => {
  if (!entryDB || !entryDB.open) {
    respondError(
      ctx,
      500,
      "Invalid database. You should load dictionary first",
    );
    entryDB = undefined;
  }
  next();
};

const loadSchema = Joi.object({
  file: Joi.string().required(),
});
router.post("/api/load", (ctx, next) => {
  const { value, error } = loadSchema.validate(ctx.request.body);
  if (error) {
    respondError(ctx, 400, error.message);
    return next();
  }
  const file = value.file;
  try {
    entryDB = new Database(file);
    root = path.dirname(file);
    const filename = path.basename(file);
    dbBasename = filename.substring(
      0,
      filename.length - path.extname(filename).length,
    );
    const dataFilename = dbBasename + "." + RES_DB_EXT;
    const dataFile = path.join(root, dataFilename);
    if (fs.existsSync(dataFile)) {
      resourceDB = new Database(dataFile);
    }
  } catch (e) {
    entryDB?.close();
    respondError(ctx, 400, "Invalid file path");
    return next();
  }
  respondJSON(ctx);
  next();
});

router.post("/api/unload", (ctx, next) => {
  if (entryDB) {
    if (entryDB.open) {
      entryDB.close();
    }
    entryDB = undefined;
  }
  if (resourceDB) {
    if (resourceDB.open) {
      resourceDB.close();
    }
    resourceDB = undefined;
  }
  respondJSON(ctx);
  next();
});

router.get("/api/file/:name", checkDatabase, (ctx, next) => {
  const name = ctx.params.name;
  const file = path.join(root, name);
  if (fs.existsSync(file)) {
    ctx.response.type = mime.getType(name) || "application/octet-stream";
    respondBuffer(ctx, fs.readFileSync(file));
  } else {
    respondError(ctx, 404, "No file found");
  }
  next();
});

const entryListQuerySchema = Joi.object({
  page: Joi.number().integer().min(1).required(),
  size: Joi.number().integer().min(1).max(100).required(),
  kw: Joi.string().allow("").default(""),
});
router.get("/api/entry", checkDatabase, (ctx, next) => {
  const { value, error } = entryListQuerySchema.validate(ctx.query);
  if (error) {
    respondError(ctx, 400, error.message);
    return next();
  }
  const total = entryDB
    .prepare(`SELECT count(*) AS total FROM ${ENTRY_TABLE}`)
    .get().total;
  const offset = (value.page - 1) * value.size;
  const [sql, params] = value.kw
    ? [
        `SELECT * FROM ${ENTRY_TABLE} WHERE name LIKE ? LIMIT ? OFFSET ?`,
        [value.kw, value.size, offset],
      ]
    : [`SELECT * FROM ${ENTRY_TABLE} LIMIT ? OFFSET ?`, [value.size, offset]];
  const rows = entryDB.prepare(sql).all(...params);
  respondJSON(ctx, {
    page: value.page,
    size: value.size,
    total,
    list: rows,
  });
});

const dataQuerySchema = Joi.object({
  name: Joi.string().required(),
});
router.get("/api/@resource", checkDatabase, (ctx, next) => {
  if (resourceDB && resourceDB.open) {
    const { value, error } = dataQuerySchema.validate(ctx.request.query);
    if (error) {
      respondError(ctx, 400, error.message);
    } else {
      let name = value.name;
      const row = resourceDB
        .prepare(`SELECT * FROM ${ENTRY_TABLE} WHERE name = ?`)
        .get(name);
      if (row) {
        ctx.response.type = mime.getType(name) || "application/octet-stream";
        // @ts-ignore
        respondBuffer(ctx, row.binary);
      } else {
        respondError(ctx, 404, "No resource: " + name);
      }
    }
  } else {
    respondError(ctx, 404, "No data resource");
  }
  next();
});

router.get("/^(api)", (ctx, next) => {
  let p = ctx.path.substring(1);
  if (p === "") {
    p = "index.html";
  }
  const file = path.resolve(WebRoot, p);
  if (fs.existsSync(file)) {
    const data = fs.readFileSync(file);
    ctx.response.type = mime.getType(file) || "application/octet-stream";
    respondBuffer(ctx, data);
  } else {
    respondError(ctx, 404, "Not Found");
  }
  next();
});

app.use(bodyParser()).use(router.routes()).use(router.allowedMethods());

app.on("error", (err, ctx) => {
  console.error(err, ctx);
});

app.listen(port).on("listening", () => {
  console.log(`Listening on ${port}...`);
});
