import { createSignal } from "solid-js";
import request from "./request";
import { getEntryList, setState } from "./store";

export default function () {
    const [file, setFile] = createSignal("");

    function load(file: string) {
        request
            .post("/load", { file })
            .then(() => {
                setState("file", file);
                setState("index", -1);
                getEntryList();
            })
            .catch((e) => {
                alert(e.response.data);
            });
    }

    function unload() {
        request.post("/unload").then(() => {
            request
                .post("/unload")
                .then(() => {
                    setState("file", "");
                    setState("list", []);
                    setState("kw", "");
                })
                .catch((e) => {
                    alert(e.response.data);
                });
        });
    }
    return (
        <div class="flex h-[50px] items-center gap-x-2 bg-slate-200 p-2">
            <input
                type="text"
                placeholder="/path/to/dictionary.bel-db"
                class="h-full grow rounded px-2"
                value={file()}
                onInput={(e) => setFile(e.currentTarget.value)}
            />
            <button
                onClick={() => load(file())}
                class="h-full shrink-0 rounded bg-sky-500 px-5 text-white hover:bg-sky-700"
            >
                Load
            </button>
            <button
                onClick={() => unload()}
                class="h-full shrink-0 rounded bg-red-500 px-5 text-white hover:bg-red-700"
            >
                Unload
            </button>
        </div>
    );
}
