import mime from "mime";
import { createEffect } from "solid-js";
import request from "./request";
import { state } from "./store";

const WORD_DB_EXT = "bel-db";

const forEach = Array.prototype.forEach;

export default function () {
    let root!: HTMLIFrameElement;

    createEffect(() => {
        root.innerHTML = "";
        const iframe = document.createElement("iframe");
        root.append(iframe);
        iframe.className = "w-full h-full overflow-y-auto";
        const doc = iframe.contentWindow!.document;
        const sep = navigator.platform === "Win32" ? "\\" : "/";
        const filename = state.file.split(sep).pop();
        const m = filename?.match(new RegExp(`(.+)\.${WORD_DB_EXT}$`));
        if (m) {
            const name = m[1];
            const jsEl = document.createElement("script");
            jsEl.src = `/api/file/${name}.js`;
            doc.head.append(jsEl);
            const cssEl = document.createElement("link");
            cssEl.rel = "stylesheet";
            cssEl.href = `/api/file/${name}.css`;
            doc.head.append(cssEl);
        }
        doc.body.innerHTML = state.definition;
        forEach.call(doc.getElementsByTagName("img"), (el) => {
            const src = el.getAttribute("src");
            if (src) {
                el.src = "/api" + src;
            }
        });
        const sounds: {
            href: string;
            data: Blob | null;
        }[] = [];
        forEach.call(doc.getElementsByTagName("a"), (el) => {
            const href = el.getAttribute("href");
            if (href && /^sound:\/\//.test(href)) {
                const item = {
                    href,
                    data: null,
                };
                sounds.push(item);
                const name = href.substring(8);
                request
                    .get("/@resource", {
                        params: { name: encodeURIComponent(name) },
                    })
                    .then((res) => {
                        item.data = res.data;
                    });
            }
        });
    });
    return <div ref={root} class="h-full w-full overflow-auto"></div>;
}
