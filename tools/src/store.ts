import { createStore } from "solid-js/store";
import request from "./request";

const [state, setState] = createStore<{
    file: string;
    page: number;
    size: number;
    total: number;
    list: { id: number; name: string; text: string }[];
    index: number;
    kw: string;
    definition: string;
}>({
    file: "",
    page: 1,
    size: 10,
    total: 0,
    list: [],
    index: -1,
    kw: "",
    definition: "",
});

export { state, setState };

export function getEntryList() {
    request
        .get("/entry", {
            params: {
                page: state.page,
                size: state.size,
                kw: state.kw,
            },
        })
        .then((res) => {
            const { page, size, total, list } = res.data;
            setState("page", page);
            setState("size", size);
            setState("total", total);
            setState("list", list);
            setState("index", -1);
            setState("definition", "");
        })
        .catch((e) => {
            alert(e.response.data);
        });
}
