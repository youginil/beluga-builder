import { For } from "solid-js";
import { getEntryList, setState, state } from "./store";

function nextPage() {
    if (state.page >= Math.ceil(state.total / state.size)) {
        return;
    }
    setState("page", state.page + 1);
    getEntryList();
}

function prevPage() {
    if (state.page <= 1) {
        return;
    }
    setState("page", state.page - 1);
    getEntryList();
}

export default function () {
    return (
        <div class="flex h-full flex-col bg-gray-100">
            <div class="flex h-10 shrink-0 gap-x-2 border-b p-1.5">
                <input
                    type="text"
                    class="grow rounded px-2"
                    placeholder="Keyword"
                    value={state.kw}
                    onInput={(e) => setState("kw", e.currentTarget.value)}
                />
                <button
                    onClick={getEntryList}
                    class="h-full shrink-0 rounded bg-emerald-500 px-5 text-white hover:bg-emerald-700"
                >
                    Query
                </button>
            </div>
            <div class="grow overflow-y-auto">
                <table class="w-full table-fixed divide-gray-200 text-left text-sm">
                    <thead>
                        <tr>
                            <th class="px-4 py-2">ID</th>
                            <th class="px-4 py-2">Name</th>
                            <th class="px-4 py-2">Text</th>
                        </tr>
                    </thead>
                    <tbody>
                        <For each={state.list}>
                            {(item, i) => (
                                <tr
                                    class="odd:bg-gray-50"
                                    onClick={() => setState("index", i)}
                                >
                                    <td class="whitespace-nowrap px-4 py-2">
                                        {item.id}
                                    </td>
                                    <td class="whitespace-nowrap px-4 py-2">
                                        {item.name}
                                    </td>
                                    <td class="overflow-hidden text-ellipsis whitespace-nowrap px-4 py-2">
                                        {item.text}
                                    </td>
                                </tr>
                            )}
                        </For>
                    </tbody>
                </table>
            </div>
            <div class="flex h-10 shrink-0 items-center justify-end gap-2 border-t px-2">
                <span class="text-sm font-semibold">
                    {"Total " + state.total + ", Page " + state.page}
                </span>
                <div class="inline-flex">
                    <button
                        onClick={prevPage}
                        class="rounded-l bg-gray-800 py-1 px-4 text-sm text-white hover:bg-gray-900"
                    >
                        Prev
                    </button>
                    <button
                        onClick={nextPage}
                        class="rounded-r bg-gray-800 py-1 px-4 text-sm text-white hover:bg-gray-900"
                    >
                        Next
                    </button>
                </div>
            </div>
        </div>
    );
}
