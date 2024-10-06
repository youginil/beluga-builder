import * as monaco from 'monaco-editor';
import HtmlWorker from 'monaco-editor/esm/vs/language/html/html.worker?worker';
import { createEffect, createSignal, onMount } from 'solid-js';
import { setState, state } from './store';

self.MonacoEnvironment = {
    getWorker() {
        return new HtmlWorker();
    },
};

export default function () {
    let editorEl!: HTMLDivElement;
    let editor: monaco.editor.IStandaloneCodeEditor;
    onMount(() => {
        editor = monaco.editor.create(editorEl, {
            language: 'html',
            automaticLayout: true,
            wordWrap: 'on',
            minimap: { enabled: false },
        });
        let timer: number | null = null;
        editor.getModel()?.onDidChangeContent(() => {
            if (timer !== null) {
                clearTimeout(timer);
            }
            timer = setTimeout(() => {
                timer = null;
                setState('definition', editor.getValue());
            }, 400);
        });
    });

    createEffect(() => {
        const item = state.list[state.index];
        if (item) {
            editor.setValue(item.text);
        }
    });

    return <div class="h-full bg-zinc-300" ref={editorEl}></div>;
}
