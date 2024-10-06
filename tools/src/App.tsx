import type { Component } from "solid-js";
import FilePath from "./FilePath";
import EntryList from "./EntryList";
import EntryCode from "./EntryCode";
import EntryPreview from "./EntryPreview";

const App: Component = () => {
    return (
        <>
            <div class="fixed top-0 left-0 right-0 h-[50px]">
                <FilePath />
            </div>
            <div class="fixed top-[50px] left-0 bottom-0 w-1/2">
                <div class="h-1/2">
                    <EntryList />
                </div>
                <div class="h-1/2">
                    <EntryCode />
                </div>
            </div>
            <div class="fixed right-0 top-[50px] bottom-0 w-1/2">
                <EntryPreview />
            </div>
        </>
    );
};

export default App;
