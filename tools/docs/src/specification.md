# Specification

## Concepts

### Entry

Entry is a word/phrase definition written by HTML. There are some rules you should know

- if the definition is `@@@LINK={another_entry_name}`, it means redirect to another entry
- resource url is `/@resource?name={resource_name}`
- `<a href="entry://{entry_name}">` entry link
- `<a href="sound://{resource_name}">` click to play sound, you can also use audio element with resource url

### Resource

Images, audios in entry are called resources

### Token

If entry is a phrase, you can split it into some words, every single word is a token. This is for searching phrases by word. It is not required.

## Dictionary Files

Dictionary is a directory which contains some files.

> - [x] means required

- [x] English `(a dictionary named English, it is also a directory)`
  - [x] index.bel `(the file consists of entries)`
  - [ ] index.beld `(resource file, such as images, audios)`
  - [ ] index.{flag}.beld `(take a flag in case you have more resource files)`
  - [ ] index.css `(the default style file)`
  - [ ] index.js `(the default javascript file)`
  - [ ] ... `(other files if you need)`
