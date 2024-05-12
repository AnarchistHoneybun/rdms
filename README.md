# CYPRIOT

An attempt at creating a relational database management system in Rust.  
Couple of things to note:
- I don't know why I'm naming this cypriot.
- I am referring to 0 books and/or papers for this, just pure delusion that I can do this.
- This project has already made me cry ~~thrice~~ five times.

<div class="callout" style="background-color: #f8d7da; color: #721c24; border-color: #f5c6cb;">
  <strong>⚠️</strong> Tests and Docs are not up to date and might not catch up
for a while! Please bear with me I'm trying to accommodate new features and
it's not feasible to update them for every change.
</div>


## The State of Things

> Massive refactoring going on things might break constantly. Stay tuned

Currently, what we have is:
- A table with relevant structs and enums and so on.
- Three supported data types: `Int`, `String`, `Float`.
- Functions to:
  - Create a table
  - Insert data (complete records or for specific columns)
  - Update data (blanket updates or conditional upon single/multiple columns)
  - Select rows (entire table or specific columns)
  - Describe the table
  - Exporting/Importing tables (currently imports only work for the formatting of the export, actively trying to figure out how to standardize them further)
> **Note:** The tests directory is not included in the repo.
> Please write them yourself while testing or raise an issue if you want me to add them.

## The Road Ahead
I'm thinking of getting the delete functionality up and running next, and then move on to 
building some sort of query language. No solid roadmap yet so I'm not making a checklist,
but we'll burn that bridge when we get to it.

## Tinkering around
Contributions are welcome, but it's too early in this project's lifetime and there's
a lot of initial scaffolding left. Even so, if you want to play around with it,
fork and make your own version or whatever, I'd love to see your work. Working
on this is as much about learning how these things operate as it is about building
the final product, so any/all help is welcome.