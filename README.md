# CYPRIOT

![](/public/code_graph.png)

<p align="center">Source code graph made through `dep-tree`</p>

> [!NOTE]
> Tests and Documentation are not up-to-date.
> Adding new features, so they might not catch up for a while

An attempt at creating a relational database management system in Rust.  
Couple things to note:
- I don't know why I'm naming this cypriot.
- I am referring to 0 books and/or papers for this, just pure delusion that I can do this.
- This project has already made me cry ~~thrice~~ ~~five~~ seven times.

## The State of Things

Currently, what we have is:
- A table with relevant structs and enums and so on.
- Three supported data types: `Int`, `String`, `Float`.
- Functions to:
  - Create a table
  - Assign columns as primary keys (handled during updates, inserts, etc)
  - Assign columns as foreign keys (handled during updates, inserts, etc. Also cascading updates!)
    - Currently foreign key connections are only allowed on primary key fields
  - Insert data
    - Whole record at once
    - Only particular columns
  - Update data
    - Blanket update on a column
    - Update a specific entry, conditional on multiple columns
  - Delete data
    - Delete records from a table meeting specific conditions
    - Both this and update cascade their changes to downstream referencing tables
  - Project from table
    - Entire table at once
    - Specific columns
  - Filter records (can be used with projection)
  - Describe the table
  - Exporting/Importing tables (currently imports only work for the formatting of the export, actively trying to figure out how to standardize them further)
> [!NOTE]
> The tests directory is not included in the repo.
> Please write them yourself while testing or raise an issue if you want me to add them.

## The Road Ahead
I'm thinking of getting the delete functionality up and running next, and then move on to 
building some sort of query language. No solid roadmap yet so I'm not making a checklist,
but we'll burn that bridge when we get to it. (please refer to accompanying Github project if needed)

## Tinkering around
Contributions are welcome, but it's too early in this project's lifetime and there's
a lot of initial scaffolding left. Even so, if you want to play around with it,
fork and make your own version or whatever, I'd love to see your work. Working
on this is as much about learning how these things operate as it is about building
the final product, so any/all help is welcome.