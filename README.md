# CYPRIOT

An attempt at creating a relational database management system in Rust.  
Couple of things to note:
- I don't know why I'm naming this cypriot.
- I am referring to 0 books and/or papers for this, just pure delusion that I can do this.
- This project has already made me cry thrice.

## The State of Things
Currently, what we have is:
- A table with relevant structs and enums and so on.
- Three supported data types: `Int`, `String`, `Float`.
- Functions to:
  - Create a table
  - Insert data (complete records or for specific columns)
  - Update data (blanket updates or conditional)
  - Select rows (entire table or specific columns)
  - Describe the table

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