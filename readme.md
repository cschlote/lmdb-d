
# This is a D language binding for the lmdb (lightning memmapped DB) library.

## Prerequisites
- You must have liblmdb.so 0.9.21 or newer installed on your system
- A working D compiler and Dub.

## Usage
- Add 'lmdb' as a dependancy to your dub project file.
- Please also read the dub documentation for details.
- Then use the following import in your sources
```
import lmdb;      /* Import the D binding module into scope */
```

## Advanced usage
There is some thin OO layer provided. It's experimental right now.
You can use it by using the import below
```
import lmdb_oo;   /* Import optionally a more OO layer, WIP */
```

## Todos
- Add CI pipeline (on my own server with GitLab)
- Added serious unit- and module-tests

Maybe:
- Create D style OO layer instead of just porting the C++ approach.
