
This is a D language binding for the lmdb (lowlevel mmapped database) library.

Prerequisites:
- You must have liblmdb.so 0.9.21 or newer installed on your system
- A working D compiler and Dub.

Usage:
- Add 'lmdb' as a dependancy to your dub project file.
- Please also read the dub documentation for details.
- Then use 

```
import lmdb;      /* Import the binding modules into scope */
import lmdb_oo;   /* Import optionally a more OO layer, WIP */
```

Todos:
- Added real unit- and module-tests
- Create D style OO layer instead of just porting the C++ approach.
