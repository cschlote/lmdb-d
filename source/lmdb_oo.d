/* This is free and unencumbered software released into the public domain. */
module lmdb_oo;

/**
 * <lmdb_oo.d> - D wrapper for LMDB.
 *
 * @author Carsten Schlote <schlote@vahanus.net>
 * @author Arto Bendiken <arto@bendiken.net>
 * @see https://sourceforge.net/projects/lmdbxx/
 */

import core.stdc.string;
import std.algorithm.mutation;
import std.conv;
import std.exception;
import std.stdio;
import std.string;

import lmdb;

alias mode = mdb_mode_t;

/* Exceptions for classes */

/** Base class for LMDB exceptions */
class MbdError : Exception {
	///
    mixin basicExceptionCtors;
    /**
   * Throws an MbdError based on the given LMDB return code.
   */
    static void raise(string origin, int rc) {
        string msg = origin; msg ~= "(" ~ fromStringz(mdb_strerror(rc)) ~ ")";
        switch (rc) {
        case MDB_KEYEXIST:
            throw new KeyExistError(origin);
        case MDB_NOTFOUND:
            throw new NotFoundError(origin);
        case MDB_CORRUPTED:
            throw new CorruptedError(origin);
        case MDB_PANIC:
            throw new PanicError(origin);
        case MDB_VERSION_MISMATCH:
            throw new VersionMismatchError(origin);
        case MDB_MAP_FULL:
            throw new MapFullError(origin);
        case MDB_BAD_DBI:
            throw new BadDbiError(origin);
        default:
            throw new RuntimeError(origin);
        }
    }
}

/** Base class for logic error conditions. */
class LogicError : MbdError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}

/** Base class for fatal error conditions. */
class FatalError : MbdError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}

/** Base class for runtime error conditions. */
class RuntimeError : MbdError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}

/** Exception class for `MDB_KEYEXIST` errors.
 * @see http://symas.com/mdb/doc/group__errors.html#ga05dc5bbcc7da81a7345bd8676e8e0e3b
 */
final class KeyExistError : RuntimeError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}

/** Exception class for `MDB_NOTFOUND` errors.
 * @see http://symas.com/mdb/doc/group__errors.html#gabeb52e4c4be21b329e31c4add1b71926
 */
final class NotFoundError : RuntimeError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}

/** Exception class for `MDB_CORRUPTED` errors.
 * @see http://symas.com/mdb/doc/group__errors.html#gaf8148bf1b85f58e264e57194bafb03ef
 */
final class CorruptedError : FatalError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}

/** Exception class for `MDB_PANIC` errors.
 * @see http://symas.com/mdb/doc/group__errors.html#gae37b9aedcb3767faba3de8c1cf6d3473
 */
final class PanicError : FatalError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}

/** Exception class for `MDB_VERSION_MISMATCH` errors.
 * @see http://symas.com/mdb/doc/group__errors.html#ga909b2db047fa90fb0d37a78f86a6f99b
 */
final class VersionMismatchError : FatalError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}

/** Exception class for `MDB_MAP_FULL` errors.
 * @see http://symas.com/mdb/doc/group__errors.html#ga0a83370402a060c9175100d4bbfb9f25
 */
final class MapFullError : RuntimeError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}

/** Exception class for `MDB_BAD_DBI` errors.
 * @since 0.9.14 (2014/09/20)
 * @see http://symas.com/mdb/doc/group__errors.html#gab4c82e050391b60a18a5df08d22a7083
 */
final class BadDbiError : RuntimeError {
public:
    /** Constructor */
    pure nothrow @nogc @safe this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
}


/* ------------------------------------------------------------------ */

/* Procedural Interface: Metadata */

// TODO: mdb_version()
// TODO: mdb_strerror()


/* Procedural Interface: Environment */

// TODO: mdb_env_set_assert()
// TODO: mdb_reader_list()
// TODO: mdb_reader_check()

/** Wrapper for mdb_env_create
 * @throws lmdb_oo.error on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gaad6be3d8dcd4ea01f8df436f41d158d4
 */
static void env_create(MDB_env** env) {
    const int rc = mdb_env_create(env);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_create", rc);
    }
}

/** Wrapper for mdb_env_open
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340
 */
static void env_open(MDB_env* env, const char* path, const uint flags, const mode mode) {
    const int rc = mdb_env_open(env, path, flags, mode);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_open", rc);
    }
}

/** Wrapper for mdb_env_copy
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga3bf50d7793b36aaddf6b481a44e24244
 * @see http://symas.com/mdb/doc/group__mdb.html#ga5d51d6130325f7353db0955dbedbc378
 */
static void env_copy(MDB_env* env, const char* path, const uint flags = 0) {
    const int rc = mdb_env_copy2(env, path, flags);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_copy2", rc);
    }
}

/** Wrapper for mdb_env_copy_fd
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga5040d0de1f14000fa01fc0b522ff1f86
 * @see http://symas.com/mdb/doc/group__mdb.html#ga470b0bcc64ac417de5de5930f20b1a28
 */
static void env_copy_fd(MDB_env* env, const mdb_filehandle_t fd, const uint flags = 0) {
    const int rc = mdb_env_copyfd2(env, fd, flags);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_copyfd2", rc);
    }
}

/** Wrapper for mdb_env_stat
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gaf881dca452050efbd434cd16e4bae255
 */
static void env_stat(MDB_env* env, MDB_stat* stat) {
    const int rc = mdb_env_stat(env, stat);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_stat", rc);
    }
}

/** Wrapper for env_info
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga18769362c7e7d6cf91889a028a5c5947
 */
static void env_info(MDB_env* env, MDB_envinfo* stat) {
    const int rc = mdb_env_info(env, stat);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_info", rc);
    }
}

/** Wrapper for mdb_env_sync
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga85e61f05aa68b520cc6c3b981dba5037
 */
static void env_sync(MDB_env* env, const bool force = true) {
    const int rc = mdb_env_sync(env, force);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_sync", rc);
    }
}

/** Wrapper for mdb_env_close
 * @see http://symas.com/mdb/doc/group__mdb.html#ga4366c43ada8874588b6a62fbda2d1e95
 */
static void env_close(MDB_env* env) nothrow {
    mdb_env_close(env);
}

/** Wrapper for mdb_env_set_flags
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga83f66cf02bfd42119451e9468dc58445
 */
static void env_set_flags(MDB_env* env, const uint flags, const bool onoff = true) {
    const int rc = mdb_env_set_flags(env, flags, onoff ? 1 : 0);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_set_flags", rc);
    }
}

/** Wrapper for mdb_env_get_flags
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga2733aefc6f50beb49dd0c6eb19b067d9
 */
static void env_get_flags(MDB_env* env, uint* flags) {
    const int rc = mdb_env_get_flags(env, flags);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_get_flags", rc);
    }
}

/** Wrapper for mdb_env_get_path
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gac699fdd8c4f8013577cb933fb6a757fe
 */
static void env_get_path(MDB_env* env, const char** path) {
    const int rc = mdb_env_get_path(env, path);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_get_path", rc);
    }
}

/** Wrapper for mdb_env_get_fd
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gaf1570e7c0e5a5d860fef1032cec7d5f2
 */
static void env_get_fd(MDB_env* env, mdb_filehandle_t* fd) {
    const int rc = mdb_env_get_fd(env, fd);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_get_fd", rc);
    }
}

/** Wrapper for mdb_env_set_mapsize
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5
 */
static void env_set_mapsize(MDB_env* env, const size_t size) {
    const int rc = mdb_env_set_mapsize(env, size);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_set_mapsize", rc);
    }
}

/** Wrapper for mdb_env_set_max_readers
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gae687966c24b790630be2a41573fe40e2
 */
static void env_set_max_readers(MDB_env* env, const uint count) {
    const int rc = mdb_env_set_maxreaders(env, count);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_set_maxreaders", rc);
    }
}

/** Wrapper for mdb_env_get_max_readers
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga70e143cf11760d869f754c9c9956e6cc
 */
static void env_get_max_readers(MDB_env* env, uint* count) {
    const int rc = mdb_env_get_maxreaders(env, count);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_get_maxreaders", rc);
    }
}

/** Wrapper for mdb_env_set_max_dbs
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gaa2fc2f1f37cb1115e733b62cab2fcdbc
 */
static void env_set_max_dbs(MDB_env* env, const MDB_dbi count) {
    const int rc = mdb_env_set_maxdbs(env, count);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_set_maxdbs", rc);
    }
}

/** Wrapper for mdb_env_get_max_keysize
 * @see http://symas.com/mdb/doc/group__mdb.html#gaaf0be004f33828bf2fb09d77eb3cef94
 */
static uint env_get_max_keysize(MDB_env* env) {
    const int rc = mdb_env_get_maxkeysize(env);
    assert(rc >= 0);
    return cast(uint) rc;
}

/** Wrapper for mdb_env_set_userctx
 * @throws lmdb_oo.MbdError on failure
 * @since 0.9.11 (2014/01/15)
 * @see http://symas.com/mdb/doc/group__mdb.html#gaf2fe09eb9c96eeb915a76bf713eecc46
 */
static void env_set_userctx(MDB_env* env, void* ctx) {
    const int rc = mdb_env_set_userctx(env, ctx);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_env_set_userctx", rc);
    }
}

/** Wrapper for mdb_env_get_userctx
 * @since 0.9.11 (2014/01/15)
 * @see http://symas.com/mdb/doc/group__mdb.html#ga45df6a4fb150cda2316b5ae224ba52f1
 */
static void* env_get_userctx(MDB_env* env) {
    return mdb_env_get_userctx(env);
}


/* Procedural Interface: Transactions */

/** Wrapper for mdb_txn_begin
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gad7ea55da06b77513609efebd44b26920
 */
static void txn_begin(MDB_env* env, MDB_txn* parent, const uint flags, MDB_txn** txn) {
    const int rc = mdb_txn_begin(env, parent, flags, txn);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_txn_begin", rc);
    }
}

/** Wrapper for mdb_txn_env
 * @see http://symas.com/mdb/doc/group__mdb.html#gaeb17735b8aaa2938a78a45cab85c06a0
 */
static MDB_env* txn_env(MDB_txn* txn) nothrow {
    return mdb_txn_env(txn);
}

/** Wrapper for mdb_txn_id
 * @note Only available in HEAD, not yet in any 0.9.x release (as of 0.9.16).
 */
static size_t txn_id(MDB_txn* txn) nothrow {
    return mdb_txn_id(txn);
}

/** Wrapper for mdb_txn_commit
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga846fbd6f46105617ac9f4d76476f6597
 */
static void txn_commit(MDB_txn* txn) {
    const int rc = mdb_txn_commit(txn);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_txn_commit", rc);
    }
}

/** Wrapper for mdb_txn_abort
 * @see http://symas.com/mdb/doc/group__mdb.html#ga73a5938ae4c3239ee11efa07eb22b882
 */
static void txn_abort(MDB_txn* txn) nothrow {
    mdb_txn_abort(txn);
}

/** Wrapper for mdb_txn_reset
 * @see http://symas.com/mdb/doc/group__mdb.html#ga02b06706f8a66249769503c4e88c56cd
 */
static void txn_reset(MDB_txn* txn) nothrow {
    mdb_txn_reset(txn);
}

/** Wrapper for mdb_txn_renew
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga6c6f917959517ede1c504cf7c720ce6d
 */
static void txn_renew(MDB_txn* txn) {
    const int rc = mdb_txn_renew(txn);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_txn_renew", rc);
    }
}


/* Procedural Interface: Databases */

/** Wrapper for mdb_dbi_open
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gac08cad5b096925642ca359a6d6f0562a
 */
static void dbi_open(MDB_txn* txn, const char* name, const uint flags, MDB_dbi* dbi) {
    const int rc = mdb_dbi_open(txn, name, flags, dbi);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_dbi_open", rc);
    }
}

/** Wrapper for mdb_stat
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gae6c1069febe94299769dbdd032fadef6
 */
static void dbi_stat(MDB_txn* txn, const MDB_dbi dbi, MDB_stat* result) {
    const int rc = mdb_stat(txn, dbi, result);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_stat", rc);
    }
}

/** Wrapper for mdb_flags
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga95ba4cb721035478a8705e57b91ae4d4
 */
static void dbi_flags(MDB_txn* txn, const MDB_dbi dbi, uint* flags) {
    const int rc = mdb_dbi_flags(txn, dbi, flags);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_dbi_flags", rc);
    }
}

/** Wrapper for mdb_close
 * @see http://symas.com/mdb/doc/group__mdb.html#ga52dd98d0c542378370cd6b712ff961b5
 */
static void dbi_close(MDB_env* env, const MDB_dbi dbi) nothrow {
    mdb_dbi_close(env, dbi);
}

/** Wrapper for mdb_drop
 * @see http://symas.com/mdb/doc/group__mdb.html#gab966fab3840fc54a6571dfb32b00f2db
 */
static void dbi_drop(MDB_txn* txn, const MDB_dbi dbi, const bool del = false) {
    const int rc = mdb_drop(txn, dbi, del ? 1 : 0);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_drop", rc);
    }
}

/** Wrapper for mdb_set_compare
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga68e47ffcf72eceec553c72b1784ee0fe
 */
static void dbi_set_compare(MDB_txn* txn, const MDB_dbi dbi, MDB_cmp_func* cmp = null) {
    const int rc = mdb_set_compare(txn, dbi, cmp);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_set_compare", rc);
    }
}

/** Wrapper for mdb_set_dupsort
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gacef4ec3dab0bbd9bc978b73c19c879ae
 */
static void dbi_set_dupsort(MDB_txn* txn, const MDB_dbi dbi, MDB_cmp_func* cmp = null) {
    const int rc = mdb_set_dupsort(txn, dbi, cmp);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_set_dupsort", rc);
    }
}

/** Wrapper for mdb_set_relfunc
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga697d82c7afe79f142207ad5adcdebfeb
 */
static void dbi_set_relfunc(MDB_txn* txn, const MDB_dbi dbi, MDB_rel_func* rel) {
    const int rc = mdb_set_relfunc(txn, dbi, rel);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_set_relfunc", rc);
    }
}

/** Wrapper for mdb_dbi_set_relctx
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga7c34246308cee01724a1839a8f5cc594
 */
static void dbi_set_relctx(MDB_txn* txn, const MDB_dbi dbi, void* ctx) {
    const int rc = mdb_set_relctx(txn, dbi, ctx);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_set_relctx", rc);
    }
}

/** Wrapper for mdb_get
 * @retval true  if the key/value pair was retrieved
 * @retval false if the key wasn't found
 * @see http://symas.com/mdb/doc/group__mdb.html#ga8bf10cd91d3f3a83a34d04ce6b07992d
 */
static bool dbi_get(MDB_txn* txn, const MDB_dbi dbi, const MDB_val* key, MDB_val* data) {
    const int rc = mdb_get(txn, cast(uint) dbi, cast(MDB_val*) key, data);
    if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
        MbdError.raise("mdb_get", rc);
    }
    return (rc == MDB_SUCCESS);
}

/** Wrapper for mdb_put
 * @retval true  if the key/value pair was inserted
 * @retval false if the key already existed
 * @see http://symas.com/mdb/doc/group__mdb.html#ga4fa8573d9236d54687c61827ebf8cac0
 */
static bool dbi_put(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* data, uint flags = 0) {
    const int rc = mdb_put(txn, dbi, key, data, flags);
    if (rc != MDB_SUCCESS && rc != MDB_KEYEXIST) {
        MbdError.raise("mdb_put", rc);
    }
    return (rc == MDB_SUCCESS);
}

/** Wrapper for mdb_del
 * @retval true  if the key/value pair was removed
 * @retval false if the key wasn't found
 * @see http://symas.com/mdb/doc/group__mdb.html#gab8182f9360ea69ac0afd4a4eaab1ddb0
 */
static bool dbi_del(MDB_txn* txn, MDB_dbi dbi, MDB_val* key, MDB_val* data = null) {
    const int rc = mdb_del(txn, dbi, key, data);
    if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
        MbdError.raise("mdb_del", rc);
    }
    return (rc == MDB_SUCCESS);
}


/* -- Procedural Interface: Cursors --------------------------------- */

/** Wrapper for cursor_open
 * @throws lmdb_oo.MbdError on failure
 */
static void cursor_open(MDB_txn* txn, const MDB_dbi dbi, MDB_cursor** cursor) {
    const int rc = mdb_cursor_open(txn, dbi, cursor);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_cursor_open", rc);
    }
}

/** Wrapper for cursor_close
 * @see http://symas.com/mdb/doc/group__mdb.html#gad685f5d73c052715c7bd859cc4c05188
 */
static void cursor_close(MDB_cursor* cursor) nothrow {
    mdb_cursor_close(cursor);
}

/** Wrapper for cursor_renew
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#gac8b57befb68793070c85ea813df481af
 */
static void cursor_renew(MDB_txn* txn, MDB_cursor* cursor) {
    const int rc = mdb_cursor_renew(txn, cursor);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_cursor_renew", rc);
    }
}

/** Wrapper for cursor_txn
 * @see http://symas.com/mdb/doc/group__mdb.html#ga7bf0d458f7f36b5232fcb368ebda79e0
 */
static MDB_txn* cursor_txn(MDB_cursor* cursor) nothrow {
    return mdb_cursor_txn(cursor);
}

/** Wrapper for cursor_dbi
 * @see http://symas.com/mdb/doc/group__mdb.html#ga2f7092cf70ee816fb3d2c3267a732372
 */
static MDB_dbi cursor_dbi(MDB_cursor* cursor) nothrow {
    return mdb_cursor_dbi(cursor);
}

/** Wrapper for cursor_get
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga48df35fb102536b32dfbb801a47b4cb0
 */
static bool cursor_get(MDB_cursor* cursor, MDB_val* key, MDB_val* data, const MDB_cursor_op op) {
    const int rc = mdb_cursor_get(cursor, key, data, op);
    if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
        MbdError.raise("mdb_cursor_get", rc);
    }
    return (rc == MDB_SUCCESS);
}

/** Wrapper for cursor_put
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga1f83ccb40011837ff37cc32be01ad91e
 */
static void cursor_put(MDB_cursor* cursor, MDB_val* key, MDB_val* data, const uint flags = 0) {
    const int rc = mdb_cursor_put(cursor, key, data, flags);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_cursor_put", rc);
    }
}

/** Wrapper for cursor_del
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga26a52d3efcfd72e5bf6bd6960bf75f95
 */
static void cursor_del(MDB_cursor* cursor, const uint flags = 0) {
    const int rc = mdb_cursor_del(cursor, flags);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_cursor_del", rc);
    }
}

/** Wrapper for cursor_count
 * @throws lmdb_oo.MbdError on failure
 * @see http://symas.com/mdb/doc/group__mdb.html#ga4041fd1e1862c6b7d5f10590b86ffbe2
 */
static void cursor_count(MDB_cursor* cursor, ref size_t count) {
    const int rc = mdb_cursor_count(cursor, &count);
    if (rc != MDB_SUCCESS) {
        MbdError.raise("mdb_cursor_count", rc);
    }
}


/* Resource Interface: Values */

/** Wrapper class for `MDB_val` structures.
 *
 * @note Instances of this class are movable and copyable both.
 * @see std.algorithm.mutation
 * @see http://symas.com/mdb/doc/group__mdb.html#structMDB__val
 */
class MdbVal {
protected:
    MDB_val _val;

public:
    /** Default constructor. */
    this() nothrow {
    }

    /** Constructor. Expects a D string */
    this(const string data) nothrow {
        this(toStringz(data), data.sizeof);
    }

    /** Constructor. Expects a C-string as input. */
    this(const char* data) nothrow {
        this(data, strlen(data));
    }

    /** Constructor. Expects some data and size */
    this(const void* data, const size_t size) nothrow {
		_val = MDB_val(data, size);
    }

    /** Constructor. Use a pointer to other object for a shallow copy */
    this(MdbVal* data) nothrow {
		_val = MDB_val(&data._val);
    }

    /** Constructor. Use a reference to a object to do a deep copy */
    this(ref MdbVal data) nothrow {
		_val = data._val; // Triggers copy-constructor of struct dupping the data.
    }

    /** Determines whether this value is empty. */
    bool empty() const nothrow {
        return _val.size() == 0;
    }

    /** Returns the size of the data. */
    size_t size() const nothrow {
        return _val.size();
    }

    /** Returns a pointer to the embedded struct */
    T* data(T)() nothrow {
        return _val.data(T)();
    }

    /** Assigns the value. */
    MdbVal assign(const ref string data) nothrow {
        return assign(toStringz(data), data.sizeof);
    }
    /** Assigns the value. */
    MdbVal assign(const char* data) nothrow {
        return assign(data, strlen(data));
    }
    /** Assigns the value. */
    MdbVal assign(T)(const T* data, const size_t size) nothrow {
        _val.mv_size = size;
        _val.mv_data = cast(void*) data;
        return this;
    }

	/** Print it some useful way... */
	override string toString() const {
		import std.format : format;
		return format("%x @ %d %s", _val.data!(ubyte*)(), _val.size(), _val.data!ubyte()[0.._val.size()]);
	}
}

//static assert(std::is_pod<lmdb_oo.MdbVal>::value, "MdbVal must be a POD type");
//static assert((lmdb_oo.MdbVal.sizeof) == (lmdb.MDB_val.sizeof), "sizeof(lmdb_oo.MdbVal) != sizeof(MDB_val)");


/* Resource Interface: Environment */

/**
 * Resource class for `MDB_env*` handles.
 *
 * @note Instances of this class are movable, but not copyable.
 * @see http://symas.com/mdb/doc/group__internal.html#structMDB__env
 */
class MdbEnv {
protected:
    MDB_env* _handle;

public:
    static uint default_flags;  ///< Default flags
    static mode default_mode = std.conv.octal!644; /**< -rw-r--r-- */

    /**
   * Creates a new LMDB environment.
   *
   * @param flags
   * @throws lmdb_oo.MbdError on failure
   */
    static MdbEnv create(const uint flags = default_flags) {
        MDB_env* handle;
        env_create(&handle);

        assert(handle != null);

        if (flags) {
            try {
                env_set_flags(handle, flags);
            } catch (const MbdError) {
                env_close(handle);
                throw new Exception("Problem opeing env");
            }
        }
        return new MdbEnv(handle);
    }

    /**
   * Constructor.
   *
   * @param handle a valid `MDB_env*` handle
   */
    this(MDB_env* handle) nothrow {
        _handle = handle;
    }

    /**
   * Move constructor.
   */
    //this(MdbEnv&& other) nothrow {
    //  std::swap(_handle, other._handle);
    //}

    /**
   * Move assignment operator.
   */
    //MdbEnv& operator=(MdbEnv&& other) nothrow {
    //  if (this != &other) {
    //   std::swap(_handle, other._handle);
    //  }
    //  return *this;
    //}

    /**
   * Destructor.
   */
    ~this() nothrow {
        try {
            close();
        } catch (Exception ex) {
        }
    }

    /**
   * Returns the underlying `MDB_env*` handle.
   */
    //operator MDB_env*() const nothrow {
    //  return _handle;
    //}

    /**
   * Returns the underlying `MDB_env*` handle.
   */
    MDB_env* handle() const nothrow {
        return cast(MDB_env*) _handle;
    }

    /**
   * Flushes data buffers to disk.
   *
   * @param force
   * @throws lmdb_oo.MbdError on failure
   */
    void sync(const bool force = true) {
        env_sync(handle(), force);
    }

    /**
   * Closes this environment, releasing the memory map.
   *
   * @note this method is idempotent
   * @post `handle() == null`
   */
    void close() nothrow {
        if (handle()) {
            env_close(handle());
            _handle = null;
        }
    }

    /**
   * Opens this environment.
   *
   * @param path
   * @param flags
   * @param mode
   * @throws lmdb_oo.MbdError on failure
   */
    MdbEnv open(const char* path, const uint flags = default_flags, const mode mode = default_mode) {
        env_open(handle(), path, flags, mode);
        return this;
    }

    /**
   * @param flags
   * @param onoff
   * @throws lmdb_oo.MbdError on failure
   */
    MdbEnv set_flags(const uint flags, const bool onoff = true) {
        env_set_flags(handle(), flags, onoff);
        return this;
    }

    /**
   * @param size
   * @throws lmdb_oo.MbdError on failure
   */
    MdbEnv set_mapsize(const size_t size) {
        env_set_mapsize(handle(), size);
        return this;
    }

    /**
   * @param count
   * @throws lmdb_oo.MbdError on failure
   */
    MdbEnv set_max_readers(const uint count) {
        env_set_max_readers(handle(), count);
        return this;
    }

    /**
   * @param count
   * @throws lmdb_oo.MbdError on failure
   */
    MdbEnv set_max_dbs(const MDB_dbi count) {
        env_set_max_dbs(handle(), count);
        return this;
    }
}


/* Resource Interface: Transactions */

/**
 * Resource class for `MDB_txn*` handles.
 *
 * @note Instances of this class are movable, but not copyable.
 * @see http://symas.com/mdb/doc/group__internal.html#structMDB__txn
 */
class MdbTxn {
protected:
    MDB_txn* _handle;

public:
    static uint default_flags;  ///< Default flags

    /**
   * Creates a new LMDB transaction.
   *
   * @param env the environment handle
   * @param parent
   * @param flags
   * @throws lmdb_oo.MbdError on failure
   */
    static MdbTxn begin(MDB_env* env, MDB_txn* parent = null, const uint flags = default_flags) {
        MDB_txn* handle;
        txn_begin(env, parent, flags, &handle);
        assert(handle != null);
        return new MdbTxn(handle);
    }

    /**
   * Constructor.
   *
   * @param handle a valid `MDB_txn*` handle
   */
    this(MDB_txn* handle) nothrow {
        _handle = handle;
    }

    /**
   * Destructor.
   */
    ~this() nothrow {
        if (_handle) {
            try {
                abort();
            } catch (Exception ex) {
            }
            _handle = null;
        }
    }

    /**
   * Returns the underlying `MDB_txn*` handle.
   */
    MDB_txn* opUnary(string s)() if (s == "*") {
      return _handle;
    }

    /**
   * Returns the underlying `MDB_txn*` handle.
   */
    MDB_txn* handle() nothrow {
      return _handle;
    }

    /**
   * Returns the transaction's `MDB_env*` handle.
   */
    MDB_env* env() nothrow {
        return txn_env(handle());
    }

    /**
   * Commits this transaction.
   *
   * @throws lmdb_oo.MbdError on failure
   * @post `handle() == null`
   */
    void commit() {
        txn_commit(_handle);
        _handle = null;
    }

    /**
   * Aborts this transaction.
   *
   * @post `handle() == null`
   */
    void abort() nothrow {
        txn_abort(_handle);
        _handle = null;
    }

    /**
   * Resets this read-only transaction.
   */
    void reset() nothrow {
        txn_reset(_handle);
    }

    /**
   * Renews this read-only transaction.
   *
   * @throws lmdb_oo.MbdError on failure
   */
    void renew() {
        txn_renew(_handle);
    }
}


/* Resource Interface: Databases */

/**
 * Resource class for `MDB_dbi` handles.
 *
 * @note Instances of this class are movable, but not copyable.
 * @see http://symas.com/mdb/doc/group__mdb.html#gadbe68a06c448dfb62da16443d251a78b
 */
class MdbDbi {
protected:
    MDB_dbi _handle;

public:
    static uint default_flags; ///< Default flags
    static uint default_put_flags; ///< Default flags

    /** Opens a database handle.
   *
   * @param txn the transaction handle
   * @param name
   * @param flags
   * @throws lmdb_oo.MbdError on failure
   */
    static MdbDbi open(MDB_txn* txn, const char* name = null, const uint flags = default_flags) {
        MDB_dbi handle;
        mdb_dbi_open(txn, name, flags, &handle);
        return new MdbDbi(handle);
    }
    /** Close a database handle
    * Param: 
    *   env = an MdbEnv
    */
    void close(MdbEnv env) {
        mdb_dbi_close(env.handle(), _handle);
    }

    /** Constructor.
   * Param:  handle = a valid `MDB_dbi` handle
   */
    this(const MDB_dbi handle) nothrow {
        _handle = handle;
    }

    /**
   * Destructor.
   */
    ~this() nothrow {
        if (_handle) {
            /* No need to call close() here. */
        }
    }

    /**
   * Returns the underlying `MDB_dbi` handle.
   */
    MDB_dbi handle() nothrow {
      return _handle;
    }

    /**
   * Returns statistics for this database.
   *
   * @param txn a transaction handle
   * @throws lmdb_oo.MbdError on failure
   */
    MDB_stat stat(MDB_txn* txn) {
        MDB_stat result;
        dbi_stat(txn, handle(), &result);
        return result;
    }

    /**
   * Retrieves the flags for this database handle.
   *
   * @param txn a transaction handle
   * @throws lmdb_oo.MbdError on failure
   */
    uint flags(MDB_txn* txn) {
        uint result;
        dbi_flags(txn, handle(), &result);
        return result;
    }

    /**
   * Returns the number of records in this database.
   *
   * @param txn a transaction handle
   * @throws lmdb_oo.MbdError on failure
   */
    size_t size(MDB_txn* txn) {
        return stat(txn).ms_entries;
    }

    /**
   * @param txn a transaction handle
   * @param del
   * @throws lmdb_oo.MbdError on failure
   */
    void drop(MDB_txn* txn, const bool del = false) {
        dbi_drop(txn, handle(), del);
    }

    /**
   * Sets a custom key comparison function for this database.
   *
   * @param txn a transaction handle
   * @param cmp the comparison function
   * @throws lmdb_oo.MbdError on failure
   */
    MdbDbi set_compare(MDB_txn* txn, MDB_cmp_func* cmp = null) {
        dbi_set_compare(txn, handle(), cmp);
        return this;
    }

    /**
   * Retrieves a key/value pair from this database.
   *
   * @param txn a transaction handle
   * @param key
   * @param data
   * @throws lmdb_oo.MbdError on failure
   */
    bool get(MDB_txn* txn, const ref MdbVal key, ref MdbVal data) {
        return dbi_get(txn, handle(), &key._val, &data._val);
    }

    /**
   * Retrieves a key from this database.
   *
   * @param txn a transaction handle
   * @param key
   * @throws lmdb_oo.MbdError on failure
   */
    bool get(K)(MDB_txn* txn, const ref K key) const {
        const MdbVal k = new MdbVal(key, sizeof(K));
        MdbVal v = new MdbVal();
        return dbi_get(txn, handle(), k, v);
    }

    /**
   * Retrieves a key/value pair from this database.
   *
   * @param txn a transaction handle
   * @param key
   * @param val
   * @throws lmdb_oo.MbdError on failure
   */
    bool get(K, V)(MDB_txn* txn, const ref K key, ref V val) const {
        const MdbVal k = new MdbVal(&key, sizeof(K));
        MdbVal v = new MdbVal();
        const bool result = dbi_get(txn, handle(), k, v);
        if (result) {
            val = *v.data(V)();
        }
        return result;
    }

    /**
   * Retrieves a key/value pair from this database.
   *
   * @param txn a transaction handle
   * @param key a NUL-terminated string key
   * @param val
   * @throws lmdb_oo.MbdError on failure
   */
    bool get(V)(MDB_txn* txn, const char* key, ref V val) const {
        const MdbVal k = new MdbVal(key, strlen(key));
        MdbVal v = new MdbVal();
        const bool result = dbi_get(txn, handle(), k, v);
        if (result) {
            val = *v.data(V);
        }
        return result;
    }

    /**
   * Stores a key/value pair into this database.
   *
   * @param txn a transaction handle
   * @param key
   * @param data
   * @param flags
   * @throws lmdb_oo.MbdError on failure
   */
    bool put(MDB_txn* txn, const ref MdbVal key, ref MdbVal data, const uint flags = default_put_flags) {
        return dbi_put(txn, handle(), cast(MDB_val*)&key._val, cast(MDB_val*) &data._val, flags);
    }

    /**
   * Stores a key into this database.
   *
   * @param txn a transaction handle
   * @param key
   * @param flags
   * @throws lmdb_oo.MbdError on failure
   */
    bool put(K)(MDB_txn* txn, const ref K key, const uint flags = default_put_flags) {
        const MdbVal k = new MdbVal(&key, sizeof(K));
        MdbVal v = new MdbVal();
        return dbi_put(txn, handle(), k, v, flags);
    }

    /**
   * Stores a key/value pair into this database.
   *
   * @param txn a transaction handle
   * @param key
   * @param val
   * @param flags
   * @throws lmdb_oo.MbdError on failure
   */
    bool put(K, V)(MDB_txn* txn, const ref K key, const ref V val,
            const uint flags = default_put_flags) {
        const MdbVal k = new MdbVal(&key, sizeof(K));
        MdbVal v = new MdbVal(&val, sizeof(V));
        return dbi_put(txn, handle(), k, v, flags);
    }

    /**
   * Stores a key/value pair into this database.
   *
   * @param txn a transaction handle
   * @param key a NUL-terminated string key
   * @param val
   * @param flags
   * @throws lmdb_oo.MbdError on failure
   */
    bool put(V)(MDB_txn* txn, const char* key, const ref V val, const uint flags = default_put_flags) {
        const MdbVal k = new MdbVal(key, strlen(key));
        MdbVal v = new MdbVal(&val, sizeof(V));
        return dbi_put(txn, handle(), k, v, flags);
    }

  /**
   * Stores a key/value pair into this database.
   *
   * @param txn a transaction handle
   * @param key a NUL-terminated string key
   * @param val a NUL-terminated string key
   * @param flags
   * @throws lmdb_oo.MbdError on failure
   */
    bool put(MDB_txn* txn, const char* key, const char* val, const uint flags = default_put_flags) {
        const MdbVal k = new MdbVal(key, strlen(key));
        MdbVal v = new MdbVal(val, strlen(val));
        return dbi_put(txn, handle(), cast(MDB_val*)&k._val, cast(MDB_val*)&v._val, flags);
    }

  /**
   * Removes a key/value pair from this database.
   *
   * @param txn a transaction handle
   * @param key
   * @throws lmdb_oo.MbdError on failure
   */
    bool del(MDB_txn* txn, const ref MdbVal key) {
        return dbi_del(txn, handle(), cast(MDB_val*)&key._val);
    }

  /**
   * Removes a key/value pair from this database.
   *
   * @param txn a transaction handle
   * @param key
   * @throws lmdb_oo.MbdError on failure
   */
    bool del(K)(MDB_txn* txn, const ref K key) {
        const MdbVal k = new MdbVal(&key, sizeof(K));
        return dbi_del(txn, handle(), k);
    }
}


/* Resource Interface: Cursors */

/** Resource class for `MDB_cursor*` handles.
 *
 * @note Instances of this class are movable, but not copyable.
 * @see http://symas.com/mdb/doc/group__internal.html#structMDB__cursor
 */
class MdbCursor {
protected:
    MDB_cursor* _handle; ///< Opace handle from lmdb library

public:
    static uint default_flags; ///< Default cursor flags for ops

    /** Creates an LMDB MdbCursor.
    * @param txn the transaction handle
    * @param dbi the database handle
    * @throws lmdb_oo.MbdError on failure
    */
    static MdbCursor open(MDB_txn* txn, const MDB_dbi dbi) {
        MDB_cursor* handle;
        cursor_open(txn, dbi, &handle);
        assert(handle != null);
        return new MdbCursor(handle);
    }

    /** Constructor.
    * @param handle a valid `MDB_cursor*` handle
    */
    this(MDB_cursor* handle) nothrow {
        _handle = handle;
    }

    /** Destructor.
    */
    ~this() nothrow {
        try {
            close();
        } catch (Exception ex) {
            writefln("Catched a exception '%s'", ex.msg);
        }
    }

    /** Returns the underlying `MDB_cursor*` handle.
    */
    MDB_cursor* handle() nothrow {
      return _handle;
    }

    /** Closes this cursor.
   * @note this method is idempotent
   * @post `handle() == null`
   */
    void close() nothrow {
        if (_handle) {
            cursor_close(_handle);
            _handle = null;
        }
    }

    /** Renews this cursor.
     *
     * @param txn the transaction scope
     * @throws lmdb_oo.MbdError on failure
     */
    void renew(MDB_txn* txn) {
        cursor_renew(txn, handle());
    }

    /** Returns the cursor's transaction handle.
   */
    MDB_txn* txn() nothrow {
        return cursor_txn(cast(MDB_cursor*)handle());
    }

    /** Returns the cursor's database handle.
   */
    MDB_dbi dbi() nothrow {
        return cursor_dbi(cast(MDB_cursor*)handle());
    }

    /** Retrieves a key from the database.
   *
   * @param key
   * @param op
   * @throws lmdb_oo.MbdError on failure
   */
    bool get(MDB_val* key, const MDB_cursor_op op) {
        return get(key, null, op);
    }

    /** Retrieves a key from the database.
   *
   * @param key
   * @param op
   * @throws lmdb_oo.MbdError on failure
   */
    bool get(ref MdbVal key, const MDB_cursor_op op) {
        return get(cast(MDB_val*)&key._val, cast(MDB_val*)null, op);
    }

    /** Retrieves a key/value pair from the database.
   *
   * @param key
   * @param val (may be `null`)
   * @param op
   * @throws lmdb_oo.MbdError on failure
   */
    bool get(MDB_val* key, MDB_val* val, const MDB_cursor_op op) {
        return cursor_get(handle(), key, val, op);
    }

    /** Retrieves a key/value pair from the database.
   *
   * @param key
   * @param val
   * @param op
   * @throws lmdb_oo.MbdError on failure
   */
    bool get(ref MdbVal key, ref MdbVal val, const MDB_cursor_op op) {
        return cursor_get(handle(), cast(MDB_val*)&key._val, cast(MDB_val*)&val._val, op);
    }

    /** Retrieves a key/value pair from the database.
   *
   * @param key
   * @param val
   * @param op
   * @throws lmdb_oo.MbdError on failure
   */
    bool get(ref string key, ref string val, const MDB_cursor_op op) {
        MdbVal k = new MdbVal(), v = new MdbVal();
        const bool found = get(k, v, op);
        if (found) {
            char[] key0; key0  ~= to!(char[])(k._val.mv_data [ 0 .. k._val.mv_size]);
            key = to!string(key0);
            char[] val0; val0  ~= to!(char[])(v._val.mv_data [ 0 .. v._val.mv_size]);
            val = to!string(val0);
        }
        return found;
    }

    /** Positions this cursor at the given key.
   *
   * @param key
   * @param op
   * @throws lmdb_oo.MbdError on failure
   */
    bool find(K)(const ref K key, const MDB_cursor_op op = MDB_SET) {
        MdbVal k = new MdbVal(&key, sizeof(K));
        return get(k, null, op);
    }
}
