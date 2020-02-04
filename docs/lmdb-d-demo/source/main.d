/* lmdb bining test executable */
import core.stdc.string;

import std.bitmanip;
import std.conv;
import std.format;
import std.random;
import std.stdio;
import std.string;

/* Import the binding modules into scope */
import lmdb;
import lmdb_oo;

/** Minimalistic demo code for lmdb 
 * @param args Commandline args as a dynamic array of strings
 */
void main(const string[] args)
{
	writefln("lmdb binding demo, compiled on %s", __DATE__);

	/* Extract the lmdb version */
	int major, minor, patch;
	char* vstr_c = mdb_version(&major, &minor, &patch);
	const string vstr = to!string(vstr_c);
	writefln("The installed lmdb version is %s", vstr);

	/* Run some demo code on the liblmdb C API bindings */
	lmdb_toy1(args);

	/* Run some demo code using some more OO binding code (WIP) */
	try {
		lmdb_toy2(args);
		lmdb_toy3(args);
	}
	catch (Exception ex) {
		writeln("Something wonderful has happened.\nCatched exeception :", ex.msg);
	}
}

/** Display the lmdb error string in case of error
 * @param rc Resultcode to check
 * @param msg Message to display in case of error
 * @param line Line number or __LINE__ if not given
 * @return  bool, true if ok, false in case of error
 */
bool check_rc(int rc, string msg, int line = __LINE__)
{
	if (rc != 0)
	{
		writefln("%d:%s, rc = %s", line, msg, to!string(mdb_strerror(rc)) );
		return false;
	}
	return true;
}

/** Simple test for the low-level C library calls
 * @param args Commandline args as a dynamic array of strings
 */
void lmdb_toy1(const string[] args)
{
	int rc;
	MDB_env* env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn* txn;
	MDB_cursor* cursor;

	uint dbi_flags;
	int[] items;

	const bool verbose = args.length >= 2 && args[1] == "-v" ? true : false;

	/* Note: Most error checking omitted for simplicity */

	rc = mdb_env_create(&env);
	if (!check_rc(rc, "Can't create MDB env")) goto free_mdb_env;
	rc = mdb_env_open(env, ".", 0, std.conv.octal!664);
	if (!check_rc(rc, "Can't open MDB file")) goto free_mdb_env;

	rc = mdb_txn_begin(env, null, 0, &txn);
	if (!check_rc(rc, "Can't start the MDB transaction")) goto close_mdb_databases;

	rc = mdb_dbi_open(txn, null, 0, &dbi);
	if (!check_rc(rc, "Can't open MDB index")) goto close_mdb_databases;

	rc = mdb_drop(txn,dbi,0);
	if (!check_rc(rc, "Can't drop MDB table")) goto close_mdb_databases;

	rc = mdb_dbi_flags(txn, dbi, &dbi_flags);
	if (!check_rc(rc, "Can't read MDB flags")) goto close_mdb_databases;

	foreach (i; 0 .. 5)
	{
		ubyte[4] kval;
		ubyte[32] dval;
		key.assign!(ubyte[4])(&kval, kval.sizeof);
		data.assign!(ubyte[32])(&dval, dval.sizeof);

		kval = nativeToBigEndian(i);
		foreach (j; 0 .. dval.length)
		{
			dval[j] = to!ubyte(uniform!("[)", int, int)(0, 2 ^^ 8));
		}
		if (verbose)
			writefln("PUT: key: %x %s, data: %x %s", key.mv_data,
					key.mv_data[0 .. key.mv_size], data.mv_data, data.mv_data[0 .. data.mv_size]);

		rc = mdb_put(txn, dbi, &key, &data, 0);
		if (!check_rc(rc, "Can't put MDB data")) goto close_mdb_databases;
	}
	rc = mdb_txn_commit(txn);
	if (!check_rc(rc, "Can't commit MDB transaction")) goto close_mdb_databases;

	rc = mdb_txn_begin(env, null, MDB_RDONLY, &txn);
	if (!check_rc(rc, "Can't start the MDB transaction")) goto close_mdb_databases;
	rc = mdb_cursor_open(txn, dbi, &cursor);
	if (!check_rc(rc, "Can't open MDB index")) goto close_mdb_databases;
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_cursor_op.MDB_NEXT)) == 0)
	{
		ubyte[4] tmp = key.data!ubyte()[0..4];
		auto kv = bigEndianToNative!(int, 4)(tmp);
		items ~= kv;
		if (verbose)
			writefln("GET: key: %x %s, data: %x %s", key.mv_data,
					key.mv_data[0 .. key.mv_size], data.mv_data, data.mv_data[0 .. data.mv_size]);
	}
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	rc = mdb_txn_begin(env, null, 0, &txn);
	if (!check_rc(rc, "Can't start the MDB transaction")) goto close_mdb_databases;
	rc = mdb_cursor_open(txn, dbi, &cursor);
	if (!check_rc(rc, "Can't open MDB index")) goto close_mdb_databases;
	foreach (i; items) {
		auto kval = nativeToBigEndian(i);
		key.assign!(ubyte[4])(&kval, kval.length);
		writefln("DEL: key: %x %s", key.mv_data, kval);
		rc = mdb_del(txn, dbi, &key, null);
		if (!check_rc(rc, "Can't open MDB index")) goto close_mdb_databases;
	}
//	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_cursor_op.MDB_FIRST)) == 0)
//	{
//		if (verbose)
//			writefln("DEL: key: %x %s, data: %x %s", key.mv_data,
//					key.mv_data[0 .. key.mv_size], data.mv_data, data.mv_data[0 .. data.mv_size]);
//		// rc = mdb_cursor_del(cursor, 0);
//		rc = mdb_del(txn, dbi, &key, null);
//		if (!check_rc(rc, "Can't delete MDB index")) goto close_mdb_databases;
//	}
	mdb_cursor_close(cursor);
	if (!check_rc(rc, "Can't del MDB item")) goto close_mdb_databases;
	mdb_txn_commit(txn);

	rc = mdb_txn_begin(env, null, MDB_RDONLY, &txn);
	if (!check_rc(rc, "Can't start the MDB transaction")) goto close_mdb_databases;
	rc = mdb_cursor_open(txn, dbi, &cursor);
	if (!check_rc(rc, "Can't open MDB index")) goto close_mdb_databases;
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_cursor_op.MDB_NEXT)) == 0)
	{
		ubyte[4] tmp = key.data!ubyte()[0..4];
		auto kv = bigEndianToNative!(int, 4)(tmp);
		items ~= kv;
		if (verbose)
			writefln("GET: key: %x %s, data: %x %s", key.mv_data,
					key.mv_data[0 .. key.mv_size], data.mv_data, data.mv_data[0 .. data.mv_size]);
	mdb_cursor_close(cursor);
	}
	mdb_txn_abort(txn);

close_mdb_databases:
	mdb_dbi_close(env, dbi);
free_mdb_env:
	mdb_env_close(env);
}

/* ------------------------------------------------------------------ */

/** Simple test for the D OO-layer classes and operation
 */
void lmdb_toy2(const string[] args)
{
	int rc;
	const bool verbose = args.length >= 2 && args[1] == "-v" ? true : false;

	/* Note: Most error checking omitted for simplicity */

	auto env = MdbEnv.create();
	env.open(".", 0, std.conv.octal!664);

	auto txn = MdbTxn.begin(env.handle());
	auto dbi = MdbDbi.open(txn.handle());

	writeln("Create data items in DB.");
	MdbVal key, data;
	foreach (i; 0 .. 5)
	{
		ubyte[4] kval = nativeToBigEndian(i);
		ubyte[32] dval = void;
		foreach (j; 0 .. dval.length) {	dval[j] = to!ubyte(uniform!("[)", int, int)(0, 2 ^^ 8)); }
		key = new MdbVal(cast(void*)kval, kval.length);
		data = new MdbVal(cast(void*)dval, dval.length);
		if (verbose)
			writefln("PUT: key: %s, data: %s", key, data);
		rc = dbi.put(txn.handle(), key, data);
	}
	txn.commit();

	MdbVal[] items;
	MdbCursor cursor;

	writeln("List of data items in DB. Remember items for deletion");
	lmdb_iterate_readonly_job(env, txn, dbi, cursor, (MdbVal x) { items ~= new MdbVal(x); return true; }, verbose);

	writefln("Delete %d remembered items", items.length);
	txn = MdbTxn.begin(env.handle(), null, 0);
	cursor = MdbCursor.open(txn.handle(), dbi.handle());
	foreach (i; items)
	{
		if (verbose)
			writefln("DEL: key: %s", i);
		rc = dbi.del(txn.handle(), i);
	}
	cursor.close();
	txn.commit();

	writeln("List of all remaining data items in DB.");
	lmdb_iterate_readonly_job(env, txn, dbi, cursor, null, verbose);

	dbi.close(env);
	env.close();
}

/* ------------------------------------------------------------------------- */

/** Some data structure */
struct MyDataTable {
	uint  field0; ///< Some filed...
	uint  field1; ///<
	uint  field2; ///<
	uint  field3; ///<
	char[10] field4; ///<
	float field5; ///<
}

/** Some test code */
void lmdb_toy3(const string[] args)
{
	int rc;
	const bool verbose = args.length >= 2 && args[1] == "-v" ? true : false;

	/* Note: Most error checking omitted for simplicity */

	auto env = MdbEnv.create();
	env.open(".", 0, std.conv.octal!664);

	auto txn = MdbTxn.begin(env.handle());
	auto dbi = MdbDbi.open(txn.handle(), null, MDB_INTEGERKEY);

	writeln("Create data items in DB.");
	MdbVal key, data;
	foreach (uint i; 0 .. 5)
	{
		ubyte[4] kval ; kval[0..4].write!uint(i, 0);
		MyDataTable table = { 42, 23, 17, i, };
		table.field4[].sformat("Idx=%d\0", table.field3);
		table.field5 = 3.14 * i;
		key = new MdbVal(cast(void*)kval, kval.length);
		data = new MdbVal(cast(void*)&table, table.sizeof);
		if (verbose)
			writefln("PUT: key: %s, data: %s", key, data);
		rc = dbi.put(txn.handle(), key, data);
	}
	txn.commit();

	MdbVal[] items;
	MdbCursor cursor;

	writeln("List of data items in DB. Remember items for deletion");
	lmdb_iterate_readonly_job(env, txn, dbi, cursor, (MdbVal x) { items ~= new MdbVal(x); return true; }, verbose);

	writefln("Delete %d remembered items", items.length);
	txn = MdbTxn.begin(env.handle(), null, 0);
	cursor = MdbCursor.open(txn.handle(), dbi.handle());
	foreach (i; items)
	{
		if (verbose)
			writefln("DEL: key: %s", i);
		rc = dbi.del(txn.handle(), i);
	}
	cursor.close();
	txn.commit();

	writeln("List of all remaining data items in DB.");
	lmdb_iterate_readonly_job(env, txn, dbi, cursor, null, verbose);

	dbi.close(env);
	env.close();
}

/* ------------------------------------------------------------------------- */

/** Iterate through all nodes in RW mode, execute a delegate for each node.
 * @param env Reference to MdbEnv class
 * @param tcn Reference to MdbTxn class
 * @param cursor Reference to MdbCursor class
 * @param fkt Delegate with reference to an MdbVal object. Returns bool=false to bereak loop.
 */
void lmdb_toy2_iterate_readwrite(MdbEnv env, MdbTxn txn, MdbDbi dbi, MdbCursor cursor, bool delegate (MdbVal) fkt, 
	string opname = "PUT", bool verbose = true)
{
	int itemcount; bool fkt_rc;
	MdbVal key = new MdbVal(), data = new MdbVal();
	txn = MdbTxn.begin(env.handle(), null, 0);
	cursor = MdbCursor.open(txn.handle(), dbi.handle());
	while (cursor.get(key, data, MDB_cursor_op.MDB_NEXT))
	{
		itemcount++;
		if (verbose)
			writefln("%s: key: %s, data: %s", opname, key, data);
		if (fkt !is null) fkt_rc = fkt(key);
		if (!fkt_rc) break;
	}
	if (!itemcount) 
		writeln("DEL: Database seems to be empty.");
	txn.abort();
}

/** Iterate through all nodes in RO mode, execute a delegate for each node.
 * Params:
 *    env = Reference to MdbEnv class
 *    tcn = Reference to MdbTxn class
 *    cursor = Reference to MdbCursor class
 *    fkt = Delegate with reference to an MdbVal object. Returns bool=false to bereak loop.
 */
void lmdb_iterate_readonly_job(MdbEnv env, MdbTxn txn, MdbDbi dbi, MdbCursor cursor, 
	bool delegate (MdbVal) fkt, bool verbose = true)
{
	int itemcount; bool fkt_rc;
	MdbVal key = new MdbVal(), data = new MdbVal();
	txn = MdbTxn.begin(env.handle(), null, MDB_RDONLY);
	cursor = MdbCursor.open(txn.handle(), dbi.handle());
	while (cursor.get(key, data, MDB_cursor_op.MDB_NEXT))
	{
		itemcount++;
		if (verbose)
			writefln("GET: key: %s, data: %s", key, data);
		if (fkt !is null) fkt_rc = fkt(key);
		if (!fkt_rc) break;
	}
	if (!itemcount) 
		writeln("GET: Database seems to be empty.");
	txn.abort();
}
